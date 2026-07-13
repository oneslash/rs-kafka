//! Kafka Consumer - A higher-level API for consuming kafka topics.
//!
//! A consumer for Kafka topics on behalf of a specified group
//! providing help in offset management.  The consumer requires at
//! least one topic for consumption and allows consuming multiple
//! topics at the same time. Further, clients can restrict the
//! consumer to only specific topic partitions as demonstrated in the
//! following example.
//!
//! # Example
//! ```no_run
//! use kafkang::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
//!
//! let mut consumer =
//!    Consumer::from_hosts(vec!("localhost:9092".to_owned()))
//!       .with_topic_partitions("my-topic".to_owned(), &[0, 1])
//!       .with_fallback_offset(FetchOffset::Earliest)
//!       .with_group("my-group".to_owned())
//!       .with_offset_storage(Some(GroupOffsetStorage::Kafka))
//!       .create()
//!       .unwrap();
//! loop {
//!   for ms in consumer.poll().unwrap().iter() {
//!     for m in ms.messages() {
//!       println!("{:?}", m);
//!     }
//!     consumer.consume_messageset(&ms);
//!   }
//!   consumer.commit_consumed().unwrap();
//! }
//! ```
//!
//! Please refer to the documentation of the individual "with" methods
//! used to set up the consumer. These contain further information or
//! links to such.
//!
//! A call to `.poll()` on the consumer will ask for the next
//! available "chunk of data" for the client code to process.  The
//! returned data are `MessageSet`s. There is at most one for each partition
//! of the consumed topics. Individual messages are embedded in the
//! retrieved messagesets and can be processed using the `messages()`
//! iterator.  Due to this embedding, an individual message's lifetime
//! is bound to the `MessageSet` it is part of. Typically, client
//! code accesses the raw data/bytes, parses it into custom data types,
//! and passes that along for further processing within the application.
//! Although inconvenient, this helps in reducing the number of
//! allocations within the pipeline of processing incoming messages.
//!
//! If the consumer is configured for a non-empty group, it helps in
//! keeping track of already consumed messages by maintaining a map of
//! the consumed offsets.  Messages can be told "consumed" either
//! through `consume_message` or `consume_messages` methods.  Once
//! these consumed messages are committed to Kafka using
//! `commit_consumed`, the consumer will start fetching messages from
//! here even after restart.  Since committing is a certain overhead,
//! it is up to the client to decide the frequency of the commits.
//! The consumer will *not* commit any messages to Kafka
//! automatically.
//!
//! The configuration of a group is optional.  If the consumer has no
//! group configured, it will behave as if it had one, only that
//! committing consumed message offsets resolves into a void operation.

use std::collections::hash_map::{Entry, HashMap};
use std::slice;

use crate::client::fetch;
use crate::client::{CommitOffset, FetchPartition, KafkaClient};
use crate::error::{Error, KafkaCode, Result};
use crate::protocol;

// public re-exports
pub use self::builder::Builder;
use self::state::TopicPartition;
pub use crate::client::FetchOffset;
pub use crate::client::GroupOffsetStorage;
pub use crate::client::fetch::Message;

mod assignment;
mod builder;
mod config;
mod state;

/// The default value for `Builder::with_retry_max_bytes_limit`.
pub const DEFAULT_RETRY_MAX_BYTES_LIMIT: i32 = 0;

/// The default value for `Builder::with_fallback_offset`.
pub const DEFAULT_FALLBACK_OFFSET: FetchOffset = FetchOffset::Latest;

/// The Kafka Consumer
///
/// See module level documentation.
#[derive(Debug)]
pub struct Consumer {
    client: KafkaClient,
    state: state::State,
    config: config::Config,
}

// XXX 1) Allow returning to a previous offset (aka seeking)
// XXX 2) Issue IO in a separate (background) thread and pre-fetch messagesets

impl Consumer {
    /// Starts building a consumer using the given kafka client.
    #[must_use]
    pub fn from_client(client: KafkaClient) -> Builder {
        builder::new(Some(client), Vec::new())
    }

    /// Starts building a consumer bootstraping internally a new kafka
    /// client from the given kafka hosts.
    #[must_use]
    pub fn from_hosts(hosts: Vec<String>) -> Builder {
        builder::new(None, hosts)
    }

    /// Borrows the underlying kafka client.
    #[must_use]
    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    /// Borrows the underlying kafka client as mut.
    #[must_use]
    pub fn client_mut(&mut self) -> &mut KafkaClient {
        &mut self.client
    }

    /// Destroys this consumer returning back the underlying kafka client.
    #[must_use]
    pub fn into_client(self) -> KafkaClient {
        self.client
    }

    /// Retrieves the topic partitions being currently consumed by
    /// this consumer.
    #[must_use]
    pub fn subscriptions(&self) -> HashMap<String, Vec<i32>> {
        // ~ current subscriptions are reflected by
        // `self.state.fetch_offsets` see `self.fetch_messages()`.
        // ~ the number of topics subscribed can be estimated from the
        // user specified assignments stored in `self.state.assignments`.
        let mut h: HashMap<String, Vec<i32>> =
            HashMap::with_capacity(self.state.assignments.as_slice().len());
        // ~ expand subscriptions to (topic-name, partition id)
        let tps = self
            .state
            .fetch_offsets
            .keys()
            .map(|tp| (self.state.topic_name(tp.topic_ref), tp.partition));
        // ~ group by topic-name
        for tp in tps {
            // ~ allocate topic-name only once per topic
            if let Some(ps) = h.get_mut(tp.0) {
                ps.push(tp.1);
                continue;
            }
            h.insert(tp.0.to_owned(), vec![tp.1]);
        }
        h
    }

    /// Polls for the next available message data.
    pub fn poll(&mut self) -> Result<MessageSets> {
        let (n, resps) = self.fetch_messages();
        self.process_fetch_responses(n, resps?)
    }

    /// Determines whether this consumer is set up to consume only a
    /// single topic partition.
    #[must_use]
    fn single_partition_consumer(&self) -> bool {
        self.state.fetch_offsets.len() == 1
    }

    /// Retrieves the group on which behalf this consumer is acting.
    /// The empty group name specifies a group-less consumer.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.config.group
    }

    /// Convenient method to allow consumer to manually reposition to a set of
    /// topic, partition and offset.
    /// let mut client: KafkaClient = KafkaClient::new(vec!["kafka.test.fio.drw:9092".to_string()]);
    /// # Examples
    ///
    /// ```no_run
    /// use kafkang::client::{FetchOffset, GroupOffsetStorage, KafkaClient};
    /// use kafkang::consumer::Consumer;
    /// use kafkang::Result;
    ///
    /// let mut consumer: Consumer = Consumer::from_hosts(vec!["localhost:9092".to_string()])
    ///     .with_topic("test-topic".to_string())
    ///     .with_fallback_offset(FetchOffset::Latest)
    ///     .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    ///     .create()
    ///     .unwrap();
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let topics = vec!["test-topic".to_string()];
    /// let offsets = client
    ///     .list_offsets(&topics, FetchOffset::ByTime(1698425676797))
    ///     .unwrap();
    /// let seek_results: Vec<Result<()>> = offsets
    ///     .into_iter()
    ///     .flat_map(|(topic, partition_offsets)| {
    ///         partition_offsets
    ///             .into_iter()
    ///             .map(move |po| (topic.clone(), po.partition, po.offset))
    ///     })
    ///     .map(|(topic, partition, offset)| consumer.seek(topic.as_str(), partition, offset))
    ///     .collect();
    /// ```
    ///
    /// This makes the consumer resets its fetch offsets to the nearest offsets after the
    /// timestamp.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let topic_ref = self.state.topic_ref(topic);
        match topic_ref {
            Some(topic_ref) => {
                let tp = TopicPartition {
                    topic_ref,
                    partition,
                };
                let maybe_entry = self.state.fetch_offsets.entry(tp);
                match maybe_entry {
                    Entry::Occupied(mut e) => {
                        e.get_mut().offset = offset;
                        Ok(())
                    }
                    Entry::Vacant(_) => Err(Error::TopicPartitionError {
                        topic_name: topic.to_string(),
                        partition_id: partition,
                        error_code: KafkaCode::UnknownTopicOrPartition,
                    }),
                }
            }
            None => Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
        }
    }

    // ~ returns (number partitions queried, fecth responses)
    fn fetch_messages(&mut self) -> (u32, Result<Vec<fetch::Response>>) {
        // ~ if there's a retry partition ... fetch messages just for
        // that one. Otherwise try to fetch messages for all assigned
        // partitions.
        if let Some(tp) = self.state.retry_partitions.pop_front() {
            let Some(s) = self.state.fetch_offsets.get(&tp) else {
                return (1, Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)));
            };

            let topic = self.state.topic_name(tp.topic_ref);
            debug!(
                "fetching retry messages: (fetch-offset: {{\"{}:{}\": {:?}}})",
                topic, tp.partition, s
            );
            (
                1,
                self.client.fetch_messages_for_partition(
                    &FetchPartition::new(topic, tp.partition, s.offset).with_max_bytes(s.max_bytes),
                ),
            )
        } else {
            let client = &mut self.client;
            let state = &self.state;
            debug!(
                "fetching messages: (fetch-offsets: {:?})",
                state.fetch_offsets_debug()
            );
            let reqs = state.fetch_offsets.iter().map(|(tp, s)| {
                let topic = state.topic_name(tp.topic_ref);
                FetchPartition::new(topic, tp.partition, s.offset).with_max_bytes(s.max_bytes)
            });
            (
                state.fetch_offsets.len() as u32,
                client.fetch_messages(reqs),
            )
        }
    }

    // ~ post process a data retrieved through fetch_messages before
    // handing them out to client code
    //   - update the fetch state for the next fetch cycle
    // ~ num_partitions_queried: the original number of partitions requested/queried for
    //   the responses
    // ~ resps: the responses to post process
    fn process_fetch_responses(
        &mut self,
        num_partitions_queried: u32,
        resps: Vec<fetch::Response>,
    ) -> Result<MessageSets> {
        struct FetchStateUpdate {
            tp: state::TopicPartition,
            offset: i64,
            max_bytes: i32,
            has_messages: bool,
            retry: bool,
        }

        let single_partition_consumer = self.single_partition_consumer();
        let default_max_bytes = self.client.fetch_max_bytes_per_partition();
        let mut empty = true;
        let mut updates = Vec::new();

        for resp in &resps {
            for t in resp.topics() {
                let topic_ref = self
                    .state
                    .assignments
                    .topic_ref(t.topic())
                    .expect("unknown topic in response");

                for p in t.partitions() {
                    let tp = state::TopicPartition {
                        topic_ref,
                        partition: p.partition(),
                    };

                    // ~ for now, as soon as a partition has an error
                    // we fail to prevent client programs from not
                    // noticing.  however, in future we don't need to
                    // fail immediately, we can try to recover from
                    // certain errors and retry the fetch operation
                    // transparently for the caller.

                    let data = p.data()?;

                    let fetch_state = self
                        .state
                        .fetch_offsets
                        .get(&tp)
                        .expect("non-requested partition");
                    let mut next_offset = fetch_state.offset;
                    let mut next_max_bytes = fetch_state.max_bytes;
                    let mut retry = false;

                    if let Some(last_msg) = data.messages().last() {
                        next_offset = last_msg.offset + 1;
                        next_max_bytes = default_max_bytes;
                        empty = false;
                    } else {
                        debug!(
                            "no data received for {}:{} (max_bytes: {} / fetch_offset: {} / \
                                highwatermark_offset: {})",
                            t.topic(),
                            tp.partition,
                            fetch_state.max_bytes,
                            fetch_state.offset,
                            data.highwatermark_offset()
                        );

                        // ~ when a partition is empty but has a
                        // highwatermark-offset equal to or greater
                        // than the one we tried to fetch ... we'll
                        // try to increase the max-fetch-size in the
                        // next fetch request
                        if fetch_state.offset < data.highwatermark_offset() {
                            if fetch_state.max_bytes < self.config.retry_max_bytes_limit {
                                // ~ try to double the max_bytes
                                let prev_max_bytes = fetch_state.max_bytes;
                                let incr_max_bytes = prev_max_bytes + prev_max_bytes;
                                if incr_max_bytes > self.config.retry_max_bytes_limit {
                                    next_max_bytes = self.config.retry_max_bytes_limit;
                                } else {
                                    next_max_bytes = incr_max_bytes;
                                }
                            } else if num_partitions_queried == 1 {
                                // ~ this was a single partition
                                // request and we didn't get anything
                                // and we won't be increasing the max
                                // fetch size ... this is will fail
                                // forever ... signal the problem to
                                // the user
                                return Err(Error::Kafka(KafkaCode::MessageSizeTooLarge));
                            }
                            // ~ if this consumer is subscribed to one
                            // partition only, there's no need to push
                            // the partition to the 'retry_partitions'
                            // (this is just a small optimization)
                            if !single_partition_consumer {
                                // ~ mark this partition for a retry on its own
                                retry = true;
                            }
                        }
                    }

                    updates.push(FetchStateUpdate {
                        tp,
                        offset: next_offset,
                        max_bytes: next_max_bytes,
                        has_messages: !data.messages().is_empty(),
                        retry,
                    });
                }
            }
        }

        // All fallible response processing is complete. Apply the planned
        // changes together so an error can never advance offsets for records
        // that are not returned to the caller.
        let assignments = &self.state.assignments;
        let fetch_offsets = &mut self.state.fetch_offsets;
        let retry_partitions = &mut self.state.retry_partitions;
        for update in updates {
            let topic = assignments[update.tp.topic_ref].topic();
            let fetch_state = fetch_offsets
                .get_mut(&update.tp)
                .expect("validated partition disappeared");
            let prev_max_bytes = fetch_state.max_bytes;

            fetch_state.offset = update.offset;
            fetch_state.max_bytes = update.max_bytes;

            if prev_max_bytes != update.max_bytes {
                if update.has_messages {
                    debug!(
                        "reset max_bytes for {}:{} from {} to {}",
                        topic, update.tp.partition, prev_max_bytes, update.max_bytes
                    );
                } else {
                    debug!(
                        "increased max_bytes for {}:{} from {} to {}",
                        topic, update.tp.partition, prev_max_bytes, update.max_bytes
                    );
                }
            }
            if update.retry {
                debug!("rescheduled for retry: {}:{}", topic, update.tp.partition);
                retry_partitions.push_back(update.tp);
            }
        }

        // XXX in future, issue one more fetch_messages request in the
        // background such that the next time the client polls that
        // request's response will likely be already ready for
        // consumption

        Ok(MessageSets {
            responses: resps,
            empty,
        })
    }

    /// Retrieves the offset of the last "consumed" message in the
    /// specified partition. Results in `None` if there is no such
    /// "consumed" message.
    #[must_use]
    pub fn last_consumed_message(&self, topic: &str, partition: i32) -> Option<i64> {
        self.state
            .topic_ref(topic)
            .and_then(|tref| {
                self.state.consumed_offsets.get(&state::TopicPartition {
                    topic_ref: tref,
                    partition,
                })
            })
            .map(|co| co.offset)
    }

    /// Marks the message at the specified offset in the specified
    /// topic partition as consumed by the caller.
    ///
    /// Note: a message with a "later/higher" offset automatically
    /// marks all preceding messages as "consumed", this is messages
    /// with "earlier/lower" offsets in the same partition.
    /// Therefore, it is not necessary to invoke this method for
    /// every consumed message.
    ///
    /// Results in an error if the specified topic partition is not
    /// being consumed by this consumer.
    pub fn consume_message(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let topic_ref = self
            .state
            .topic_ref(topic)
            .ok_or_else(|| Error::Kafka(KafkaCode::UnknownTopicOrPartition))?;

        let tp = state::TopicPartition {
            topic_ref,
            partition,
        };
        match self.state.consumed_offsets.entry(tp) {
            Entry::Vacant(v) => {
                v.insert(state::ConsumedOffset {
                    offset,
                    dirty: true,
                });
            }
            Entry::Occupied(mut v) => {
                let o = v.get_mut();
                if offset > o.offset {
                    o.offset = offset;
                    o.dirty = true;
                }
            }
        }
        Ok(())
    }

    /// A convenience method to mark the given message set consumed as a
    /// whole by the caller. This is equivalent to marking the last
    /// message of the given set as consumed.
    pub fn consume_messageset(&mut self, msgs: &MessageSet<'_>) -> Result<()> {
        if let Some(last) = msgs.messages().last() {
            self.consume_message(msgs.topic(), msgs.partition(), last.offset)
        } else {
            Ok(())
        }
    }

    /// Persists the so-far "marked as consumed" messages (on behalf
    /// of this consumer's group for the underlying topic - if any.)
    ///
    /// See also `Consumer::consume_message` and
    /// `Consumer::consume_messageset`.
    pub fn commit_consumed(&mut self) -> Result<()> {
        if self.config.group.is_empty() {
            return Err(Error::UnsetGroupId);
        }
        debug!(
            "commit_consumed: committing dirty-only consumer offsets (group: {} / offsets: {:?}",
            self.config.group,
            self.state.consumed_offsets_debug()
        );
        let (client, state) = (&mut self.client, &mut self.state);
        client.commit_offsets(
            &self.config.group,
            state
                .consumed_offsets
                .iter()
                .filter(|&(_, o)| o.dirty)
                .map(|(tp, o)| {
                    let topic = state.topic_name(tp.topic_ref);

                    // Note that the offset that is committed should be the
                    // offset of the next message a consumer should read, so
                    // add one to the consumed message's offset.
                    //
                    // https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
                    CommitOffset::new(topic, tp.partition, o.offset + 1)
                }),
        )?;
        for co in state.consumed_offsets.values_mut() {
            if co.dirty {
                co.dirty = false;
            }
        }
        Ok(())
    }
}

// --------------------------------------------------------------------

/// Messages retrieved from kafka in one fetch request.  This is a
/// concatenation of blocks of messages successfully retrieved from
/// the consumed topic partitions.  Each such partitions is guaranteed
/// to be present at most once in this structure.
#[derive(Debug)]
pub struct MessageSets {
    responses: Vec<fetch::Response>,

    /// Precomputed; Says whether there are some messages or whether
    /// the responses actually contain consumeable messages
    empty: bool,
}

impl MessageSets {
    /// Determines efficiently whether there are any consumeable
    /// messages in this data set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    #[must_use]
    pub fn iter(&self) -> MessageSetsIter<'_> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

/// Iterates over the message sets delivering the fetched message
/// data of consumed topic partitions.
impl<'a> IntoIterator for &'a MessageSets {
    type Item = MessageSet<'a>;
    type IntoIter = MessageSetsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut responses = self.responses.iter();
        let mut topics = responses.next().map(|r| r.topics().iter());
        let (curr_topic, partitions) = topics
            .as_mut()
            .and_then(Iterator::next)
            .map_or(("", None), |t| (t.topic(), Some(t.partitions().iter())));
        MessageSetsIter {
            responses,
            topics,
            curr_topic,
            partitions,
        }
    }
}

/// A set of messages successfully retrieved from a specific topic
/// partition.
pub struct MessageSet<'a> {
    topic: &'a str,
    partition: i32,
    messages: &'a [Message<'a>],
}

impl<'a> MessageSet<'a> {
    #[inline]
    #[must_use]
    pub fn topic(&self) -> &'a str {
        self.topic
    }

    #[inline]
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    #[inline]
    #[must_use]
    pub fn messages(&self) -> &'a [Message<'a>] {
        self.messages
    }
}

/// An iterator over the consumed topic partition message sets.
pub struct MessageSetsIter<'a> {
    responses: slice::Iter<'a, fetch::Response>,
    topics: Option<slice::Iter<'a, fetch::Topic<'a>>>,
    curr_topic: &'a str,
    partitions: Option<slice::Iter<'a, fetch::Partition<'a>>>,
}

impl<'a> Iterator for MessageSetsIter<'a> {
    type Item = MessageSet<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // ~ then the next available partition
            if let Some(p) = self.partitions.as_mut().and_then(Iterator::next) {
                // ~ skip erroneous partitions
                // ~ skip empty partitions
                if let Some(messages) = p
                    .data()
                    .ok()
                    .map(protocol::fetch::Data::messages)
                    .filter(|msgs| !msgs.is_empty())
                {
                    return Some(MessageSet {
                        topic: self.curr_topic,
                        partition: p.partition(),
                        messages,
                    });
                }
                continue;
            }
            // ~ then the next available topic
            if let Some(t) = self.topics.as_mut().and_then(Iterator::next) {
                self.curr_topic = t.topic();
                self.partitions = Some(t.partitions().iter());
                continue;
            }
            // ~ then the next available response
            if let Some(r) = self.responses.next() {
                self.curr_topic = "";
                self.topics = Some(r.topics().iter());
                continue;
            }
            // ~ finally we know there's nothing available anymore
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use super::*;
    use crate::client::DEFAULT_MAX_DECOMPRESSED_BYTES;
    use crate::codecs::ToByte;
    use crate::compression::Compression;
    use crate::protocol::ResponseParser as _;
    use crate::protocol::records::encode_record_batch;

    const TOPIC: &str = "partial-fetch-error";
    const INITIAL_FETCH_OFFSET: i64 = 0;

    fn encode_fetch_partition(
        response: &mut Vec<u8>,
        partition: i32,
        error_code: i16,
        highwatermark_offset: i64,
        record_set: &[u8],
    ) {
        partition.encode(response).unwrap();
        error_code.encode(response).unwrap();
        highwatermark_offset.encode(response).unwrap();
        highwatermark_offset.encode(response).unwrap(); // last_stable_offset
        INITIAL_FETCH_OFFSET.encode(response).unwrap(); // log_start_offset
        (0i32).encode(response).unwrap(); // aborted_transactions
        (-1i32).encode(response).unwrap(); // preferred_read_replica
        record_set.encode(response).unwrap();
    }

    fn mixed_success_and_error_response() -> fetch::Response {
        let mut request = protocol::FetchRequest::new_v11(1, "test", 100, 1, 1024, "");
        request.add(TOPIC, 0, INITIAL_FETCH_OFFSET, 1024);
        request.add(TOPIC, 1, INITIAL_FETCH_OFFSET, 1024);

        let record_set =
            encode_record_batch(&[(None, Some(b"must not be skipped"))], Compression::NONE)
                .unwrap();

        let mut response = Vec::new();
        (1i32).encode(&mut response).unwrap(); // correlation_id
        (0i32).encode(&mut response).unwrap(); // throttle_time_ms
        (0i16).encode(&mut response).unwrap(); // top-level error_code
        (0i32).encode(&mut response).unwrap(); // session_id
        (1i32).encode(&mut response).unwrap(); // topics
        TOPIC.encode(&mut response).unwrap();
        (2i32).encode(&mut response).unwrap(); // partitions
        encode_fetch_partition(&mut response, 0, 0, 1, &record_set);
        encode_fetch_partition(
            &mut response,
            1,
            KafkaCode::NotLeaderForPartition as i16,
            0,
            &[],
        );

        protocol::fetch::ResponseParser {
            validate_crc: true,
            max_decompressed_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
            requests: Some(&request),
        }
        .parse(response)
        .unwrap()
    }

    fn consumer_for_partial_fetch_test() -> (Consumer, assignment::AssignmentRef) {
        let assignments = assignment::from_map(HashMap::from([(TOPIC.to_owned(), vec![0, 1])]));
        let topic_ref = assignments.topic_ref(TOPIC).unwrap();
        let mut fetch_offsets =
            HashMap::with_capacity_and_hasher(2, state::PartitionHasher::default());
        for partition in [0, 1] {
            fetch_offsets.insert(
                state::TopicPartition {
                    topic_ref,
                    partition,
                },
                state::FetchState {
                    offset: INITIAL_FETCH_OFFSET,
                    max_bytes: 1024,
                },
            );
        }

        let state = state::State {
            assignments,
            fetch_offsets,
            retry_partitions: VecDeque::new(),
            consumed_offsets: HashMap::with_hasher(state::PartitionHasher::default()),
        };
        let config = config::Config {
            group: String::new(),
            fallback_offset: FetchOffset::Earliest,
            retry_max_bytes_limit: 0,
        };

        (
            Consumer {
                client: KafkaClient::new(Vec::new()),
                state,
                config,
            },
            topic_ref,
        )
    }

    #[test]
    fn partial_fetch_error_does_not_advance_successful_partition_offset() {
        let (mut consumer, topic_ref) = consumer_for_partial_fetch_test();
        let response = mixed_success_and_error_response();

        let result = consumer.process_fetch_responses(2, vec![response]);

        assert!(result.is_err(), "the partition error must fail the poll");
        let successful_partition = state::TopicPartition {
            topic_ref,
            partition: 0,
        };
        assert_eq!(
            consumer.state.fetch_offsets[&successful_partition].offset, INITIAL_FETCH_OFFSET,
            "a failed poll must not advance an offset for records it never returned"
        );
    }
}
