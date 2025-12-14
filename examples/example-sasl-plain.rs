fn main() {
    example::main();
}

mod example {
    use std::env;
    use std::process;

    use kafkang::client::{KafkaClient, SaslConfig};
    use tracing::info;

    pub fn main() {
        tracing_subscriber::fmt::init();

        let cfg = match Config::from_cmdline() {
            Ok(cfg) => cfg,
            Err(e) => {
                println!("{e}");
                process::exit(1);
            }
        };

        info!("connecting to brokers: {:?}", cfg.brokers);
        let mut client = KafkaClient::new(cfg.brokers);
        client.set_sasl_config(Some(SaslConfig::plain(cfg.username, cfg.password)));

        match client.load_metadata_all() {
            Ok(()) => {
                let topics_view = client.topics();
                let topics: Vec<&str> = topics_view.names().collect();
                if topics.is_empty() {
                    println!("Connected, but no topics are available.");
                } else {
                    println!("Topics:");
                    for t in topics {
                        println!("- {t}");
                    }
                }
            }
            Err(e) => {
                println!("{e:?}");
                process::exit(1);
            }
        }
    }

    struct Config {
        brokers: Vec<String>,
        username: String,
        password: String,
    }

    impl Config {
        fn from_cmdline() -> Result<Config, String> {
            let mut opts = getopts::Options::new();
            opts.optflag("h", "help", "Print this help screen");
            opts.optopt(
                "",
                "brokers",
                "Specify kafka brokers (comma separated)",
                "HOSTS",
            );
            opts.optopt("", "username", "SASL username", "USERNAME");
            opts.optopt("", "password", "SASL password", "PASSWORD");

            let args: Vec<_> = env::args().collect();
            let m = match opts.parse(&args[1..]) {
                Ok(m) => m,
                Err(e) => return Err(format!("{e}")),
            };

            if m.opt_present("help") {
                let brief = format!("{} [options]", args[0]);
                return Err(opts.usage(&brief));
            };

            let brokers = m
                .opt_str("brokers")
                .map(|s| {
                    s.split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_else(|| vec!["localhost:9096".into()]);
            if brokers.is_empty() {
                return Err("Invalid --brokers specified!".to_owned());
            }

            let username = m
                .opt_str("username")
                .or_else(|| env::var("KAFKANG_SASL_USERNAME").ok())
                .ok_or_else(|| "Missing --username (or env KAFKANG_SASL_USERNAME)".to_owned())?;

            let password = m
                .opt_str("password")
                .or_else(|| env::var("KAFKANG_SASL_PASSWORD").ok())
                .ok_or_else(|| "Missing --password (or env KAFKANG_SASL_PASSWORD)".to_owned())?;

            Ok(Config {
                brokers,
                username,
                password,
            })
        }
    }
}
