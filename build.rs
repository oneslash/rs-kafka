use std::env;
use std::process::Command;

fn main() {
    // Allow using `cfg(kafka_rust_nightly)` without triggering `unexpected_cfgs`.
    println!("cargo:rustc-check-cfg=cfg(kafka_rust_nightly)");

    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let is_nightly = Command::new(rustc)
        .arg("-vV")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|stdout| stdout.lines().next().map(str::to_owned))
        .is_some_and(|first_line| first_line.contains("-nightly"));

    if is_nightly {
        println!("cargo:rustc-cfg=kafka_rust_nightly");
    }

    println!("cargo:rerun-if-changed=build.rs");
}
