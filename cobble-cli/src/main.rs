use cobble::{Config, DedicatedCompactionService, RemoteCompactionServer};
use cobble_web_monitor::{MonitorConfig, MonitorConfigSource, MonitorServer};
use log::LevelFilter::Info;
use std::error::Error;
use std::time::Duration;

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {}", err);
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        return Ok(());
    };
    if command == "--help" || command == "-h" {
        print_usage();
        return Ok(());
    }
    match command.as_str() {
        "remote-compactor" => run_remote_compactor(args),
        "web-monitor" => run_web_monitor(args),
        "compact" => run_compact(args),
        _ => {
            print_usage();
            Err(format!("Unknown command: {}", command).into())
        }
    }
}

fn run_remote_compactor(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let mut config_path: Option<String> = None;
    let mut bind_addr: Option<String> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = args.next();
            }
            "--bind" | "--address" => {
                bind_addr = args.next();
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            _ => {
                return Err(format!("Unknown argument: {}", arg).into());
            }
        }
    }

    let config = if let Some(config_path) = config_path {
        Config::from_path(&config_path)?
    } else {
        Config {
            log_console: true,
            log_level: Info,
            ..Config::default()
        }
    };
    let bind_addr = bind_addr
        .or_else(|| config.compaction_remote_addr.clone())
        .or_else(|| Some("127.0.0.1:0".to_string()))
        .unwrap();
    let server = RemoteCompactionServer::new(config)?;
    server.serve(&bind_addr)?;
    Ok(())
}

fn run_web_monitor(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let mut config_path: Option<String> = None;
    let mut bind_addr: Option<String> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = args.next();
            }
            "--bind" | "--address" => {
                bind_addr = args.next();
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            _ => {
                return Err(format!("Unknown argument: {}", arg).into());
            }
        }
    }

    let Some(config_path) = config_path else {
        return Err("web-monitor requires --config <path>".into());
    };

    let mut server = MonitorServer::new(MonitorConfig {
        source: MonitorConfigSource::ConfigPath(config_path),
        bind_addr: bind_addr.unwrap_or_else(|| "127.0.0.1:0".to_string()),
        global_snapshot_id: None,
        ..MonitorConfig::default()
    })?;

    let handle = server.serve()?;
    eprintln!("web-monitor listening on http://{}/", handle.bind_addr());

    // Keep process alive until interrupted; axum server runs on monitor runtime.
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

fn print_usage() {
    eprintln!(
        "Usage:\n  \
         cobble-cli remote-compactor [--config <path>] [--bind <host:port>]\n  \
         cobble-cli web-monitor --config <path> [--bind <host:port>]\n  \
         cobble-cli compact --config <path> [--workers <n>] [--scan-interval <ms>] \
<path-or-url> [<path-or-url> ...]\n"
    );
}

fn run_compact(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let mut config_path: Option<String> = None;
    let mut worker_count: Option<usize> = None;
    let mut scan_interval_ms: Option<u64> = None;
    let mut paths = Vec::<String>::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(
                    args.next()
                        .ok_or("compact --config requires a path argument")?,
                );
            }
            "--path" => {
                paths.push(
                    args.next()
                        .ok_or("compact --path requires a path or storage URL")?,
                );
            }
            "--workers" => {
                let value = args
                    .next()
                    .ok_or("compact --workers requires a positive integer")?;
                worker_count = Some(
                    value
                        .parse()
                        .map_err(|_| "compact --workers requires a positive integer")?,
                );
            }
            "--scan-interval" | "--poll-interval" => {
                let value = args
                    .next()
                    .ok_or("compact --scan-interval requires milliseconds")?;
                scan_interval_ms = Some(
                    value
                        .parse()
                        .map_err(|_| "compact --scan-interval requires milliseconds")?,
                );
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            _ if arg.starts_with('-') => {
                return Err(format!("Unknown argument: {}", arg).into());
            }
            _ => paths.push(arg),
        }
    }

    let config_path = config_path.ok_or("compact requires --config <path>")?;
    if paths.is_empty() {
        return Err("compact requires at least one DB directory or parent directory".into());
    }

    let mut config = Config::from_path(&config_path)?;
    config.log_console = true;
    if let Some(ms) = scan_interval_ms {
        config.compaction_dedicated_poll_interval_ms = ms;
    }
    let worker_count = worker_count.unwrap_or(config.compaction_threads.max(1));
    let scan_interval = Duration::from_millis(
        scan_interval_ms.unwrap_or(config.compaction_dedicated_poll_interval_ms),
    );
    let service =
        DedicatedCompactionService::open_storage_paths(config, paths, worker_count, scan_interval)?;
    service.run()?;
    Ok(())
}
