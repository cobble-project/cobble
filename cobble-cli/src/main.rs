use cobble::{Config, DedicatedCompactor, RemoteCompactionServer};
use cobble_web_monitor::{MonitorConfig, MonitorConfigSource, MonitorServer};
use log::LevelFilter::Info;
use std::error::Error;

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
         cobble-cli compact --config <path> --db-id <db-id> [--poll-interval <ms>]\n"
    );
}

fn run_compact(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let mut config_path: Option<String> = None;
    let mut db_id: Option<String> = None;
    let mut poll_interval_ms: Option<u64> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = args.next();
            }
            "--db-id" => {
                db_id = args.next();
            }
            "--poll-interval" => {
                poll_interval_ms = args.next().and_then(|s| s.parse().ok());
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

    let config_path = config_path.ok_or("compact requires --config <path>")?;
    let db_id = db_id.ok_or("compact requires --db-id <db-id>")?;

    let mut config = Config::from_path(&config_path)?;
    config.log_console = true;
    if let Some(ms) = poll_interval_ms {
        config.compaction_dedicated_poll_interval_ms = ms;
    }

    let compactor = DedicatedCompactor::open(config, &db_id)?;
    compactor.run()?;
    Ok(())
}
