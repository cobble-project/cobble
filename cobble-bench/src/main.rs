use cobble::{Config, Db, VolumeDescriptor};
use log::LevelFilter::Debug;
use rand_core::Rng;
use rand_core::SeedableRng;
use rand_xorshift::XorShiftRng;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const DEFAULT_KEY_COUNT: u64 = 100_000_000;
const DEFAULT_DB_PATH: &str = "/tmp/cobble-bench/bulk-load";
const DEFAULT_SEED: u64 = 0x6a09e667f3bcc909;
const DEFAULT_KEY_LEN: usize = 8;

struct Args {
    key_count: u64,
    db_path: PathBuf,
    seed: u64,
    key_len: usize,
    remote_compactor: Option<String>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            key_count: DEFAULT_KEY_COUNT,
            db_path: PathBuf::from(DEFAULT_DB_PATH),
            seed: DEFAULT_SEED,
            key_len: DEFAULT_KEY_LEN,
            remote_compactor: None,
        }
    }
}

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    if let Err(message) = run(args) {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

fn usage() -> &'static str {
    "Usage: cobble-bench [--keys <count>] [--db-path <path>] [--seed <seed>] [--key-len <bytes>] [--remote-compactor <host:port>]"
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--keys" => {
                args.key_count = parse_value(&mut iter, "--keys")?;
            }
            "--db-path" => {
                args.db_path = PathBuf::from(parse_string(&mut iter, "--db-path")?);
            }
            "--seed" => {
                args.seed = parse_value(&mut iter, "--seed")?;
            }
            "--key-len" | "--key-length" => {
                args.key_len = parse_value(&mut iter, "--key-len")?;
            }
            "--remote-compactor" => {
                args.remote_compactor = Some(parse_string(&mut iter, "--remote-compactor")?);
            }
            "-h" | "--help" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            _ => {
                return Err(format!("Unknown option: {arg}\n{}", usage()));
            }
        }
    }
    if args.key_count == 0 {
        return Err("Key count must be greater than 0.".to_string());
    }
    if args.key_len < 8 {
        return Err("Key length must be at least 8 bytes.".to_string());
    }
    Ok(args)
}

fn parse_string(iter: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    iter.next()
        .ok_or_else(|| format!("Missing value for {name}.\n{}", usage()))
}

fn parse_value<T: std::str::FromStr>(
    iter: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, String> {
    let raw = parse_string(iter, name)?;
    raw.parse::<T>()
        .map_err(|_| format!("Invalid value for {name}: {raw}.\n{}", usage()))
}

trait KeyGenerator {
    fn next_key(&mut self) -> Option<&[u8]>;
}

struct RandomKeyGenerator {
    key_count: u64,
    _seed: u64,
    index: u64,
    buffer: Vec<u8>,
    rng: XorShiftRng,
}

impl RandomKeyGenerator {
    fn new(key_count: u64, key_len: usize, seed: u64) -> Self {
        Self {
            key_count,
            _seed: seed,
            index: 0,
            buffer: vec![0u8; key_len],
            rng: XorShiftRng::seed_from_u64(seed),
        }
    }
}

impl KeyGenerator for RandomKeyGenerator {
    fn next_key(&mut self) -> Option<&[u8]> {
        if self.index >= self.key_count {
            return None;
        }
        self.index += 1;
        self.rng.fill_bytes(self.buffer.as_mut_slice());
        Some(self.buffer.as_slice())
    }
}

fn run(args: Args) -> Result<(), String> {
    prepare_db_dir(&args.db_path)?;
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", args.db_path.display())),
        compaction_remote_addr: args.remote_compactor.clone(),
        log_level: Debug,
        log_console: true,
        ..Config::default()
    };
    let db = Db::open(config).map_err(|err| format!("Failed to open db: {err}"))?;

    println!(
        "bulk load: keys={} key_len={} db_path={} seed={} remote_compactor={}",
        args.key_count,
        args.key_len,
        args.db_path.display(),
        args.seed,
        args.remote_compactor.as_deref().unwrap_or("local")
    );

    let progress_every = (args.key_count / 100).max(1);
    let start = Instant::now();
    let mut inserted = 0u64;
    let mut generator = RandomKeyGenerator::new(args.key_count, args.key_len, args.seed);

    while let Some(key_bytes) = generator.next_key() {
        db.put(key_bytes, 0, key_bytes)
            .map_err(|err| format!("Write failed at key {}: {err}", inserted + 1))?;
        inserted += 1;

        if inserted.is_multiple_of(progress_every) {
            let elapsed = start.elapsed();
            let seconds = elapsed.as_secs_f64().max(1e-9);
            let rate = inserted as f64 / seconds;
            eprintln!(
                "progress: {inserted}/{} keys ({:.2}%) {:.2} keys/s",
                args.key_count,
                (inserted as f64 / args.key_count as f64) * 100.0,
                rate
            );
        }
    }

    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64().max(1e-9);
    let rate = args.key_count as f64 / seconds;
    println!(
        "loaded {} keys in {:.2}s ({:.2} keys/s)",
        args.key_count, seconds, rate
    );

    db.snapshot()
        .map_err(|err| format!("Failed to flush snapshot: {err}"))?;
    db.close()
        .map_err(|err| format!("Failed to close db: {err}"))?;
    Ok(())
}

fn prepare_db_dir(path: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_dir_all(path)
            .map_err(|err| format!("Failed to remove {}: {err}", path.display()))?;
    }
    fs::create_dir_all(path)
        .map_err(|err| format!("Failed to create {}: {err}", path.display()))?;
    Ok(())
}
