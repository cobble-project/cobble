use cobble::{Config, ReadOptions, Reader, ReaderConfig, SingleNodeDb, VolumeDescriptor};
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
const DEFAULT_VALUE_LEN: usize = 8;
const SEPARATED_BULKLOAD_MIN_VALUE_LEN: usize = 1024;
const SEPARATED_BULKLOAD_DEFAULT_VALUE_LEN: usize = 1024;
const SEPARATED_BULKLOAD_THRESHOLD: usize = 512;
const DEFAULT_READ_COUNT: Option<u64> = None;

#[derive(Clone, Copy, Debug)]
enum BenchMode {
    BulkLoad,
    BulkLoadSeparated,
    RandomRead,
}

struct Args {
    key_count: u64,
    db_path: PathBuf,
    seed: u64,
    key_len: usize,
    value_len: usize,
    remote_compactor: Option<String>,
    mode: BenchMode,
    read_count: Option<u64>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            key_count: DEFAULT_KEY_COUNT,
            db_path: PathBuf::from(DEFAULT_DB_PATH),
            seed: DEFAULT_SEED,
            key_len: DEFAULT_KEY_LEN,
            value_len: DEFAULT_VALUE_LEN,
            remote_compactor: None,
            mode: BenchMode::BulkLoad,
            read_count: DEFAULT_READ_COUNT,
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
    "Usage: cobble-bench [--mode <bulkload|bulkload-separated|randomread>] [--keys <count>] [--reads <count>] [--db-path <path>] [--seed <seed>] [--key-len <bytes>] [--value-len <bytes>] [--remote-compactor <host:port>]"
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut value_len_set = false;
    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--keys" => {
                args.key_count = parse_value(&mut iter, "--keys")?;
            }
            "--reads" => {
                args.read_count = Some(parse_value(&mut iter, "--reads")?);
            }
            "--db-path" => {
                args.db_path = PathBuf::from(parse_string(&mut iter, "--db-path")?);
            }
            "--seed" => {
                args.seed = parse_value(&mut iter, "--seed")?;
            }
            "--mode" => {
                let mode = parse_string(&mut iter, "--mode")?;
                args.mode = match mode.as_str() {
                    "bulkload" => BenchMode::BulkLoad,
                    "bulkload-separated" => BenchMode::BulkLoadSeparated,
                    "randomread" => BenchMode::RandomRead,
                    _ => {
                        return Err(format!(
                            "Invalid mode: {mode}. Expected bulkload, bulkload-separated, or randomread.\n{}",
                            usage()
                        ));
                    }
                };
            }
            "--key-len" | "--key-length" => {
                args.key_len = parse_value(&mut iter, "--key-len")?;
            }
            "--value-len" | "--value-length" => {
                args.value_len = parse_value(&mut iter, "--value-len")?;
                value_len_set = true;
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
    if !value_len_set {
        args.value_len = if matches!(args.mode, BenchMode::BulkLoadSeparated) {
            SEPARATED_BULKLOAD_DEFAULT_VALUE_LEN
        } else {
            args.key_len
        };
    }
    if args.value_len == 0 {
        return Err("Value length must be greater than 0.".to_string());
    }
    if matches!(args.mode, BenchMode::BulkLoadSeparated)
        && args.value_len < SEPARATED_BULKLOAD_MIN_VALUE_LEN
    {
        return Err(format!(
            "For bulkload-separated mode, value length must be at least {} bytes.",
            SEPARATED_BULKLOAD_MIN_VALUE_LEN
        ));
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
    if matches!(
        args.mode,
        BenchMode::BulkLoad | BenchMode::BulkLoadSeparated
    ) {
        prepare_db_dir(&args.db_path)?;
    } else if !args.db_path.exists() {
        return Err(format!(
            "Database path {} does not exist.",
            args.db_path.display()
        ));
    }
    let value_separation_threshold = if matches!(args.mode, BenchMode::BulkLoadSeparated) {
        SEPARATED_BULKLOAD_THRESHOLD
    } else {
        usize::MAX
    };
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", args.db_path.display())),
        compaction_remote_addr: args.remote_compactor.clone(),
        value_separation_threshold,
        log_level: Debug,
        log_console: true,
        ..Config::default()
    };

    match args.mode {
        BenchMode::BulkLoad | BenchMode::BulkLoadSeparated => {
            let db = SingleNodeDb::open(config.clone())
                .map_err(|err| format!("Failed to open db: {err}"))?;

            println!(
                "bulk load: mode={:?} keys={} key_len={} value_len={} separation_threshold={} db_path={} seed={} remote_compactor={}",
                args.mode,
                args.key_count,
                args.key_len,
                args.value_len,
                value_separation_threshold,
                args.db_path.display(),
                args.seed,
                args.remote_compactor.as_deref().unwrap_or("local")
            );

            let progress_every = (args.key_count / 100).max(1);
            let start = Instant::now();
            let mut inserted = 0u64;
            let mut generator = RandomKeyGenerator::new(args.key_count, args.key_len, args.seed);
            let mut value = vec![0u8; args.value_len];

            while let Some(key_bytes) = generator.next_key() {
                fill_value_from_key(&mut value, key_bytes);
                db.put(0, key_bytes, 0, &value)
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
        }
        BenchMode::RandomRead => {
            let read_count = args.read_count.unwrap_or(args.key_count);
            if read_count > args.key_count {
                return Err(format!(
                    "Read count {} exceeds key count {}.",
                    read_count, args.key_count
                ));
            }
            println!(
                "random read: reads={} key_len={} db_path={} seed={}",
                read_count,
                args.key_len,
                args.db_path.display(),
                args.seed
            );
            let mut proxy = Reader::open_current(ReaderConfig::from_config(&config))
                .map_err(|err| format!("Failed to open read proxy: {err}"))?;
            let progress_every = (read_count / 100).max(1);
            let start = Instant::now();
            let mut reads = 0u64;
            let mut hits = 0u64;
            let mut generator = RandomKeyGenerator::new(read_count, args.key_len, args.seed);
            let read_options = ReadOptions::default();
            while let Some(key_bytes) = generator.next_key() {
                if proxy
                    .get(0, key_bytes, &read_options)
                    .map_err(|err| format!("Read failed at key {}: {err}", reads + 1))?
                    .is_some()
                {
                    hits += 1;
                }
                reads += 1;
                if reads.is_multiple_of(progress_every) {
                    let elapsed = start.elapsed();
                    let seconds = elapsed.as_secs_f64().max(1e-9);
                    let rate = reads as f64 / seconds;
                    eprintln!(
                        "progress: {reads}/{} reads ({:.2}%) {:.2} reads/s hit_rate={:.2}%",
                        read_count,
                        (reads as f64 / read_count as f64) * 100.0,
                        rate,
                        (hits as f64 / reads as f64) * 100.0
                    );
                }
            }
            let elapsed = start.elapsed();
            let seconds = elapsed.as_secs_f64().max(1e-9);
            let rate = read_count as f64 / seconds;
            let hit_rate = if read_count == 0 {
                0.0
            } else {
                hits as f64 / read_count as f64 * 100.0
            };
            println!(
                "read {} keys in {:.2}s ({:.2} reads/s) hit_rate={:.2}%",
                read_count, seconds, rate, hit_rate
            );
        }
    }
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

fn fill_value_from_key(value: &mut [u8], key: &[u8]) {
    for (idx, byte) in value.iter_mut().enumerate() {
        *byte = key[idx % key.len()];
    }
}
