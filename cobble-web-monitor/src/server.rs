use crate::error::{Error, Result};
use axum::Router;
use axum::extract::{Json as AxumJson, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cobble::{Config, ReadOptions, Reader, ReaderConfig, ScanOptions};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const DEFAULT_INSPECT_LIMIT: usize = 100;
const DEFAULT_INSPECT_MAX_LIMIT: usize = 1000;

#[derive(Clone, Debug)]
pub enum MonitorConfigSource {
    Config(Box<Config>),
    ConfigPath(String),
}

#[derive(Clone, Debug)]
pub struct MonitorConfig {
    pub source: MonitorConfigSource,
    pub bind_addr: String,
    pub global_snapshot_id: Option<u64>,
    pub inspect_default_limit: usize,
    pub inspect_max_limit: usize,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            source: MonitorConfigSource::Config(Box::default()),
            bind_addr: "127.0.0.1:0".to_string(),
            global_snapshot_id: None,
            inspect_default_limit: DEFAULT_INSPECT_LIMIT,
            inspect_max_limit: DEFAULT_INSPECT_MAX_LIMIT,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MonitorServerHandle {
    bind_addr: SocketAddr,
}

impl MonitorServerHandle {
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.bind_addr)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InspectMode {
    Lookup,
    Scan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectQuery {
    pub mode: InspectMode,
    pub bucket: u16,
    pub keys: Vec<Vec<u8>>,
    pub prefix: Option<Vec<u8>>,
    pub start_after: Option<Vec<u8>>,
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct InspectColumnValue {
    pub b64: String,
    pub utf8: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct InspectItem {
    pub key_b64: String,
    pub key_utf8: Option<String>,
    pub columns: Vec<Option<InspectColumnValue>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetaResponse {
    pub bind_addr: String,
    pub read_mode: String,
    pub configured_snapshot_id: Option<u64>,
    pub current_global_snapshot_id: u64,
    pub total_buckets: u32,
    pub shard_snapshot_count: usize,
    pub inspect_default_limit: usize,
    pub inspect_max_limit: usize,
    pub uptime_millis: u64,
}

#[derive(Clone, Debug, Serialize)]
struct SnapshotSummary {
    id: u64,
    total_buckets: u32,
    shard_snapshot_count: usize,
    is_current: bool,
}

#[derive(Clone, Debug, Serialize)]
struct SnapshotListResponse {
    current_global_snapshot_id: u64,
    snapshots: Vec<SnapshotSummary>,
}

struct AppState {
    proxy: Mutex<Reader>,
    read_proxy_config: ReaderConfig,
    inspect_default_limit: usize,
    inspect_max_limit: usize,
    bind_addr: Mutex<Option<SocketAddr>>,
    started_at: Instant,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = status_for_error(&self);
        (
            status,
            Json(ErrorBody {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

pub struct MonitorServer {
    runtime: Arc<Runtime>,
    config: MonitorConfig,
    state: Arc<AppState>,
    listener_task: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MonitorServer {
    pub fn new(config: MonitorConfig) -> Result<Self> {
        if config.inspect_default_limit == 0 {
            return Err(Error::InputError(
                "inspect_default_limit must be greater than 0".to_string(),
            ));
        }
        if config.inspect_max_limit == 0 {
            return Err(Error::InputError(
                "inspect_max_limit must be greater than 0".to_string(),
            ));
        }
        if config.inspect_default_limit > config.inspect_max_limit {
            return Err(Error::InputError(
                "inspect_default_limit must be <= inspect_max_limit".to_string(),
            ));
        }

        let cobble_config = load_cobble_config(&config.source)?;
        let read_proxy_config = ReaderConfig::from_config(&cobble_config);
        let proxy = if let Some(snapshot_id) = config.global_snapshot_id {
            Reader::open(read_proxy_config.clone(), snapshot_id)?
        } else {
            Reader::open_current(read_proxy_config.clone())?
        };

        let runtime = Arc::new(
            Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|err| Error::HttpServerError(format!("build runtime failed: {}", err)))?,
        );

        Ok(Self {
            runtime,
            state: Arc::new(AppState {
                proxy: Mutex::new(proxy),
                read_proxy_config,
                inspect_default_limit: config.inspect_default_limit,
                inspect_max_limit: config.inspect_max_limit,
                bind_addr: Mutex::new(None),
                started_at: Instant::now(),
            }),
            config,
            listener_task: None,
            shutdown_tx: None,
        })
    }

    pub fn serve(&mut self) -> Result<MonitorServerHandle> {
        if self.listener_task.is_some() {
            return Err(Error::HttpServerError(
                "monitor server already running".to_string(),
            ));
        }

        let listener = self.runtime.block_on(async {
            TcpListener::bind(self.config.bind_addr.as_str())
                .await
                .map_err(|err| {
                    Error::HttpServerError(format!(
                        "bind {} failed: {}",
                        self.config.bind_addr, err
                    ))
                })
        })?;
        let bind_addr = listener
            .local_addr()
            .map_err(|err| Error::HttpServerError(format!("local_addr failed: {}", err)))?;
        {
            let mut guard = self.state.bind_addr.lock().map_err(|_| {
                Error::HttpServerError("monitor state bind_addr lock poisoned".to_string())
            })?;
            *guard = Some(bind_addr);
        }

        let app = Router::new()
            .route("/healthz", get(healthz_handler))
            .route("/api/v1/meta", get(meta_handler))
            .route("/api/v1/inspect", get(inspect_handler))
            .route("/api/v1/snapshots", get(list_snapshots_handler))
            .route("/api/v1/mode", post(switch_mode_handler))
            .with_state(Arc::clone(&self.state));

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        self.shutdown_tx = Some(shutdown_tx);
        self.listener_task = Some(self.runtime.spawn(async move {
            let serve = axum::serve(listener, app).with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            });
            let _ = serve.await;
        }));

        log::info!("cobble-web-monitor listening on {}", bind_addr);
        Ok(MonitorServerHandle { bind_addr })
    }

    pub fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(listener_task) = self.listener_task.take() {
            self.runtime.block_on(async {
                let _ = listener_task.await;
            });
        }
        Ok(())
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.state.bind_addr.lock().ok().and_then(|guard| *guard)
    }
}

impl Drop for MonitorServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn load_cobble_config(source: &MonitorConfigSource) -> Result<Config> {
    match source {
        MonitorConfigSource::Config(config) => Ok((**config).clone()),
        MonitorConfigSource::ConfigPath(path) => {
            if path.trim().is_empty() {
                return Err(Error::InputError(
                    "config path in MonitorConfigSource::ConfigPath must not be empty".to_string(),
                ));
            }
            Config::from_path(path).map_err(Into::into)
        }
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn healthz_handler() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn meta_handler(
    State(state): State<Arc<AppState>>,
) -> std::result::Result<Json<MetaResponse>, Error> {
    let response = build_meta_response(&state)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
struct InspectParams {
    mode: Option<String>,
    bucket: Option<u16>,
    keys: Option<String>,
    keys_b64: Option<String>,
    prefix: Option<String>,
    prefix_b64: Option<String>,
    start_after: Option<String>,
    start_after_b64: Option<String>,
    limit: Option<usize>,
}

async fn inspect_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<InspectParams>,
) -> std::result::Result<Json<InspectResponse>, Error> {
    let query = parse_inspect_query(
        &params,
        state.inspect_default_limit,
        state.inspect_max_limit,
    )?;
    let response = run_inspect_query(&state, &query)?;
    Ok(Json(response))
}

async fn list_snapshots_handler(
    State(state): State<Arc<AppState>>,
) -> std::result::Result<Json<SnapshotListResponse>, Error> {
    let guard = state
        .proxy
        .lock()
        .map_err(|_| Error::HttpServerError("monitor proxy lock poisoned".to_string()))?;
    let summaries = guard.list_global_snapshots()?;
    let current_id = guard.current_global_snapshot().id;
    let snapshots = summaries
        .into_iter()
        .map(|summary| SnapshotSummary {
            id: summary.id,
            total_buckets: summary.total_buckets,
            shard_snapshot_count: summary.shard_snapshot_count,
            is_current: summary.is_current,
        })
        .collect();
    Ok(Json(SnapshotListResponse {
        current_global_snapshot_id: current_id,
        snapshots,
    }))
}

#[derive(Debug, Deserialize)]
struct SwitchModeRequest {
    mode: String,
    snapshot_id: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SwitchModeResponse {
    read_mode: String,
    configured_snapshot_id: Option<u64>,
}

async fn switch_mode_handler(
    State(state): State<Arc<AppState>>,
    AxumJson(request): AxumJson<SwitchModeRequest>,
) -> std::result::Result<Json<SwitchModeResponse>, Error> {
    let state_for_blocking = Arc::clone(&state);
    let response = tokio::task::spawn_blocking(move || {
        switch_mode_blocking(state_for_blocking.as_ref(), request)
    })
    .await
    .map_err(|err| Error::HttpServerError(format!("switch mode task failed: {}", err)))??;
    Ok(Json(response))
}

fn switch_mode_blocking(
    state: &AppState,
    request: SwitchModeRequest,
) -> Result<SwitchModeResponse> {
    let proxy = match request.mode.as_str() {
        "current" => Reader::open_current(state.read_proxy_config.clone())?,
        "snapshot" => {
            let snapshot_id = request.snapshot_id.ok_or_else(|| {
                Error::InputError("`snapshot_id` is required when mode=snapshot".to_string())
            })?;
            Reader::open(state.read_proxy_config.clone(), snapshot_id)?
        }
        raw => {
            return Err(Error::InputError(format!(
                "invalid `mode`: {} (expect current|snapshot)",
                raw
            )));
        }
    };

    let mut guard = state
        .proxy
        .lock()
        .map_err(|_| Error::HttpServerError("monitor proxy lock poisoned".to_string()))?;
    *guard = proxy;

    Ok(SwitchModeResponse {
        read_mode: guard.read_mode().to_string(),
        configured_snapshot_id: guard.configured_snapshot_id(),
    })
}

fn parse_inspect_query(
    params: &InspectParams,
    default_limit: usize,
    max_limit: usize,
) -> Result<InspectQuery> {
    let mode = match params.mode.as_deref().unwrap_or("scan") {
        "lookup" => InspectMode::Lookup,
        "scan" => InspectMode::Scan,
        raw => {
            return Err(Error::InputError(format!(
                "invalid `mode`: {} (expect lookup|scan)",
                raw
            )));
        }
    };

    let bucket = params
        .bucket
        .ok_or_else(|| Error::InputError("query param `bucket` is required".to_string()))?;

    match mode {
        InspectMode::Lookup => {
            let keys = parse_keys(params)?;
            if keys.is_empty() {
                return Err(Error::InputError(
                    "lookup mode requires non-empty `keys` or `keys_b64`".to_string(),
                ));
            }
            Ok(InspectQuery {
                mode,
                bucket,
                keys,
                prefix: None,
                start_after: None,
                limit: default_limit,
            })
        }
        InspectMode::Scan => {
            let prefix = decode_optional_bytes(
                params.prefix.as_deref(),
                params.prefix_b64.as_deref(),
                "prefix_b64",
            )?
            .ok_or_else(|| {
                Error::InputError("scan mode requires `prefix` or `prefix_b64`".to_string())
            })?;
            if prefix.is_empty() {
                return Err(Error::InputError(
                    "`prefix` must not be empty in scan mode".to_string(),
                ));
            }

            let start_after = decode_optional_bytes(
                params.start_after.as_deref(),
                params.start_after_b64.as_deref(),
                "start_after_b64",
            )?;
            if let Some(start_after) = start_after.as_ref()
                && !start_after.starts_with(prefix.as_slice())
            {
                return Err(Error::InputError(
                    "`start_after` must share the same prefix".to_string(),
                ));
            }

            let limit = params.limit.unwrap_or(default_limit);
            if limit == 0 {
                return Err(Error::InputError(
                    "`limit` must be greater than 0".to_string(),
                ));
            }

            Ok(InspectQuery {
                mode,
                bucket,
                keys: Vec::new(),
                prefix: Some(prefix),
                start_after,
                limit: limit.min(max_limit),
            })
        }
    }
}

fn parse_keys(params: &InspectParams) -> Result<Vec<Vec<u8>>> {
    if let Some(raw) = params.keys_b64.as_deref() {
        let mut output = Vec::new();
        for part in raw.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            output.push(STANDARD.decode(trimmed).map_err(|err| {
                Error::InputError(format!(
                    "invalid `keys_b64` base64 item `{}`: {}",
                    trimmed, err
                ))
            })?);
        }
        return Ok(output);
    }
    if let Some(raw) = params.keys.as_deref() {
        let output = raw
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| part.as_bytes().to_vec())
            .collect();
        return Ok(output);
    }
    Ok(Vec::new())
}

fn decode_optional_bytes(
    plain_value: Option<&str>,
    b64_value: Option<&str>,
    b64_field_name: &str,
) -> Result<Option<Vec<u8>>> {
    if let Some(value) = b64_value {
        let decoded = STANDARD.decode(value).map_err(|err| {
            Error::InputError(format!("invalid `{}` base64: {}", b64_field_name, err))
        })?;
        return Ok(Some(decoded));
    }
    Ok(plain_value.map(|value| value.as_bytes().to_vec()))
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum InspectModeResponse {
    Lookup,
    Scan,
}

#[derive(Serialize)]
struct LookupResultItem {
    key_b64: String,
    key_utf8: Option<String>,
    value: Option<Vec<Option<InspectColumnValue>>>,
}

#[derive(Serialize)]
struct ScanResult {
    prefix_b64: String,
    start_after_b64: Option<String>,
    limit: usize,
    has_more: bool,
    next_start_after_b64: Option<String>,
    items: Vec<InspectItem>,
}

#[derive(Serialize)]
struct InspectResponse {
    mode: InspectModeResponse,
    bucket: u16,
    refreshed: bool,
    lookup: Option<Vec<LookupResultItem>>,
    scan: Option<ScanResult>,
}

fn run_inspect_query(state: &Arc<AppState>, query: &InspectQuery) -> Result<InspectResponse> {
    let mut guard = state
        .proxy
        .lock()
        .map_err(|_| Error::HttpServerError("monitor proxy lock poisoned".to_string()))?;
    let refreshed = if guard.read_mode() == "current" {
        guard.refresh()?;
        true
    } else {
        false
    };

    match query.mode {
        InspectMode::Lookup => {
            let mut output = Vec::with_capacity(query.keys.len());
            for key in &query.keys {
                let value = guard.get(query.bucket, key.as_slice(), &ReadOptions::default())?;
                output.push(LookupResultItem {
                    key_b64: STANDARD.encode(key),
                    key_utf8: std::str::from_utf8(key.as_slice())
                        .ok()
                        .map(|value| value.to_string()),
                    value: value.map(convert_columns),
                });
            }
            Ok(InspectResponse {
                mode: InspectModeResponse::Lookup,
                bucket: query.bucket,
                refreshed,
                lookup: Some(output),
                scan: None,
            })
        }
        InspectMode::Scan => {
            let prefix = query
                .prefix
                .as_ref()
                .ok_or_else(|| Error::InputError("scan query missing prefix".to_string()))?;
            let range_end = next_prefix(prefix.as_slice()).ok_or_else(|| {
                Error::InputError(
                    "prefix is all 0xFF bytes and cannot build an exclusive upper bound"
                        .to_string(),
                )
            })?;

            let scan_start = query
                .start_after
                .as_ref()
                .cloned()
                .unwrap_or_else(|| prefix.clone());

            let mut iter = guard.scan(
                query.bucket,
                scan_start.as_slice()..range_end.as_slice(),
                &ScanOptions::default(),
            )?;

            let mut items = Vec::with_capacity(query.limit.saturating_add(1));
            for row in &mut iter {
                let (key, columns) = row?;
                if !key.starts_with(prefix.as_slice()) {
                    continue;
                }
                if let Some(start_after) = query.start_after.as_ref()
                    && key.as_ref() <= start_after.as_slice()
                {
                    continue;
                }
                items.push(InspectItem {
                    key_b64: STANDARD.encode(key.as_ref()),
                    key_utf8: std::str::from_utf8(key.as_ref())
                        .ok()
                        .map(|value| value.to_string()),
                    columns: convert_columns(columns),
                });
                if items.len() > query.limit {
                    break;
                }
            }

            let has_more = items.len() > query.limit;
            if has_more {
                let _ = items.pop();
            }
            let next_start_after_b64 = if has_more {
                items.last().map(|item| item.key_b64.clone())
            } else {
                None
            };

            Ok(InspectResponse {
                mode: InspectModeResponse::Scan,
                bucket: query.bucket,
                refreshed,
                lookup: None,
                scan: Some(ScanResult {
                    prefix_b64: STANDARD.encode(prefix),
                    start_after_b64: query
                        .start_after
                        .as_ref()
                        .map(|start_after| STANDARD.encode(start_after)),
                    limit: query.limit,
                    has_more,
                    next_start_after_b64,
                    items,
                }),
            })
        }
    }
}

fn convert_columns<T: AsRef<[u8]>>(columns: Vec<Option<T>>) -> Vec<Option<InspectColumnValue>> {
    columns
        .into_iter()
        .map(|column| {
            column.map(|bytes| InspectColumnValue {
                b64: STANDARD.encode(bytes.as_ref()),
                utf8: std::str::from_utf8(bytes.as_ref())
                    .ok()
                    .map(|value| value.to_string()),
            })
        })
        .collect()
}

fn build_meta_response(state: &Arc<AppState>) -> Result<MetaResponse> {
    let bind_addr = state
        .bind_addr
        .lock()
        .map_err(|_| Error::HttpServerError("monitor state bind_addr lock poisoned".to_string()))?
        .map(|addr| addr.to_string())
        .unwrap_or_default();
    let guard = state
        .proxy
        .lock()
        .map_err(|_| Error::HttpServerError("monitor proxy lock poisoned".to_string()))?;
    let current = guard.current_global_snapshot();

    Ok(MetaResponse {
        bind_addr,
        read_mode: guard.read_mode().to_string(),
        configured_snapshot_id: guard.configured_snapshot_id(),
        current_global_snapshot_id: current.id,
        total_buckets: current.total_buckets,
        shard_snapshot_count: current.shard_snapshots.len(),
        inspect_default_limit: state.inspect_default_limit,
        inspect_max_limit: state.inspect_max_limit,
        uptime_millis: state.started_at.elapsed().as_millis() as u64,
    })
}

fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != 0xFF {
            upper[idx] = upper[idx].saturating_add(1);
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }
    None
}

fn status_for_error(err: &Error) -> StatusCode {
    match err {
        Error::InputError(_) | Error::AddrParseError(_) => StatusCode::BAD_REQUEST,
        Error::CobbleError(_) | Error::HttpServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_prefix() {
        assert_eq!(next_prefix(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(next_prefix(&[0x10, 0xFF]), Some(vec![0x11]));
        assert_eq!(next_prefix(&[0xFF, 0xFF]), None);
    }

    #[test]
    fn test_parse_scan_query() {
        let params = InspectParams {
            mode: Some("scan".to_string()),
            bucket: Some(7),
            keys: None,
            keys_b64: None,
            prefix: Some("user:".to_string()),
            prefix_b64: None,
            start_after: None,
            start_after_b64: None,
            limit: Some(200),
        };
        let query = parse_inspect_query(&params, 100, 500).expect("parse query");
        assert_eq!(query.mode, InspectMode::Scan);
        assert_eq!(query.bucket, 7);
        assert_eq!(query.prefix, Some(b"user:".to_vec()));
        assert_eq!(query.limit, 200);
    }

    #[test]
    fn test_parse_lookup_query_base64_keys() {
        let params = InspectParams {
            mode: Some("lookup".to_string()),
            bucket: Some(1),
            keys: None,
            keys_b64: Some(format!(
                "{},{}",
                STANDARD.encode(b"k1"),
                STANDARD.encode(b"k2")
            )),
            prefix: None,
            prefix_b64: None,
            start_after: None,
            start_after_b64: None,
            limit: None,
        };
        let query = parse_inspect_query(&params, 50, 100).expect("parse query");
        assert_eq!(query.mode, InspectMode::Lookup);
        assert_eq!(query.keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert_eq!(query.limit, 50);
    }

    #[test]
    fn test_parse_scan_query_reject_empty_prefix() {
        let params = InspectParams {
            mode: Some("scan".to_string()),
            bucket: Some(0),
            keys: None,
            keys_b64: None,
            prefix: Some(String::new()),
            prefix_b64: None,
            start_after: None,
            start_after_b64: None,
            limit: None,
        };
        let err = parse_inspect_query(&params, 10, 20).expect_err("empty prefix rejected");
        assert!(err.to_string().contains("must not be empty"));
    }
}
