use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::{HeaderName, HeaderValue, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use prometheus::{CounterVec, Encoder, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder};
use tracing_subscriber::EnvFilter;

use crate::error::{CasError, CasResult};

pub const REQUEST_ID_HEADER: &str = "x-request-id";

tokio::task_local! {
    /// Request id for the current task, readable from `IntoResponse` impls so
    /// error bodies can carry it without threading it through every handler.
    pub static REQUEST_ID_SCOPE: String;
}

/// Request id of the task currently being served, if any.
pub fn current_request_id() -> Option<String> {
    REQUEST_ID_SCOPE.try_with(|id| id.clone()).ok()
}

static HTTP_METRICS: OnceLock<HttpMetrics> = OnceLock::new();
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub struct HttpMetrics {
    pub requests_total: CounterVec,
    pub request_duration_seconds: HistogramVec,
}

impl HttpMetrics {
    pub fn new(registry: &Registry) -> CasResult<Self> {
        let requests_total = CounterVec::new(
            Opts::new(
                "axiom_http_requests_total",
                "Total HTTP requests by method, path, and status code",
            ),
            &["method", "path", "status"],
        )
        .map_err(|e| CasError::Store(format!("prometheus register axiom_http_requests_total: {e}")))?;

        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "axiom_http_request_duration_seconds",
                "HTTP request latency in seconds by method and path",
            )
            .buckets(
                prometheus::exponential_buckets(0.01, 2.0, 11)
                    .map_err(|e| CasError::Store(format!("prometheus bucket spec: {e}")))?,
            ),
            &["method", "path"],
        )
        .map_err(|e| {
            CasError::Store(format!(
                "prometheus register axiom_http_request_duration_seconds: {e}"
            ))
        })?;

        registry
            .register(Box::new(requests_total.clone()))
            .map_err(|e| CasError::Store(format!("prometheus register requests_total: {e}")))?;
        registry
            .register(Box::new(request_duration_seconds.clone()))
            .map_err(|e| CasError::Store(format!("prometheus register request_duration_seconds: {e}")))?;

        Ok(Self {
            requests_total,
            request_duration_seconds,
        })
    }

    pub fn global() -> &'static Self {
        HTTP_METRICS.get_or_init(|| {
            Self::new(prometheus::default_registry())
                .expect("observability metrics registration should succeed")
        })
    }

    pub fn record(&self, method: &str, path: &str, status: u16, duration: Duration) {
        let status = status.to_string();
        self.requests_total
            .with_label_values(&[method, path, &status])
            .inc();
        self.request_duration_seconds
            .with_label_values(&[method, path])
            .observe(duration.as_secs_f64());
    }
}

/// Count a quota rejection by plan tier (P1-1/P1-2).
pub fn record_quota_rejection(plan: Option<&str>) {
    use std::sync::OnceLock;

    static COUNTER: OnceLock<prometheus::IntCounterVec> = OnceLock::new();
    let counter = COUNTER.get_or_init(|| {
        let vec = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "axiom_quota_rejections_total",
                "Upload/push requests rejected because the tenant exceeded its quota",
            ),
            &["plan"],
        )
        .expect("quota rejection metric");
        let _ = prometheus::default_registry().register(Box::new(vec.clone()));
        vec
    });
    counter
        .with_label_values(&[crate::quota::QuotaPolicy::plan_name(plan)])
        .inc();
}

// ---------------------------------------------------------------------------
// Domain metrics (P1-2)
// ---------------------------------------------------------------------------

fn counter(name: &'static str, help: &'static str, labels: &'static [&'static str]) -> prometheus::IntCounterVec {
    use std::sync::OnceLock;

    // Leaked once per metric name: a fixed, process-lifetime set.
    static REGISTRY: OnceLock<parking_lot::Mutex<std::collections::HashMap<&'static str, prometheus::IntCounterVec>>> =
        OnceLock::new();
    let registry = REGISTRY.get_or_init(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

    let mut guard = registry.lock();
    if let Some(existing) = guard.get(name) {
        return existing.clone();
    }
    let vec = prometheus::IntCounterVec::new(prometheus::Opts::new(name, help), labels)
        .expect("metric definition is valid");
    let _ = prometheus::default_registry().register(Box::new(vec.clone()));
    guard.insert(name, vec.clone());
    vec
}

/// Pre-register label combinations so dashboards show `0` instead of "no data"
/// before the first real event.
pub fn init_domain_metrics() {
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        for plan in ["free", "pro", "enterprise"] {
            counter(
                "axiom_quota_rejections_total",
                "Upload/push requests rejected because the tenant exceeded its quota",
                &["plan"],
            )
            .with_label_values(&[plan])
            .inc_by(0);
        }
        for reason in [
            "missing_bearer",
            "invalid_token",
            "token_expired",
            "untrusted_proxy",
            "missing_identity_header",
        ] {
            counter(
                "axiom_auth_failures_total",
                "Rejected authentication attempts, by reason",
                &["reason"],
            )
            .with_label_values(&[reason])
            .inc_by(0);
        }
        for operation in [
            "list_refs",
            "negotiate_push",
            "upload_pack",
            "finalize_refs",
            "negotiate_pull",
            "download_pack",
            "clone_init",
            "clone_download",
        ] {
            for result in ["ok", "error"] {
                counter(
                    "axiom_sync_operations_total",
                    "Sync RPCs handled, by operation and result",
                    &["operation", "result"],
                )
                .with_label_values(&[operation, result])
                .inc_by(0);
            }
        }
        counter(
            "axiom_storage_operations_total",
            "Storage operations, by backend, operation and result",
            &["backend", "operation", "result"],
        )
        .with_label_values(&["chunk_store", "put", "ok"])
        .inc_by(0);
    });
}

/// Update the readiness gauge (`1` ready, `0` not ready).
pub fn set_ready(ready: bool) {
    use std::sync::OnceLock;

    static GAUGE: OnceLock<prometheus::IntGauge> = OnceLock::new();
    let gauge = GAUGE.get_or_init(|| {
        let gauge = prometheus::IntGauge::new(
            "axiom_ready",
            "1 when all readiness checks pass, 0 otherwise",
        )
        .expect("ready gauge");
        let _ = prometheus::default_registry().register(Box::new(gauge.clone()));
        gauge
    });
    gauge.set(if ready { 1 } else { 0 });
}

/// Record one sync RPC outcome (`operation` = push/pull/clone/list_refs).
pub fn record_sync_operation(operation: &str, result: &str) {
    counter(
        "axiom_sync_operations_total",
        "Sync RPCs handled, by operation and result",
        &["operation", "result"],
    )
    .with_label_values(&[operation, result])
    .inc();
}

/// Record an authentication rejection and its reason.
pub fn record_auth_failure(reason: &str) {
    counter(
        "axiom_auth_failures_total",
        "Rejected authentication attempts, by reason",
        &["reason"],
    )
    .with_label_values(&[reason])
    .inc();
}

/// Record a storage operation outcome (`backend` = local/s3/fdb).
pub fn record_storage_operation(backend: &str, operation: &str, result: &str) {
    counter(
        "axiom_storage_operations_total",
        "Storage operations, by backend, operation and result",
        &["backend", "operation", "result"],
    )
    .with_label_values(&[backend, operation, result])
    .inc();
}

/// Extract the trace id from a W3C `traceparent` header value.
///
/// Format: `00-<32 hex trace id>-<16 hex parent id>-<2 hex flags>`.
pub fn trace_id_from_traceparent(value: &str) -> Option<String> {
    let mut parts = value.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    if version.len() != 2 || trace_id.len() != 32 || !trace_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(trace_id.to_ascii_lowercase())
}

pub fn init_logging() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let log_format = std::env::var("AXIOM_LOG_FORMAT").unwrap_or_else(|_| "human".to_owned());

    match log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .init();
        }
    }
}

pub fn render_prometheus() -> CasResult<String> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|e| CasError::Store(format!("prometheus encode metrics: {e}")))?;
    String::from_utf8(buffer)
        .map_err(|e| CasError::Store(format!("prometheus metrics utf8: {e}")))
}

pub async fn observe_http_request(mut req: Request<Body>, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().to_string();
    let path = matched_path(&req);
    let tenant_id = header_value(&req, "x-tenant-id").unwrap_or_else(|| "-".to_owned());
    let request_id = header_value(&req, REQUEST_ID_HEADER).unwrap_or_else(next_request_id);
    let trace_id = header_value(&req, "traceparent")
        .and_then(|v| trace_id_from_traceparent(&v))
        .unwrap_or_else(|| "-".to_owned());

    if let Ok(header_value) = HeaderValue::from_str(&request_id) {
        req.headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), header_value);
    }

    tracing::info!(
        target: "axiom.http",
        request_id = %request_id,
        trace_id = %trace_id,
        method = %method,
        path = %path,
        tenant_id = %tenant_id,
        "request started"
    );

    let timeout_secs = std::env::var("AXIOM_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(300);

    let outcome = REQUEST_ID_SCOPE
        .scope(
            request_id.clone(),
            tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), next.run(req)),
        )
        .await;

    let mut response = match outcome {
        Ok(response) => response,
        Err(_) => {
            tracing::warn!(
                target: "axiom.http",
                request_id = %request_id,
                path = %path,
                timeout_secs,
                "request timed out"
            );
            let body = serde_json::json!({
                "error": "upstream_timeout",
                "code": "upstream_timeout",
                "message": format!("request exceeded the {timeout_secs}s budget"),
                "request_id": request_id,
                "retryable": true,
            });
            (
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                axum::Json(body),
            )
                .into_response()
        }
    };
    let status = response.status().as_u16();
    let duration = start.elapsed();

    if let Ok(header_value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), header_value);
    }

    HttpMetrics::global().record(&method, &path, status, duration);

    tracing::info!(
        target: "axiom.http",
        request_id = %request_id,
        trace_id = %trace_id,
        method = %method,
        path = %path,
        tenant_id = %tenant_id,
        status,
        duration_ms = duration.as_secs_f64() * 1000.0,
        "request completed"
    );

    response
}

fn matched_path(req: &Request<Body>) -> String {
    req.extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or_else(|| req.uri().path())
        .to_owned()
}

fn header_value(req: &Request<Body>, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn next_request_id() -> String {
    let unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("req-{unix_millis:016x}-{sequence:08x}")
}