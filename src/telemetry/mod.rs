use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::{HeaderName, HeaderValue, Request};
use axum::middleware::Next;
use axum::response::Response;
use prometheus::{CounterVec, Encoder, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder};
use tracing_subscriber::EnvFilter;

use crate::error::{CasError, CasResult};

pub const REQUEST_ID_HEADER: &str = "x-request-id";

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

    if let Ok(header_value) = HeaderValue::from_str(&request_id) {
        req.headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), header_value);
    }

    tracing::info!(
        target: "axiom.http",
        request_id = %request_id,
        method = %method,
        path = %path,
        tenant_id = %tenant_id,
        "request started"
    );

    let mut response = next.run(req).await;
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