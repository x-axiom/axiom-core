#![cfg(feature = "local")]

use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::RocksDbCasStore;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;

fn test_app() -> (axum::Router, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas_path = tmp.path().join("cas");
    let meta_path = tmp.path().join("meta.db");

    let cas = RocksDbCasStore::open(&cas_path).unwrap();
    let meta = SqliteMetadataStore::open(&meta_path).unwrap();

    (build_router(AppState::local(cas, meta)), tmp)
}

async fn body_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn metric_value(body: &str, prefix: &str) -> Option<f64> {
    body.lines()
        .find_map(|line| line.strip_prefix(prefix))
        .and_then(|rest| rest.trim().parse::<f64>().ok())
}

#[tokio::test]
async fn observability_routes_expose_live_ready_and_request_id() {
    let (app, _tmp) = test_app();

    let live = app
        .clone()
        .oneshot(Request::builder().uri("/live").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(live.status(), StatusCode::OK);
    assert!(live.headers().contains_key("x-request-id"));

    let ready = app
        .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::OK);
    let ready_body = body_text(ready).await;
    assert!(ready_body.contains("metadata_refs"));
    assert!(ready_body.contains("chunk_store"));
}

#[tokio::test]
async fn observability_metrics_endpoint_records_http_requests() {
    let (app, _tmp) = test_app();
    let metric_prefix = "axiom_http_requests_total{method=\"GET\",path=\"/health\",status=\"200\"} ";

    let baseline = app
        .clone()
        .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(baseline.status(), StatusCode::OK);
    assert_eq!(
        baseline.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let before = metric_value(&body_text(baseline).await, metric_prefix).unwrap_or(0.0);

    let health = app
        .clone()
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    assert!(health.headers().contains_key("x-request-id"));

    let after_metrics = app
        .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let metrics_body = body_text(after_metrics).await;
    let after = metric_value(&metrics_body, metric_prefix).unwrap_or(0.0);
    assert!(after > before, "expected {after} > {before} for metric {metric_prefix}");
    assert!(metrics_body.contains("axiom_http_request_duration_seconds_bucket"));
}