use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::RocksDbCasStore;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Initialize tracing / logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Open storage backends.
    let cas_path = ".axiom/cas";
    let meta_path = ".axiom/meta.db";

    std::fs::create_dir_all(".axiom").expect("failed to create .axiom directory");

    let cas = RocksDbCasStore::open(cas_path).expect("failed to open RocksDB CAS");
    let meta = SqliteMetadataStore::open(meta_path).expect("failed to open SQLite metadata");

    let state = AppState::new(cas, meta);
    let app = build_router(state);

    let bind = "0.0.0.0:3000";
    tracing::info!("axiom-core listening on {bind}");

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .expect("failed to bind");
    axum::serve(listener, app).await.expect("server error");
}
