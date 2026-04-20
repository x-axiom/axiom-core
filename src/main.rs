use axiom_core::api::{build_router, state::AppState};
use clap::{Parser, ValueEnum};
use tracing_subscriber::EnvFilter;

/// Axiom — high-performance versioned content-addressed object storage.
#[derive(Parser)]
#[command(name = "axiom-core", version, about)]
struct Cli {
    /// Storage backend mode.
    #[arg(long, default_value = "local", value_enum)]
    mode: Mode,

    /// Directory for local data (RocksDB CAS + SQLite metadata).
    #[arg(long, default_value = ".axiom")]
    data_dir: String,

    /// Address to listen on.
    #[arg(long, default_value = "0.0.0.0:3000")]
    listen: String,

    /// FoundationDB cluster file (required for cloud mode).
    #[arg(long)]
    fdb_cluster: Option<String>,

    /// S3 bucket name (required for cloud mode).
    #[arg(long)]
    s3_bucket: Option<String>,
}

#[derive(Clone, ValueEnum)]
enum Mode {
    Local,
    Cloud,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize tracing / logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let state = match cli.mode {
        Mode::Local => build_local_state(&cli),
        Mode::Cloud => build_cloud_state(&cli),
    };

    let app = build_router(state);

    tracing::info!("axiom-core listening on {}", cli.listen);

    let listener = tokio::net::TcpListener::bind(&cli.listen)
        .await
        .expect("failed to bind");
    axum::serve(listener, app).await.expect("server error");
}

#[cfg(feature = "local")]
fn build_local_state(cli: &Cli) -> AppState {
    use axiom_core::store::sqlite::SqliteMetadataStore;
    use axiom_core::store::RocksDbCasStore;

    let cas_path = format!("{}/cas", cli.data_dir);
    let meta_path = format!("{}/meta.db", cli.data_dir);

    std::fs::create_dir_all(&cli.data_dir).expect("failed to create data directory");

    let cas = RocksDbCasStore::open(&cas_path).expect("failed to open RocksDB CAS");
    let meta = SqliteMetadataStore::open(&meta_path).expect("failed to open SQLite metadata");

    AppState::local(cas, meta)
}

#[cfg(not(feature = "local"))]
fn build_local_state(_cli: &Cli) -> AppState {
    panic!("local mode requires the `local` feature; rebuild with --features local");
}

fn build_cloud_state(cli: &Cli) -> AppState {
    if cli.fdb_cluster.is_none() || cli.s3_bucket.is_none() {
        eprintln!("error: cloud mode requires --fdb-cluster and --s3-bucket");
        std::process::exit(1);
    }
    panic!("cloud mode is not yet implemented");
}
