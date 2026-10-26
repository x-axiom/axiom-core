use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::StoreFactory;
use axiom_core::telemetry;
use clap::{Parser, ValueEnum};

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

    /// TOML configuration file (used by cloud mode).
    #[arg(long)]
    config: Option<String>,

    /// FoundationDB cluster file (required for cloud mode).
    #[arg(long)]
    fdb_cluster: Option<String>,

    /// FoundationDB tenant (required for cloud mode without --config).
    #[arg(long)]
    fdb_tenant: Option<String>,

    /// FoundationDB workspace (required for cloud mode without --config).
    #[arg(long)]
    fdb_workspace: Option<String>,

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

    #[cfg(feature = "fdb")]
    let _fdb_network_guard = if matches!(&cli.mode, Mode::Cloud) {
        // Safety: keep the network guard alive for the full cloud-mode process.
        Some(unsafe { foundationdb::boot() })
    } else {
        None
    };

    // Initialize tracing / logging.
    telemetry::init_logging();

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
    let factory = if let Some(config) = &cli.config {
        StoreFactory::from_file(config)
    } else {
        // Prefer explicit CLI args over environment variables for one-shot
        // usage; missing values still fall through to env-var detection.
        // SAFETY: single-threaded at this point — Tokio runtime not yet started.
        unsafe {
            if let Some(bucket) = &cli.s3_bucket {
                std::env::set_var("AXIOM_S3_BUCKET", bucket);
            }
            if let Some(cluster) = &cli.fdb_cluster {
                std::env::set_var("AXIOM_FDB_CLUSTER_FILE", cluster);
            }
            if let Some(tenant) = &cli.fdb_tenant {
                std::env::set_var("AXIOM_FDB_TENANT", tenant);
            }
            if let Some(workspace) = &cli.fdb_workspace {
                std::env::set_var("AXIOM_FDB_WORKSPACE", workspace);
            }
            std::env::set_var("AXIOM_BACKEND", "cloud");
        }
        StoreFactory::from_env()
    };

    factory.and_then(|f| f.create()).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    })
}
