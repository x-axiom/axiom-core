use std::sync::Arc;

use axiom_core::api::{build_router, state::AppState, state::HttpAuthMode};
use axiom_core::auth::jwt::{JwtKeyPair, JwtService};
use axiom_core::auth::oauth::{GitHubOAuth, GitHubOAuthConfig};
use axiom_core::auth::refresh::{MemoryRefreshTokenStore, RefreshTokenStore};
use axiom_core::auth::service::AuthService;
use axiom_core::store::StoreFactory;
use axiom_core::telemetry;
use axiom_core::tenant::TenantDirectory;
use axiom_core::tenant::memory::MemoryTenantDirectory;
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

    /// gRPC sync listen address (cloud mode only).
    #[arg(long, default_value = "0.0.0.0:50051")]
    grpc_listen: String,

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

    let state = wire_auth(state, &cli);
    tracing::info!(http_auth_mode = ?state.http_auth_mode, "http auth mode");

    // Single shutdown switch shared by the HTTP and gRPC servers.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    // ── gRPC sync service (cloud mode) ───────────────────────────────────
    #[cfg(feature = "cloud")]
    if matches!(cli.mode, Mode::Cloud) {
        let grpc_state = state.clone();
        let grpc_listen = cli.grpc_listen.clone();
        let grpc_auth = grpc_state.auth.clone();
        let mut grpc_shutdown = shutdown_rx.clone();
        tracing::info!("axiom-core gRPC sync listening on {grpc_listen}");
        tokio::spawn(async move {
            let shutdown = async move {
                let _ = grpc_shutdown.changed().await;
            };
            if let Err(err) =
                axiom_core::grpc::serve(grpc_state, &grpc_listen, grpc_auth, shutdown).await
            {
                tracing::error!(error = %err, "gRPC sync server stopped");
            }
        });
    }

    let app = build_router(state);

    tracing::info!("axiom-core listening on {}", cli.listen);

    let listener = tokio::net::TcpListener::bind(&cli.listen)
        .await
        .expect("failed to bind");
    let mut http_shutdown = shutdown_rx;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = http_shutdown.changed().await;
        })
        .await
        .expect("server error");
}

/// Resolve `SIGTERM` / `Ctrl-C` into a shutdown future so Kubernetes rolling
/// updates drain in-flight requests instead of severing them.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("shutdown: Ctrl+C"),
        _ = terminate => tracing::info!("shutdown: SIGTERM"),
    }
}

#[cfg(feature = "local")]
fn build_local_state(cli: &Cli) -> AppState {
    use axiom_core::store::RocksDbCasStore;
    use axiom_core::store::sqlite::SqliteMetadataStore;

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

// ---------------------------------------------------------------------------
// Auth wiring
// ---------------------------------------------------------------------------

/// Attach `/auth/*` when `AXIOM_AUTH_ENABLED` is truthy.
///
/// Selection rules:
/// - cloud mode → `TrustedGatewayHeaders` (the gateway is the only ingress)
/// - local mode → `Jwt` (core verifies bearer tokens itself)
/// - `AXIOM_AUTH_MODE` overrides both (`trusted-gateway` | `jwt` | `disabled`)
fn wire_auth(mut state: AppState, cli: &Cli) -> AppState {
    if !env_flag("AXIOM_AUTH_ENABLED") {
        return state;
    }

    let directory: Arc<dyn TenantDirectory> = match state.tenant_directory.clone() {
        Some(dir) => dir,
        None => {
            tracing::warn!(
                "auth enabled without a tenant directory; using in-memory directory (dev only)"
            );
            Arc::new(MemoryTenantDirectory::new())
        }
    };

    let jwt = match build_jwt_service(cli) {
        Ok(jwt) => Arc::new(jwt),
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(1);
        }
    };

    let github = match GitHubOAuthConfig::from_env() {
        Some(cfg) => match GitHubOAuth::new(cfg) {
            Ok(client) => Some(client),
            Err(err) => {
                eprintln!("error: {err}");
                std::process::exit(1);
            }
        },
        None => {
            tracing::warn!(
                "AXIOM_GITHUB_CLIENT_ID/SECRET/REDIRECT_URI not set; /auth/login provider=github is unavailable"
            );
            None
        }
    };

    if std::env::var("AXIOM_AUTH_DEV_SECRET").is_ok() {
        tracing::warn!(
            "AXIOM_AUTH_DEV_SECRET is set; dev login is enabled (never use in production)"
        );
    }
    let dev_secret = std::env::var("AXIOM_AUTH_DEV_SECRET").ok();

    let auth = Arc::new(AuthService::new(jwt, directory, github, dev_secret));

    let mode = match std::env::var("AXIOM_AUTH_MODE").as_deref() {
        Ok("jwt") => HttpAuthMode::Jwt,
        Ok("trusted-gateway") => HttpAuthMode::TrustedGatewayHeaders,
        Ok("disabled") => HttpAuthMode::Disabled,
        _ => match cli.mode {
            Mode::Cloud => HttpAuthMode::TrustedGatewayHeaders,
            Mode::Local => HttpAuthMode::Jwt,
        },
    };

    state = state.with_auth(auth).with_http_auth(mode);
    state
}

fn build_jwt_service(cli: &Cli) -> axiom_core::error::CasResult<JwtService> {
    let store: Arc<dyn RefreshTokenStore> = refresh_store(cli)?;

    let private = std::env::var("AXIOM_JWT_PRIVATE_KEY").ok();
    let public = std::env::var("AXIOM_JWT_PUBLIC_KEY").ok();

    match (private, public) {
        (Some(private), Some(public)) => {
            let kid = std::env::var("AXIOM_JWT_KID").unwrap_or_else(|_| "default".to_owned());
            let key = JwtKeyPair::from_pem_files(kid, &private, &public)?;
            Ok(JwtService::new(vec![key], 0, store))
        }
        _ => {
            tracing::warn!(
                "AXIOM_JWT_PRIVATE_KEY/AXIOM_JWT_PUBLIC_KEY not set; generating an ephemeral key \
                 (tokens will not verify across restarts and static gateway keys will not match)"
            );
            Ok(JwtService::new(vec![JwtKeyPair::generate()?], 0, store))
        }
    }
}

fn refresh_store(cli: &Cli) -> axiom_core::error::CasResult<Arc<dyn RefreshTokenStore>> {
    #[cfg(feature = "fdb")]
    {
        if matches!(cli.mode, Mode::Cloud) {
            let cluster = cli
                .fdb_cluster
                .clone()
                .or_else(|| std::env::var("AXIOM_FDB_CLUSTER_FILE").ok());
            let store = axiom_core::auth::refresh::FdbRefreshTokenStore::open(cluster.as_deref())?;
            tracing::info!("refresh tokens: FoundationDB");
            return Ok(Arc::new(store));
        }
    }

    let _ = cli;
    tracing::info!("refresh tokens: in-memory (local mode)");
    Ok(Arc::new(MemoryRefreshTokenStore::new()))
}

fn env_flag(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}
