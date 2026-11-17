//! P0-3: the sync gRPC service must actually be **served**.
//!
//! Before this change the Push/Pull handlers existed only as library code and
//! nothing listened on the sync port. This test starts the real server and
//! performs an RPC round-trip over TCP.

#![cfg(feature = "cloud")]

use std::time::Duration;

use axiom_core::api::state::AppState;
use axiom_core::sync::proto::ListRefsRequest;
use axiom_core::sync::proto::sync_service_client::SyncServiceClient;

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_until_connectable(addr: &str) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("gRPC server did not start listening on {addr}");
}

#[tokio::test]
async fn sync_service_serves_list_refs_over_tcp() {
    let state = AppState::memory();
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server_addr = addr.clone();
    let handle = tokio::spawn(async move {
        axiom_core::grpc::serve(state, &server_addr, None, async move {
            let _ = stop_rx.await;
        })
        .await
    });

    wait_until_connectable(&addr).await;

    let mut client = SyncServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("client must connect to the sync port");

    let response = client
        .list_refs(ListRefsRequest {
            workspace_id: "default".into(),
            tenant_id: "local".into(),
        })
        .await
        .expect("list_refs must be served");

    assert!(response.into_inner().refs.is_empty());

    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
async fn sync_service_rejects_unauthenticated_calls_when_auth_is_enabled() {
    use std::sync::Arc;

    use axiom_core::auth::jwt::JwtService;
    use axiom_core::auth::refresh::MemoryRefreshTokenStore;
    use axiom_core::auth::service::AuthService;
    use axiom_core::tenant::memory::MemoryTenantDirectory;

    let jwt = Arc::new(
        JwtService::new(
            vec![axiom_core::auth::jwt::JwtKeyPair::generate().unwrap()],
            0,
            Arc::new(MemoryRefreshTokenStore::new()),
        ),
    );
    let auth = Arc::new(AuthService::new(
        jwt,
        Arc::new(MemoryTenantDirectory::new()),
        None,
        None,
    ));

    let state = AppState::memory().with_auth(auth.clone());
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server_addr = addr.clone();
    let handle = tokio::spawn(async move {
        axiom_core::grpc::serve(state, &server_addr, Some(auth), async move {
            let _ = stop_rx.await;
        })
        .await
    });

    wait_until_connectable(&addr).await;

    let mut client = SyncServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let err = client
        .list_refs(ListRefsRequest {
            workspace_id: "default".into(),
            tenant_id: "t".into(),
        })
        .await
        .expect_err("unauthenticated gRPC call must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}
