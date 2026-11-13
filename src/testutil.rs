//! Test helpers shared by unit tests and (through the `testutil` feature, enabled by the self
//! dev-dependency in Cargo.toml) integration tests.

use axum::Router;

/// Serve `app` on `127.0.0.1:0` for the rest of the process; returns `http://127.0.0.1:<port>`.
pub async fn spawn(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}
