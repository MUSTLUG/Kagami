use std::env;

use supervisor::start_supervisor_server;

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let nats_url =
        env::var("KAGAMI_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let auto_approve = env::var("SUPERVISOR_AUTO_APPROVE")
        .map(|v| v != "0" && v.to_lowercase() != "false")
        .unwrap_or(true);
    let http_addr =
        env::var("SUPERVISOR_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:21000".to_string());
    let db_url = env::var("SUPERVISOR_DATABASE_URL")
        .unwrap_or_else(|_| "sqlite://supervisor.db?mode=rwc".to_string());

    if let Err(err) = start_supervisor_server(nats_url, http_addr, db_url, auto_approve).await {
        tracing::error!(error = %err, "Supervisor terminated with error");
        return Err(err);
    }

    Ok(())
}
