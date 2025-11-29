use std::env;

use actix::Addr;
use actix_web::{App, HttpResponse, HttpServer, web};
use supervisor::{GetResources, SupervisorActor, init_database, run_supervisor};

struct AppState {
    supervisor: Addr<SupervisorActor>,
}

async fn list_resources(state: web::Data<AppState>) -> HttpResponse {
    match state.supervisor.send(GetResources).await {
        Ok(resources) => HttpResponse::Ok().json(resources),
        Err(err) => {
            HttpResponse::InternalServerError().body(format!("Failed to fetch resources: {err}"))
        }
    }
}

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

    let nats = async_nats::connect(nats_url).await?;
    let db = init_database(&db_url).await?;
    let supervisor_addr = run_supervisor(nats, auto_approve, Some(db)).await?;

    let app_state = web::Data::new(AppState {
        supervisor: supervisor_addr.clone(),
    });

    let server = HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/resources", web::get().to(list_resources))
    })
    .bind(http_addr.clone())?
    .run();

    let handle = server.handle();
    tokio::select! {
        res = server => res?,
        _ = tokio::signal::ctrl_c() => {
            handle.stop(true).await;
        }
    }

    Ok(())
}
