#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let settings = worker::load_settings(None)?;
    worker::run_worker(settings).await
}
