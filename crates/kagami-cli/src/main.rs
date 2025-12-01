use std::{path::PathBuf, process::Command};

use anyhow::{Context, anyhow, bail};
use clap::{ArgAction, Parser, Subcommand};
use reqwest::StatusCode;

use kagami_supervisor::start_supervisor_server;
use kagami_worker::{Settings as WorkerSettings, load_settings, run_worker};

#[derive(Parser)]
#[command(name = "kagami-cli")]
#[command(about = "Kagami deployment helper")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run local binaries
    Run {
        #[command(subcommand)]
        which: RunWhich,
    },
    /// Run via docker run
    Docker {
        #[command(subcommand)]
        command: DockerCommand,
    },
    /// Supervisor operations (HTTP)
    Supervisor {
        #[command(subcommand)]
        action: SupervisorAction,
    },
}

#[derive(Subcommand)]
enum RunWhich {
    Worker {
        #[arg(long)]
        nats_url: Option<String>,
        #[arg(long)]
        data_dir: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    Supervisor {
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        nats_url: String,
        #[arg(long, default_value = "0.0.0.0:21000")]
        http_addr: String,
        #[arg(long, default_value = "sqlite://supervisor.db?mode=rwc")]
        db_url: String,
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        auto_approve: bool,
    },
}

#[derive(Subcommand)]
enum DockerWhich {
    Worker {
        #[arg(long, default_value = "kagami-worker:latest")]
        image: String,
        #[arg(long, default_value = "nats://nats:4222")]
        nats_url: String,
        #[arg(long, value_name = "HOST_PATH")]
        data_dir: PathBuf,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, default_value_t = 8080)]
        port: u16,
        #[arg(long)]
        network: Option<String>,
    },
    Supervisor {
        #[arg(long, default_value = "kagami-supervisor:latest")]
        image: String,
        #[arg(long, default_value = "nats://nats:4222")]
        nats_url: String,
        #[arg(long, default_value = "0.0.0.0:21000")]
        http_addr: String,
        #[arg(long, default_value = "sqlite:///app/supervisor.db?mode=rwc")]
        db_url: String,
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        auto_approve: bool,
        #[arg(long, default_value_t = 21000)]
        port: u16,
        #[arg(long)]
        network: Option<String>,
    },
}

#[derive(Subcommand)]
enum DockerCommand {
    Run {
        #[command(subcommand)]
        which: DockerWhich,
    },
    Nats {
        #[arg(long, default_value = "nats:2")]
        image: String,
        #[arg(long, default_value_t = 4222)]
        port: u16,
        #[arg(long, default_value_t = 8222)]
        monitor_port: u16,
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        jetstream: bool,
    },
    Build {
        #[command(subcommand)]
        target: DockerBuildTarget,
    },
}

#[derive(Subcommand)]
enum DockerBuildTarget {
    Worker {
        #[arg(long, default_value = "kagami-worker:latest")]
        tag: String,
    },
    Supervisor {
        #[arg(long, default_value = "kagami-supervisor:latest")]
        tag: String,
    },
    All {
        #[arg(long, default_value = "kagami-worker:latest")]
        worker_tag: String,
        #[arg(long, default_value = "kagami-supervisor:latest")]
        supervisor_tag: String,
    },
}

#[derive(Subcommand)]
enum SupervisorAction {
    Approve {
        worker_id: String,
        #[arg(long, default_value = "http://127.0.0.1:21000")]
        api: String,
    },
    Terminate {
        worker_id: String,
        #[arg(long, default_value = "http://127.0.0.1:21000")]
        api: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { which } => match which {
            RunWhich::Worker {
                nats_url,
                data_dir,
                config,
            } => {
                let settings_path = config.as_ref().map(|p| p.to_string_lossy().to_string());
                let mut settings: WorkerSettings =
                    load_settings(settings_path.as_deref()).context("load worker settings")?;
                if let Some(url) = nats_url {
                    settings.nats_url = Some(url);
                }
                if let Some(dir) = data_dir {
                    settings.data_dir = dir;
                }
                run_worker(settings).await?;
            }
            RunWhich::Supervisor {
                nats_url,
                http_addr,
                db_url,
                auto_approve,
            } => {
                start_supervisor_server(nats_url, http_addr, db_url, auto_approve).await?;
            }
        },
        Commands::Docker { command } => match command {
            DockerCommand::Run { which } => match which {
                DockerWhich::Worker {
                    image,
                    nats_url,
                    data_dir,
                    config,
                    port,
                    network,
                } => docker_run_worker(
                    &image,
                    &nats_url,
                    &data_dir,
                    config.as_ref(),
                    port,
                    network.as_deref(),
                )?,
                DockerWhich::Supervisor {
                    image,
                    nats_url,
                    http_addr,
                    db_url,
                    auto_approve,
                    port,
                    network,
                } => docker_run_supervisor(
                    &image,
                    &nats_url,
                    &http_addr,
                    &db_url,
                    auto_approve,
                    port,
                    network.as_deref(),
                )?,
            },
            DockerCommand::Nats {
                image,
                port,
                monitor_port,
                jetstream,
            } => docker_run_nats(&image, port, monitor_port, jetstream)?,
            DockerCommand::Build { target } => match target {
                DockerBuildTarget::Worker { tag } => docker_build_worker(&tag)?,
                DockerBuildTarget::Supervisor { tag } => docker_build_supervisor(&tag)?,
                DockerBuildTarget::All {
                    worker_tag,
                    supervisor_tag,
                } => {
                    docker_build_worker(&worker_tag)?;
                    docker_build_supervisor(&supervisor_tag)?;
                }
            },
        },
        Commands::Supervisor { action } => match action {
            SupervisorAction::Approve { worker_id, api } => {
                let url = format!("{api}/workers/{worker_id}/approve");
                let resp = reqwest::Client::new().post(url).send().await?;
                match resp.status() {
                    StatusCode::OK => println!("Approved {worker_id}"),
                    StatusCode::NOT_FOUND => println!("Worker {worker_id} not found"),
                    other => bail!("Request failed with status {other}"),
                }
            }
            SupervisorAction::Terminate { worker_id, api } => {
                let url = format!("{api}/workers/{worker_id}");
                let resp = reqwest::Client::new().delete(url).send().await?;
                match resp.status() {
                    StatusCode::NO_CONTENT => println!("Terminated {worker_id}"),
                    StatusCode::NOT_FOUND => println!("Worker {worker_id} not found"),
                    other => bail!("Request failed with status {other}"),
                }
            }
        },
    }

    Ok(())
}

fn docker_run_worker(
    image: &str,
    nats_url: &str,
    data_dir: &PathBuf,
    config: Option<&PathBuf>,
    port: u16,
    network: Option<&str>,
) -> anyhow::Result<()> {
    let mut cmd = Command::new("docker");
    cmd.args(["run", "-d", "--rm", "-p", &format!("{port}:8080")]);
    if let Some(net) = network {
        cmd.args(["--network", net]);
    }
    cmd.args(["-e", &format!("KAGAMI_NATS_URL={nats_url}")]);
    cmd.args(["-e", "KAGAMI_DATA_DIR=/data"]);
    let host_data = data_dir.canonicalize().unwrap_or_else(|_| data_dir.clone());
    let host_str = host_data
        .to_str()
        .ok_or_else(|| anyhow!("invalid data_dir path"))?;
    cmd.args(["-v", &format!("{host_str}:/data")]);
    if let Some(cfg) = config {
        let host_path = cfg.canonicalize().context("canonicalize worker config")?;
        let host_str = host_path
            .to_str()
            .ok_or_else(|| anyhow!("invalid config path"))?;
        cmd.args(["-v", &format!("{host_str}:/app/worker.toml:ro")]);
    }
    cmd.arg(image);
    run_cmd(cmd, "docker run worker")
}

fn docker_run_supervisor(
    image: &str,
    nats_url: &str,
    http_addr: &str,
    db_url: &str,
    auto_approve: bool,
    port: u16,
    network: Option<&str>,
) -> anyhow::Result<()> {
    let mut cmd = Command::new("docker");
    cmd.args(["run", "-d", "--rm", "-p", &format!("{port}:21000")]);
    if let Some(net) = network {
        cmd.args(["--network", net]);
    }
    cmd.args(["-e", &format!("KAGAMI_NATS_URL={nats_url}")]);
    cmd.args(["-e", &format!("SUPERVISOR_HTTP_ADDR={http_addr}")]);
    cmd.args(["-e", &format!("SUPERVISOR_DATABASE_URL={db_url}")]);
    cmd.args(["-e", &format!("SUPERVISOR_AUTO_APPROVE={auto_approve}")]);
    cmd.arg(image);
    run_cmd(cmd, "docker run supervisor")
}

fn docker_run_nats(
    image: &str,
    port: u16,
    monitor_port: u16,
    jetstream: bool,
) -> anyhow::Result<()> {
    let mut cmd = Command::new("docker");
    cmd.args([
        "run",
        "-d",
        "--rm",
        "-p",
        &format!("{port}:4222"),
        "-p",
        &format!("{monitor_port}:8222"),
    ]);
    cmd.arg(image);
    if jetstream {
        cmd.arg("-js");
    }
    run_cmd(cmd, "docker run nats")
}

fn docker_build_worker(tag: &str) -> anyhow::Result<()> {
    let mut cmd = Command::new("docker");
    cmd.args(["build", "-f", "Dockerfile.worker", "-t", tag, "."]);
    run_cmd(cmd, "docker build worker")
}

fn docker_build_supervisor(tag: &str) -> anyhow::Result<()> {
    let mut cmd = Command::new("docker");
    cmd.args(["build", "-f", "Dockerfile.supervisor", "-t", tag, "."]);
    run_cmd(cmd, "docker build supervisor")
}

fn run_cmd(mut cmd: Command, label: &str) -> anyhow::Result<()> {
    let status = cmd
        .status()
        .with_context(|| format!("{label} failed to start"))?;
    if !status.success() {
        bail!("{label} exited with status {status}");
    }
    Ok(())
}
