use actix::fut::wrap_future;
use actix::{Actor, ActorContext, AsyncContext, Context, Handler, Message, System};
use actix_web::{App, HttpResponse, HttpServer, web};
use bytes::Bytes;
use common::{
    CommandAck, Heartbeat, JOIN_SUBJECT, JoinRequest, JoinResponse, ProviderKind, ProviderReplica,
    ReplicaStatus, SupervisorCommand, WorkerStatus, heartbeat_subject, worker_command_subject,
};
use config::Config;
use futures_util::stream::StreamExt;
use prometheus::{Encoder, TextEncoder, gather};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum ProviderConfig {
    Name(String),
    Detailed {
        name: String,
        kind: Option<ProviderKind>,
        upstream: Option<String>,
    },
}

impl ProviderConfig {
    fn into_replica(self) -> ProviderReplica {
        match self {
            ProviderConfig::Name(name) => ProviderReplica {
                replica_id: Uuid::new_v4().to_string(),
                name,
                kind: ProviderKind::Unknown,
                upstream: String::new(),
                status: ReplicaStatus::Init,
            },
            ProviderConfig::Detailed {
                name,
                kind,
                upstream,
            } => ProviderReplica {
                replica_id: Uuid::new_v4().to_string(),
                name,
                kind: kind.unwrap_or(ProviderKind::Unknown),
                upstream: upstream.unwrap_or_default(),
                status: ReplicaStatus::Init,
            },
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct CommandEvent {
    command: SupervisorCommand,
    reply_to: Option<String>,
}

struct WorkerActor {
    worker_id: String,
    replicas: HashMap<String, ProviderReplica>,
    nats: async_nats::Client,
    rejected: Arc<AtomicBool>,
}

impl Actor for WorkerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        tracing::info!("WorkerActor started. id={}", self.worker_id);

        ctx.run_interval(Duration::from_secs(15), |act, ctx| {
            if act.rejected.load(Ordering::SeqCst) {
                ctx.stop();
                System::current().stop();
                return;
            }
            let client = act.nats.clone();
            let worker_id = act.worker_id.clone();
            let replicas = act.replicas.values().cloned().collect::<Vec<_>>();

            let fut = async move {
                let hb = Heartbeat {
                    worker_id: worker_id.clone(),
                    time_stamp: chrono::Utc::now().timestamp(),
                    replicas,
                };

                let subject = heartbeat_subject(&worker_id);
                let payload = Bytes::from(serde_json::to_vec(&hb).unwrap());

                if let Err(e) = client.publish(subject, payload).await {
                    tracing::error!("Heartbeat publish failed: {e}");
                }
            };

            ctx.spawn(wrap_future(fut));
        });
    }
}

impl Handler<CommandEvent> for WorkerActor {
    type Result = ();

    fn handle(&mut self, msg: CommandEvent, ctx: &mut Self::Context) -> Self::Result {
        let command = msg.command.clone();
        tracing::info!("Received supervisor command: {:?}", command);

        match &command {
            SupervisorCommand::SyncResource { resource } => {
                if let Some(target) = resource.clone() {
                    for replica in self.replicas.values_mut() {
                        if replica.name == target {
                            replica.status = ReplicaStatus::Syncing;
                        }
                    }
                } else {
                    for replica in self.replicas.values_mut() {
                        replica.status = ReplicaStatus::Syncing;
                    }
                }
            }
            SupervisorCommand::RemoveReplica { replica_id } => {
                self.replicas.remove(replica_id);
            }
            SupervisorCommand::Reject { reason } => {
                self.rejected.store(true, Ordering::SeqCst);
                if let Some(reason) = reason {
                    tracing::warn!("Worker rejected by supervisor: {reason}");
                } else {
                    tracing::warn!("Worker rejected by supervisor");
                }
                ctx.stop();
                System::current().stop();
            }
            SupervisorCommand::Terminate => {
                tracing::warn!("Terminate command received; shutting down worker");
                ctx.stop();
                System::current().stop();
            }
            SupervisorCommand::Ping => {
                tracing::info!("Supervisor ping acknowledged");
            }
        }

        if let Some(reply) = msg.reply_to {
            let ack = CommandAck {
                worker_id: self.worker_id.clone(),
                command: format!("{:?}", command),
                accepted: true,
                detail: None,
            };
            let client = self.nats.clone();
            ctx.spawn(wrap_future(async move {
                let payload = Bytes::from(serde_json::to_vec(&ack).unwrap());
                if let Err(err) = client.publish(reply, payload).await {
                    tracing::warn!("Failed to reply to supervisor command: {err}");
                }
            }));
        }
    }
}

async fn ping() -> &'static str {
    "pong"
}

async fn metrics() -> HttpResponse {
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    if let Err(e) = encoder.encode(&gather(), &mut buf) {
        tracing::error!("Metrics encode err: {e}");
    }
    actix_web::HttpResponse::Ok()
        .content_type(encoder.format_type())
        .body(buf)
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub providers: Vec<ProviderConfig>,
    pub nats_url: Option<String>,
}

async fn register_with_supervisor(
    nats: async_nats::Client,
    worker_id: String,
    replicas: Vec<ProviderReplica>,
    rejected: Arc<AtomicBool>,
) {
    let mut attempt: u64 = 0;
    loop {
        if rejected.load(Ordering::SeqCst) {
            break;
        }

        let join = JoinRequest {
            worker_id: worker_id.clone(),
            replicas: replicas.clone(),
        };

        match nats
            .request(
                JOIN_SUBJECT,
                Bytes::from(serde_json::to_vec(&join).unwrap()),
            )
            .await
        {
            Ok(resp) => match serde_json::from_slice::<JoinResponse>(&resp.payload) {
                Ok(resp) => match resp.status {
                    WorkerStatus::Approved => {
                        tracing::info!("Join accepted by supervisor");
                        break;
                    }
                    WorkerStatus::Rejected => {
                        rejected.store(true, Ordering::SeqCst);
                        let reason = resp.reason.unwrap_or_else(|| "no reason provided".into());
                        tracing::warn!("Join rejected by supervisor: {reason}");
                        System::current().stop();
                        break;
                    }
                    WorkerStatus::Pending => {
                        tracing::info!(
                            "Join pending approval{}",
                            resp.reason
                                .as_ref()
                                .map(|r| format!(" ({r})"))
                                .unwrap_or_default()
                        );
                    }
                },
                Err(err) => tracing::warn!("Failed to parse join response: {err}"),
            },
            Err(err) => tracing::warn!("Supervisor join request failed: {err}"),
        }

        attempt = (attempt + 1).min(10);
        let delay = Duration::from_secs(5 * attempt);
        tokio::time::sleep(delay).await;
    }
}

pub fn load_settings(config_path: Option<&str>) -> anyhow::Result<Settings> {
    let mut builder = Config::builder();
    let path = config_path.unwrap_or("worker");
    builder = builder.add_source(config::File::with_name(path).required(false));
    builder = builder.add_source(config::Environment::with_prefix("KAGAMI"));
    Ok(builder.build()?.try_deserialize()?)
}

pub async fn run_worker(settings: Settings) -> anyhow::Result<()> {
    let nats_url = settings
        .nats_url
        .clone()
        .unwrap_or_else(|| "nats://127.0.0.1:4222".into());
    let nats = async_nats::connect(nats_url).await?;

    let worker_id = Uuid::new_v4().to_string();
    let replicas_vec = settings
        .providers
        .into_iter()
        .map(ProviderConfig::into_replica)
        .collect::<Vec<_>>();
    let replicas_map = replicas_vec
        .iter()
        .cloned()
        .map(|r| (r.replica_id.clone(), r))
        .collect::<HashMap<_, _>>();

    let rejected = Arc::new(AtomicBool::new(false));

    let addr = WorkerActor {
        worker_id: worker_id.clone(),
        replicas: replicas_map,
        nats: nats.clone(),
        rejected: rejected.clone(),
    }
    .start();

    let join_client = nats.clone();
    let rejected_for_join = rejected.clone();
    tokio::spawn(register_with_supervisor(
        join_client,
        worker_id.clone(),
        replicas_vec.clone(),
        rejected_for_join,
    ));

    let mut cmd_sub = nats.subscribe(worker_command_subject(&worker_id)).await?;
    let cmd_addr = addr.clone();
    tokio::spawn(async move {
        while let Some(msg) = cmd_sub.next().await {
            match serde_json::from_slice::<SupervisorCommand>(&msg.payload) {
                Ok(command) => cmd_addr.do_send(CommandEvent {
                    command,
                    reply_to: msg.reply.clone().map(|r| r.to_string()),
                }),
                Err(err) => {
                    tracing::warn!("Failed to parse supervisor command: {err}");
                }
            }
        }
    });

    HttpServer::new(|| {
        App::new()
            .route("/ping", web::get().to(ping))
            .route("/metrics", web::get().to(metrics))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await?;
    Ok(())
}
