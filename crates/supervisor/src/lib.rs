mod entity;
mod persistence;

use actix::fut::wrap_future;
use actix::{Actor, AsyncContext, Context, Handler, Message};
use bytes::Bytes;
use common::{
    HB_SUBJECT_PREFIX, Heartbeat, JOIN_SUBJECT, JoinRequest, JoinResponse, ProviderKind,
    ProviderReplica, ReplicaStatus, ResourceStatus, SupervisorCommand, worker_command_subject,
};
use futures_util::stream::StreamExt;
use sea_orm::DatabaseConnection;
use serde::Serialize;
use std::collections::HashMap;
use tracing::{error, info, warn};

pub struct SupervisorActor {
    nats: async_nats::Client,
    auto_approve: bool,
    db: Option<DatabaseConnection>,
    workers: HashMap<String, WorkerRecord>,
    resources: HashMap<String, ResourceState>,
}

struct WorkerRecord {
    replicas: HashMap<String, ProviderReplica>,
    last_seen: i64,
    approved: bool,
}

struct ResourceState {
    status: ResourceStatus,
    replicas: Vec<ReplicaRef>,
}

struct ReplicaRef {
    worker_id: String,
    replica: ProviderReplica,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct JoinEvent {
    pub request: JoinRequest,
    pub reply_to: Option<String>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct HeartbeatEvent {
    pub heartbeat: Heartbeat,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct SendCommand {
    pub worker_id: String,
    pub command: SupervisorCommand,
}

#[derive(Message)]
#[rtype(result = "Vec<ResourceView>")]
pub struct GetResources;

#[derive(Clone, Serialize)]
pub struct ReplicaView {
    pub worker_id: String,
    pub replica_id: String,
    pub name: String,
    pub kind: ProviderKind,
    pub upstream: String,
    pub status: ReplicaStatus,
}

#[derive(Clone, Serialize)]
pub struct ResourceView {
    pub name: String,
    pub status: ResourceStatus,
    pub replicas: Vec<ReplicaView>,
}

impl SupervisorActor {
    pub fn new(
        nats: async_nats::Client,
        auto_approve: bool,
        db: Option<DatabaseConnection>,
    ) -> Self {
        Self {
            nats,
            auto_approve,
            db,
            workers: HashMap::new(),
            resources: HashMap::new(),
        }
    }

    fn upsert_worker(&mut self, worker_id: String, replicas: Vec<ProviderReplica>) {
        let (approved, last_seen, persist_replicas) = {
            let record = self
                .workers
                .entry(worker_id.clone())
                .or_insert(WorkerRecord {
                    replicas: HashMap::new(),
                    last_seen: chrono::Utc::now().timestamp(),
                    approved: self.auto_approve,
                });

            record.replicas = replicas
                .into_iter()
                .map(|r| (r.replica_id.clone(), r))
                .collect();
            record.last_seen = chrono::Utc::now().timestamp();
            record.approved = record.approved || self.auto_approve;

            (
                record.approved,
                record.last_seen,
                record.replicas.values().cloned().collect::<Vec<_>>(),
            )
        };

        // persist snapshot if database available
        self.persist_snapshot(&worker_id, approved, last_seen, persist_replicas);

        self.rebuild_resources();
    }

    fn rebuild_resources(&mut self) {
        let mut resources: HashMap<String, Vec<ReplicaRef>> = HashMap::new();

        for (worker_id, worker) in &self.workers {
            if !worker.approved {
                continue;
            }
            for replica in worker.replicas.values() {
                resources
                    .entry(replica.name.clone())
                    .or_default()
                    .push(ReplicaRef {
                        worker_id: worker_id.clone(),
                        replica: replica.clone(),
                    });
            }
        }

        self.resources.clear();
        for (name, replica_refs) in resources {
            let status = aggregate_resource_status(&replica_refs);
            self.resources.insert(
                name,
                ResourceState {
                    status,
                    replicas: replica_refs,
                },
            );
        }
    }

    fn persist_snapshot(
        &self,
        worker_id: &str,
        approved: bool,
        last_seen: i64,
        replicas: Vec<ProviderReplica>,
    ) {
        if let Some(db) = &self.db {
            let db = db.clone();
            let worker_id = worker_id.to_string();
            tokio::spawn(async move {
                if let Err(err) =
                    persistence::upsert_worker(&db, &worker_id, approved, last_seen).await
                {
                    warn!("Failed to persist worker: {err}");
                }
                if let Err(err) = persistence::sync_replicas(&db, &worker_id, &replicas).await {
                    warn!("Failed to persist replicas: {err}");
                }
            });
        }
    }
}

fn aggregate_resource_status(replicas: &[ReplicaRef]) -> ResourceStatus {
    if replicas.is_empty() {
        return ResourceStatus::Error;
    }

    let mut failed = 0;
    let mut syncing_or_init = 0;
    for replica in replicas {
        match replica.replica.status {
            ReplicaStatus::Failed => failed += 1,
            ReplicaStatus::Init | ReplicaStatus::Syncing => syncing_or_init += 1,
            ReplicaStatus::Success => {}
        }
    }

    if failed == replicas.len() {
        ResourceStatus::Error
    } else if failed > 0 {
        ResourceStatus::Failed
    } else if syncing_or_init > 0 {
        ResourceStatus::Syncing
    } else {
        ResourceStatus::Ready
    }
}

impl Actor for SupervisorActor {
    type Context = Context<Self>;
}

impl Handler<JoinEvent> for SupervisorActor {
    type Result = ();

    fn handle(&mut self, msg: JoinEvent, ctx: &mut Self::Context) -> Self::Result {
        let accepted = self.auto_approve;
        self.upsert_worker(msg.request.worker_id.clone(), msg.request.replicas);
        info!("Worker join request from {}", msg.request.worker_id);

        if let Some(reply) = msg.reply_to {
            let response = JoinResponse {
                accepted,
                reason: (!accepted).then_some("manual approval required".to_string()),
            };
            let client = self.nats.clone();
            ctx.spawn(wrap_future(async move {
                let payload = Bytes::from(serde_json::to_vec(&response).unwrap());
                if let Err(err) = client.publish(reply, payload).await {
                    warn!("Failed to respond to join request: {err}");
                }
            }));
        }
    }
}

impl Handler<HeartbeatEvent> for SupervisorActor {
    type Result = ();

    fn handle(&mut self, msg: HeartbeatEvent, _ctx: &mut Self::Context) -> Self::Result {
        self.upsert_worker(msg.heartbeat.worker_id, msg.heartbeat.replicas);
    }
}

impl Handler<SendCommand> for SupervisorActor {
    type Result = ();

    fn handle(&mut self, msg: SendCommand, ctx: &mut Self::Context) -> Self::Result {
        let subject = worker_command_subject(&msg.worker_id);
        let payload = match serde_json::to_vec(&msg.command) {
            Ok(p) => Bytes::from(p),
            Err(err) => {
                error!("Failed to serialize supervisor command: {err}");
                return;
            }
        };

        let client = self.nats.clone();
        ctx.spawn(wrap_future(async move {
            if let Err(err) = client.publish(subject, payload).await {
                warn!("Failed to publish supervisor command: {err}");
            }
        }));
    }
}

impl Handler<GetResources> for SupervisorActor {
    type Result = Vec<ResourceView>;

    fn handle(&mut self, _msg: GetResources, _ctx: &mut Self::Context) -> Self::Result {
        self.resources
            .iter()
            .map(|(name, res)| ResourceView {
                name: name.clone(),
                status: res.status.clone(),
                replicas: res
                    .replicas
                    .iter()
                    .map(|r| ReplicaView {
                        worker_id: r.worker_id.clone(),
                        replica_id: r.replica.replica_id.clone(),
                        name: r.replica.name.clone(),
                        kind: r.replica.kind.clone(),
                        upstream: r.replica.upstream.clone(),
                        status: r.replica.status.clone(),
                    })
                    .collect(),
            })
            .collect()
    }
}

pub async fn run_supervisor(
    nats: async_nats::Client,
    auto_approve: bool,
    db: Option<DatabaseConnection>,
) -> anyhow::Result<actix::Addr<SupervisorActor>> {
    let supervisor = SupervisorActor::new(nats.clone(), auto_approve, db).start();

    // Join subscription
    let mut join_sub = nats.subscribe(JOIN_SUBJECT).await?;
    let join_addr = supervisor.clone();
    tokio::spawn(async move {
        while let Some(msg) = join_sub.next().await {
            match serde_json::from_slice::<JoinRequest>(&msg.payload) {
                Ok(req) => {
                    let reply_to = msg.reply.clone().map(|r| r.to_string());
                    join_addr.do_send(JoinEvent {
                        request: req,
                        reply_to,
                    });
                }
                Err(err) => warn!("Failed to parse join request: {err}"),
            }
        }
    });

    // Heartbeat subscription (wildcard)
    let mut hb_sub = nats.subscribe(format!("{}>", HB_SUBJECT_PREFIX)).await?;
    let hb_addr = supervisor.clone();
    tokio::spawn(async move {
        while let Some(msg) = hb_sub.next().await {
            match serde_json::from_slice::<Heartbeat>(&msg.payload) {
                Ok(hb) => hb_addr.do_send(HeartbeatEvent { heartbeat: hb }),
                Err(err) => warn!("Failed to parse heartbeat: {err}"),
            }
        }
    });

    Ok(supervisor)
}

pub async fn init_database(database_url: &str) -> anyhow::Result<DatabaseConnection> {
    persistence::init(database_url).await
}
