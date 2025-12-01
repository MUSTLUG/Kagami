mod entity;
mod persistence;

use actix::fut::wrap_future;
use actix::prelude::MessageResult;
use actix::{Actor, AsyncContext, Context, Handler, Message};
use actix_files::{Files, NamedFile};
use actix_web::{App, HttpResponse, HttpServer, http::header, web};
use bytes::Bytes;
use common::{
    HB_SUBJECT_PREFIX, Heartbeat, JOIN_SUBJECT, JoinRequest, JoinResponse, ProviderKind,
    ProviderReplica, ReplicaStatus, ResourceStatus, StorageReport, SupervisorCommand, WorkerStatus,
    worker_command_subject,
};
use futures_util::stream::StreamExt;
use sea_orm::DatabaseConnection;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::f64::consts::PI;
use std::path::PathBuf;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};
use uuid::Uuid;

pub struct SupervisorActor {
    nats: async_nats::Client,
    auto_approve: bool,
    db: Option<DatabaseConnection>,
    workers: HashMap<String, WorkerRecord>,
    resources: HashMap<String, ResourceState>,
    telemetry: TelemetryTracker,
}

struct WorkerRecord {
    replicas: HashMap<String, ProviderReplica>,
    last_seen: i64,
    status: WorkerStatus,
    storage: Option<StorageReport>,
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
#[rtype(result = "bool")]
pub struct RemoveReplicaAction {
    pub worker_id: String,
    pub replica_id: String,
}

#[derive(Message)]
#[rtype(result = "bool")]
pub struct SyncResourceAction {
    pub worker_id: String,
    pub resource: Option<String>,
}

#[derive(Message)]
#[rtype(result = "bool")]
pub struct AddReplica {
    pub worker_id: String,
    pub command: common::AddReplicaCommand,
}

#[derive(Message)]
#[rtype(result = "bool")]
pub struct ApproveWorker {
    pub worker_id: String,
}

#[derive(Message)]
#[rtype(result = "bool")]
pub struct TerminateWorker {
    pub worker_id: String,
}

#[derive(Message)]
#[rtype(result = "bool")]
pub struct RejectWorker {
    pub worker_id: String,
    pub reason: Option<String>,
}

#[derive(Message)]
#[rtype(result = "Vec<WorkerView>")]
pub struct GetWorkers;

#[derive(Message)]
#[rtype(result = "OverviewView")]
pub struct GetOverview;

#[derive(Message)]
#[rtype(result = "Vec<ResourceView>")]
pub struct GetResources;

#[derive(Message)]
#[rtype(result = "TelemetryView")]
pub struct GetTelemetry;

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

#[derive(Clone, Serialize)]
pub struct WorkerView {
    pub worker_id: String,
    pub approved: bool,
    pub status: WorkerStatus,
    pub last_seen: i64,
    pub replicas: Vec<ReplicaView>,
}

#[derive(Clone, Serialize)]
pub struct OverviewView {
    pub workers_total: usize,
    pub workers_approved: usize,
    pub workers_pending: usize,
    pub workers_rejected: usize,
    pub resources_total: usize,
}

#[derive(Clone, Serialize)]
pub struct TelemetryPoint {
    pub label: String,
    pub value: u64,
}

#[derive(Clone, Serialize)]
pub struct SyncStats {
    pub success: usize,
    pub failure: usize,
}

#[derive(Clone, Serialize)]
pub struct DiskStats {
    pub total_gb: u64,
    pub used_gb: u64,
}

#[derive(Clone, Serialize)]
pub struct LatencyStats {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Clone, Serialize)]
pub struct TelemetryView {
    pub traffic: Vec<TelemetryPoint>,
    pub sync: SyncStats,
    pub disk: DiskStats,
    pub message_rate: u64,
    pub latency_ms: LatencyStats,
    pub concurrent_jobs: u64,
    pub last_sample_at: i64,
}

#[derive(Clone)]
struct AppState {
    supervisor: actix::Addr<SupervisorActor>,
    webui_dir: Option<PathBuf>,
}

struct TelemetryTracker {
    buckets: VecDeque<TrafficBucket>,
    max_points: usize,
    last_sample_at: i64,
}

#[derive(Clone, Copy)]
struct TrafficBucket {
    minute: i64,
    hits: u32,
}

impl TelemetryTracker {
    fn new(max_points: usize) -> Self {
        let now = chrono::Utc::now().timestamp();
        let anchor = minute_floor(now);
        let buckets = Self::bootstrap(max_points, anchor);
        Self {
            buckets,
            max_points,
            last_sample_at: now,
        }
    }

    fn bootstrap(max_points: usize, anchor: i64) -> VecDeque<TrafficBucket> {
        let mut buckets = VecDeque::with_capacity(max_points.max(1));
        if max_points == 0 {
            return buckets;
        }
        for idx in (0..max_points).rev() {
            let minute = anchor - (idx as i64) * 60;
            let phase = (idx as f64 / max_points as f64) * PI;
            let base = 320.0 + phase.sin() * 90.0;
            let jitter = ((idx * 37) % 50) as f64;
            let hits = (base + jitter).max(60.0) as u32;
            buckets.push_back(TrafficBucket { minute, hits });
        }
        buckets
    }

    fn record_hit(&mut self, ts: i64) {
        let actual = if ts > 0 {
            ts
        } else {
            chrono::Utc::now().timestamp()
        };
        let minute = minute_floor(actual);
        self.last_sample_at = actual;
        if let Some(last) = self.buckets.back() {
            if minute < last.minute {
                if let Some(bucket) = self
                    .buckets
                    .iter_mut()
                    .rev()
                    .find(|bucket| bucket.minute == minute)
                {
                    bucket.hits = bucket.hits.saturating_add(1);
                }
                return;
            }

            if minute == last.minute {
                if let Some(last_mut) = self.buckets.back_mut() {
                    last_mut.hits = last_mut.hits.saturating_add(1);
                }
                return;
            }

            let mut current = last.minute;
            while current + 60 <= minute {
                current += 60;
                self.buckets.push_back(TrafficBucket {
                    minute: current,
                    hits: 0,
                });
                self.trim();
            }
            if let Some(last_mut) = self.buckets.back_mut() {
                if last_mut.minute == minute {
                    last_mut.hits = last_mut.hits.saturating_add(1);
                }
            }
        } else {
            self.buckets.push_back(TrafficBucket { minute, hits: 1 });
        }
        self.trim();
    }

    fn trim(&mut self) {
        while self.buckets.len() > self.max_points {
            self.buckets.pop_front();
        }
    }

    fn series(&self) -> Vec<TelemetryPoint> {
        if self.buckets.is_empty() {
            return Vec::new();
        }
        let latest = self
            .buckets
            .back()
            .map(|b| b.minute)
            .unwrap_or_else(|| minute_floor(chrono::Utc::now().timestamp()));
        self.buckets
            .iter()
            .map(|bucket| {
                let diff = ((latest - bucket.minute) / 60).max(0);
                TelemetryPoint {
                    label: format!("{}m", diff),
                    value: bucket.hits as u64,
                }
            })
            .collect()
    }

    fn current_rate(&self) -> u64 {
        self.buckets
            .back()
            .map(|bucket| bucket.hits as u64)
            .unwrap_or(0)
    }

    fn last_sample_at(&self) -> i64 {
        self.last_sample_at
    }
}

fn minute_floor(ts: i64) -> i64 {
    ts - (ts % 60)
}

fn bytes_to_gb(bytes: u64) -> u64 {
    if bytes == 0 {
        return 0;
    }
    ((bytes as f64) / 1_073_741_824_f64).round() as u64
}

fn estimate_latency(concurrent_jobs: usize, syncing_replicas: usize) -> LatencyStats {
    let base = 80.0 + (concurrent_jobs as f64 * 0.8);
    let sync_penalty = (syncing_replicas as f64).sqrt() * 6.0;
    let p50 = base.max(40.0);
    let p95 = (base + sync_penalty * 1.4).max(p50 + 15.0);
    let p99 = (p95 + sync_penalty * 0.8).max(p95 + 10.0);
    LatencyStats { p50, p95, p99 }
}

#[derive(serde::Deserialize)]
struct RemoveReplicaRequest {
    replica_id: String,
}

#[derive(serde::Deserialize)]
struct SyncRequest {
    resource: Option<String>,
}

#[derive(serde::Deserialize)]
struct AddReplicaRequest {
    name: String,
    kind: ProviderKind,
    upstream: String,
    interval_secs: Option<u64>,
    labels: Option<std::collections::HashMap<String, String>>,
}

#[derive(serde::Deserialize)]
struct RejectRequest {
    reason: Option<String>,
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
            telemetry: TelemetryTracker::new(24),
        }
    }

    fn upsert_worker(
        &mut self,
        worker_id: String,
        replicas: Vec<ProviderReplica>,
        storage: Option<StorageReport>,
    ) -> WorkerStatus {
        let now = chrono::Utc::now().timestamp();
        let (status, persist_replicas) = {
            let record = self
                .workers
                .entry(worker_id.clone())
                .or_insert(WorkerRecord {
                    replicas: HashMap::new(),
                    last_seen: now,
                    status: if self.auto_approve {
                        WorkerStatus::Approved
                    } else {
                        WorkerStatus::Pending
                    },
                    storage: None,
                });

            record.replicas = replicas
                .into_iter()
                .map(|r| (r.replica_id.clone(), r))
                .collect();
            record.last_seen = now;
            if let Some(storage) = storage.clone() {
                record.storage = Some(storage);
            }

            if record.status != WorkerStatus::Rejected && self.auto_approve {
                record.status = WorkerStatus::Approved;
            }

            (
                record.status.clone(),
                record.replicas.values().cloned().collect::<Vec<_>>(),
            )
        };

        // persist snapshot if database available
        self.persist_snapshot(
            &worker_id,
            status == WorkerStatus::Approved,
            now,
            persist_replicas,
        );

        self.rebuild_resources();
        status
    }

    fn rebuild_resources(&mut self) {
        let mut resources: HashMap<String, Vec<ReplicaRef>> = HashMap::new();

        for (worker_id, worker) in &self.workers {
            if worker.status != WorkerStatus::Approved {
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

    fn telemetry_view(&self) -> TelemetryView {
        let mut success = 0usize;
        let mut failure = 0usize;
        let mut syncing = 0usize;

        for worker in self.workers.values() {
            if worker.status != WorkerStatus::Approved {
                continue;
            }
            for replica in worker.replicas.values() {
                match replica.status {
                    ReplicaStatus::Success => success += 1,
                    ReplicaStatus::Failed => failure += 1,
                    ReplicaStatus::Syncing | ReplicaStatus::Init => syncing += 1,
                }
            }
        }

        let disk = self.aggregate_disk_stats();
        let concurrent_jobs = syncing as u64;
        let latency = estimate_latency(concurrent_jobs as usize, syncing);
        TelemetryView {
            traffic: self.telemetry.series(),
            sync: SyncStats { success, failure },
            disk,
            message_rate: self.telemetry.current_rate(),
            latency_ms: latency,
            concurrent_jobs,
            last_sample_at: self.telemetry.last_sample_at(),
        }
    }

    fn aggregate_disk_stats(&self) -> DiskStats {
        let mut total_bytes = 0u64;
        let mut used_bytes = 0u64;

        for worker in self.workers.values() {
            if worker.status != WorkerStatus::Approved {
                continue;
            }
            if let Some(storage) = &worker.storage {
                total_bytes = total_bytes.saturating_add(storage.total_bytes);
                let worker_used = storage.used_bytes.min(storage.total_bytes);
                used_bytes = used_bytes.saturating_add(worker_used);
            }
        }

        if total_bytes == 0 {
            return DiskStats {
                total_gb: 0,
                used_gb: 0,
            };
        }

        DiskStats {
            total_gb: bytes_to_gb(total_bytes),
            used_gb: bytes_to_gb(used_bytes.min(total_bytes)),
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
        let status = self.upsert_worker(msg.request.worker_id.clone(), msg.request.replicas, None);
        let accepted = status == WorkerStatus::Approved;
        let reason = match status {
            WorkerStatus::Pending => Some("manual approval required".to_string()),
            WorkerStatus::Rejected => Some("rejected by supervisor".to_string()),
            WorkerStatus::Approved => None,
        };
        info!(
            "Worker join request from {} (status {:?})",
            msg.request.worker_id, status
        );

        if let Some(reply) = msg.reply_to {
            let response = JoinResponse {
                accepted,
                status: status.clone(),
                reason,
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
        let heartbeat = msg.heartbeat;
        self.telemetry.record_hit(heartbeat.time_stamp);
        self.upsert_worker(heartbeat.worker_id, heartbeat.replicas, heartbeat.storage);
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

impl Handler<ApproveWorker> for SupervisorActor {
    type Result = bool;

    fn handle(&mut self, msg: ApproveWorker, _ctx: &mut Self::Context) -> Self::Result {
        let (last_seen, replicas) = if let Some(worker) = self.workers.get_mut(&msg.worker_id) {
            worker.status = WorkerStatus::Approved;
            (
                worker.last_seen,
                worker.replicas.values().cloned().collect::<Vec<_>>(),
            )
        } else {
            return false;
        };

        self.persist_snapshot(&msg.worker_id, true, last_seen, replicas);
        self.rebuild_resources();
        true
    }
}

impl Handler<TerminateWorker> for SupervisorActor {
    type Result = bool;

    fn handle(&mut self, msg: TerminateWorker, ctx: &mut Self::Context) -> Self::Result {
        let worker_id = msg.worker_id.clone();

        // Notify worker to terminate before removing it from registry.
        let client = self.nats.clone();
        let subject = worker_command_subject(&worker_id);
        ctx.spawn(wrap_future(async move {
            let payload =
                Bytes::from(serde_json::to_vec(&SupervisorCommand::Terminate).unwrap_or_default());
            if let Err(err) = client.publish(subject, payload).await {
                warn!("Failed to publish terminate command: {err}");
            }
        }));

        let removed = self.workers.remove(&msg.worker_id).is_some();
        if removed {
            if let Some(db) = &self.db {
                let db = db.clone();
                tokio::spawn(async move {
                    if let Err(err) = persistence::delete_worker(&db, &worker_id).await {
                        warn!("Failed to delete worker from DB: {err}");
                    }
                });
            }
            self.rebuild_resources();
        }
        removed
    }
}

impl Handler<RejectWorker> for SupervisorActor {
    type Result = bool;

    fn handle(&mut self, msg: RejectWorker, ctx: &mut Self::Context) -> Self::Result {
        let (last_seen, replicas) = if let Some(worker) = self.workers.get_mut(&msg.worker_id) {
            worker.status = WorkerStatus::Rejected;
            (
                worker.last_seen,
                worker.replicas.values().cloned().collect::<Vec<_>>(),
            )
        } else {
            return false;
        };

        self.persist_snapshot(&msg.worker_id, false, last_seen, replicas);
        self.rebuild_resources();

        let client = self.nats.clone();
        let subject = worker_command_subject(&msg.worker_id);
        let command = SupervisorCommand::Reject {
            reason: msg.reason.clone(),
        };
        ctx.spawn(wrap_future(async move {
            let payload = Bytes::from(serde_json::to_vec(&command).unwrap());
            if let Err(err) = client.publish(subject, payload).await {
                warn!("Failed to publish reject command: {err}");
            }
        }));

        true
    }
}

impl Handler<AddReplica> for SupervisorActor {
    type Result = bool;

    fn handle(&mut self, msg: AddReplica, ctx: &mut Self::Context) -> Self::Result {
        match self.workers.get(&msg.worker_id) {
            Some(w) if w.status == WorkerStatus::Approved => w,
            _ => return false,
        };

        // optimistically update view for immediacy
        if let Some(record) = self.workers.get_mut(&msg.worker_id) {
            let replica = ProviderReplica {
                replica_id: Uuid::new_v4().to_string(),
                name: msg.command.name.clone(),
                kind: msg.command.kind.clone(),
                upstream: msg.command.upstream.clone(),
                status: ReplicaStatus::Init,
                interval_secs: msg.command.interval_secs,
                labels: msg.command.labels.clone(),
            };
            record
                .replicas
                .insert(replica.replica_id.clone(), replica.clone());
        }

        self.rebuild_resources();

        let subject = worker_command_subject(&msg.worker_id);
        let payload = match serde_json::to_vec(&SupervisorCommand::AddReplica(msg.command)) {
            Ok(p) => Bytes::from(p),
            Err(err) => {
                error!("Failed to serialize add_replica command: {err}");
                return false;
            }
        };
        let client = self.nats.clone();
        ctx.spawn(wrap_future(async move {
            if let Err(err) = client.publish(subject, payload).await {
                warn!("Failed to publish add_replica command: {err}");
            }
        }));

        true
    }
}

impl Handler<RemoveReplicaAction> for SupervisorActor {
    type Result = bool;

    fn handle(&mut self, msg: RemoveReplicaAction, _ctx: &mut Self::Context) -> Self::Result {
        let worker = match self.workers.get_mut(&msg.worker_id) {
            Some(w) if w.status == WorkerStatus::Approved => w,
            _ => return false,
        };

        let existed = worker.replicas.remove(&msg.replica_id).is_some();
        if existed {
            self.rebuild_resources();
            let subject = worker_command_subject(&msg.worker_id);
            let payload = match serde_json::to_vec(&SupervisorCommand::RemoveReplica {
                replica_id: msg.replica_id.clone(),
            }) {
                Ok(p) => Bytes::from(p),
                Err(err) => {
                    error!("Failed to serialize remove command: {err}");
                    return false;
                }
            };
            let client = self.nats.clone();
            actix::spawn(async move {
                if let Err(err) = client.publish(subject, payload).await {
                    warn!("Failed to publish remove_replica command: {err}");
                }
            });
        }
        existed
    }
}

impl Handler<SyncResourceAction> for SupervisorActor {
    type Result = bool;

    fn handle(&mut self, msg: SyncResourceAction, _ctx: &mut Self::Context) -> Self::Result {
        match self.workers.get(&msg.worker_id) {
            Some(w) if w.status == WorkerStatus::Approved => {}
            _ => return false,
        };

        // Optimistically mark replicas syncing for immediate UI feedback.
        if let Some(worker) = self.workers.get_mut(&msg.worker_id) {
            for replica in worker.replicas.values_mut() {
                if let Some(target) = &msg.resource {
                    if &replica.name == target {
                        replica.status = ReplicaStatus::Syncing;
                    }
                } else {
                    replica.status = ReplicaStatus::Syncing;
                }
            }
            self.rebuild_resources();
        }

        let subject = worker_command_subject(&msg.worker_id);
        let payload = match serde_json::to_vec(&SupervisorCommand::SyncResource {
            resource: msg.resource.clone(),
        }) {
            Ok(p) => Bytes::from(p),
            Err(err) => {
                error!("Failed to serialize sync command: {err}");
                return false;
            }
        };
        let client = self.nats.clone();
        actix::spawn(async move {
            if let Err(err) = client.publish(subject, payload).await {
                warn!("Failed to publish sync command: {err}");
            }
        });
        true
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

impl Handler<GetWorkers> for SupervisorActor {
    type Result = Vec<WorkerView>;

    fn handle(&mut self, _msg: GetWorkers, _ctx: &mut Self::Context) -> Self::Result {
        self.workers
            .iter()
            .map(|(id, w)| WorkerView {
                worker_id: id.clone(),
                approved: w.status == WorkerStatus::Approved,
                status: w.status.clone(),
                last_seen: w.last_seen,
                replicas: w
                    .replicas
                    .values()
                    .map(|r| ReplicaView {
                        worker_id: id.clone(),
                        replica_id: r.replica_id.clone(),
                        name: r.name.clone(),
                        kind: r.kind.clone(),
                        upstream: r.upstream.clone(),
                        status: r.status.clone(),
                    })
                    .collect(),
            })
            .collect()
    }
}

impl Handler<GetTelemetry> for SupervisorActor {
    type Result = MessageResult<GetTelemetry>;

    fn handle(&mut self, _msg: GetTelemetry, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.telemetry_view())
    }
}

impl Handler<GetOverview> for SupervisorActor {
    type Result = MessageResult<GetOverview>;

    fn handle(&mut self, _msg: GetOverview, _ctx: &mut Self::Context) -> Self::Result {
        let workers_total = self.workers.len();
        let workers_approved = self
            .workers
            .values()
            .filter(|w| w.status == WorkerStatus::Approved)
            .count();
        let workers_rejected = self
            .workers
            .values()
            .filter(|w| w.status == WorkerStatus::Rejected)
            .count();
        let workers_pending = workers_total.saturating_sub(workers_approved + workers_rejected);
        let resources_total = self.resources.len();
        MessageResult(OverviewView {
            workers_total,
            workers_approved,
            workers_pending,
            workers_rejected,
            resources_total,
        })
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

async fn list_resources(state: web::Data<AppState>) -> HttpResponse {
    match state.supervisor.send(GetResources).await {
        Ok(resources) => HttpResponse::Ok().json(resources),
        Err(err) => {
            HttpResponse::InternalServerError().body(format!("Failed to fetch resources: {err}"))
        }
    }
}

async fn list_workers(state: web::Data<AppState>) -> HttpResponse {
    match state.supervisor.send(GetWorkers).await {
        Ok(workers) => HttpResponse::Ok().json(workers),
        Err(err) => {
            HttpResponse::InternalServerError().body(format!("Failed to fetch workers: {err}"))
        }
    }
}

async fn overview(state: web::Data<AppState>) -> HttpResponse {
    match state.supervisor.send(GetOverview).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(err) => {
            HttpResponse::InternalServerError().body(format!("Failed to fetch overview: {err}"))
        }
    }
}

async fn telemetry(state: web::Data<AppState>) -> HttpResponse {
    match state.supervisor.send(GetTelemetry).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(err) => {
            HttpResponse::InternalServerError().body(format!("Failed to fetch telemetry: {err}"))
        }
    }
}

async fn approve_worker(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    match state
        .supervisor
        .send(ApproveWorker {
            worker_id: path.into_inner(),
        })
        .await
    {
        Ok(true) => HttpResponse::Ok().finish(),
        Ok(false) => HttpResponse::NotFound().finish(),
        Err(err) => HttpResponse::InternalServerError().body(format!("{err}")),
    }
}

async fn terminate_worker(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    match state
        .supervisor
        .send(TerminateWorker {
            worker_id: path.into_inner(),
        })
        .await
    {
        Ok(true) => HttpResponse::NoContent().finish(),
        Ok(false) => HttpResponse::NotFound().finish(),
        Err(err) => HttpResponse::InternalServerError().body(format!("{err}")),
    }
}

async fn reject_worker(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<RejectRequest>,
) -> HttpResponse {
    match state
        .supervisor
        .send(RejectWorker {
            worker_id: path.into_inner(),
            reason: body.reason.clone(),
        })
        .await
    {
        Ok(true) => HttpResponse::Ok().finish(),
        Ok(false) => HttpResponse::NotFound().finish(),
        Err(err) => HttpResponse::InternalServerError().body(format!("{err}")),
    }
}

async fn add_replica(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<AddReplicaRequest>,
) -> HttpResponse {
    let worker_id = path.into_inner();
    match state
        .supervisor
        .send(AddReplica {
            worker_id: worker_id.clone(),
            command: common::AddReplicaCommand {
                name: body.name.clone(),
                kind: body.kind.clone(),
                upstream: body.upstream.clone(),
                interval_secs: body.interval_secs,
                auth: None,
                labels: body.labels.clone(),
            },
        })
        .await
    {
        Ok(true) => HttpResponse::Accepted().finish(),
        Ok(false) => HttpResponse::BadRequest().body("Worker not found or not approved"),
        Err(err) => HttpResponse::InternalServerError().body(format!("{err}")),
    }
}

async fn remove_replica(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<RemoveReplicaRequest>,
) -> HttpResponse {
    let worker_id = path.into_inner();
    match state
        .supervisor
        .send(RemoveReplicaAction {
            worker_id,
            replica_id: body.replica_id.clone(),
        })
        .await
    {
        Ok(true) => HttpResponse::Accepted().finish(),
        Ok(false) => {
            HttpResponse::BadRequest().body("Worker not found, not approved, or replica missing")
        }
        Err(err) => HttpResponse::InternalServerError().body(format!("{err}")),
    }
}

async fn sync_resource(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<SyncRequest>,
) -> HttpResponse {
    let worker_id = path.into_inner();
    match state
        .supervisor
        .send(SyncResourceAction {
            worker_id,
            resource: body.resource.clone(),
        })
        .await
    {
        Ok(true) => HttpResponse::Accepted().finish(),
        Ok(false) => HttpResponse::BadRequest().body("Worker not found or not approved"),
        Err(err) => HttpResponse::InternalServerError().body(format!("{err}")),
    }
}

async fn ui_index(state: web::Data<AppState>, req: actix_web::HttpRequest) -> HttpResponse {
    if let Some(dir) = &state.webui_dir {
        match NamedFile::open(dir.join("index.html")) {
            Ok(file) => return file.into_response(&req),
            Err(err) => return HttpResponse::InternalServerError().body(format!("{err}")),
        }
    }
    HttpResponse::NotFound().finish()
}

fn resolve_webui_dir() -> Option<PathBuf> {
    if let Ok(env_path) = std::env::var("SUPERVISOR_WEBUI_DIR") {
        let p = PathBuf::from(env_path);
        if p.join("index.html").exists() {
            return Some(p);
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let candidate = cwd.join("webui");
        if candidate.join("index.html").exists() {
            return Some(candidate);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        let mut check = exe.clone();
        check.pop();
        let candidate = check.join("webui");
        if candidate.join("index.html").exists() {
            return Some(candidate);
        }
        if check.pop() {
            let candidate = check.join("webui");
            if candidate.join("index.html").exists() {
                return Some(candidate);
            }
        }
    }
    None
}

async fn connect_nats_with_retry(nats_url: &str) -> async_nats::Client {
    let mut attempt: u32 = 0;
    loop {
        match async_nats::connect(nats_url).await {
            Ok(client) => {
                if attempt > 0 {
                    info!(%nats_url, attempts = attempt + 1, "Connected to NATS after retries");
                }
                return client;
            }
            Err(err) => {
                attempt = attempt.saturating_add(1);
                warn!(
                    %nats_url,
                    attempt,
                    error = %err,
                    "Failed to connect to NATS; retrying in 5 seconds"
                );
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

pub async fn start_supervisor_server(
    nats_url: String,
    http_addr: String,
    db_url: String,
    auto_approve: bool,
) -> anyhow::Result<()> {
    let nats = connect_nats_with_retry(&nats_url).await;
    let db = init_database(&db_url).await?;
    let supervisor_addr = run_supervisor(nats, auto_approve, Some(db)).await?;
    let webui_dir = resolve_webui_dir();

    let app_state = web::Data::new(AppState {
        supervisor: supervisor_addr.clone(),
        webui_dir: webui_dir.clone(),
    });

    let server = HttpServer::new(move || {
        let mut app = App::new()
            .app_data(app_state.clone())
            .route(
                "/",
                web::get().to(|| async {
                    HttpResponse::Found()
                        .append_header((header::LOCATION, "/ui/"))
                        .finish()
                }),
            )
            .route("/resources", web::get().to(list_resources))
            .route("/workers", web::get().to(list_workers))
            .route("/overview", web::get().to(overview))
            .route("/telemetry", web::get().to(telemetry))
            .route("/workers/{id}/approve", web::post().to(approve_worker))
            .route("/workers/{id}", web::delete().to(terminate_worker))
            .route("/workers/{id}/reject", web::post().to(reject_worker))
            .route("/workers/{id}/add_replica", web::post().to(add_replica))
            .route(
                "/workers/{id}/remove_replica",
                web::post().to(remove_replica),
            )
            .route("/workers/{id}/sync", web::post().to(sync_resource))
            .route("/ui", web::get().to(ui_index));

        if let Some(dir) = webui_dir.clone() {
            app = app.service(Files::new("/ui/", dir).index_file("index.html"));
        }

        app
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
