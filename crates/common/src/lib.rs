use serde::{Deserialize, Serialize};

pub const HB_SUBJECT_PREFIX: &str = "kagami.hb.";
pub const JOIN_SUBJECT: &str = "kagami.supervisor.join";
pub const PROVIDER_STATUS_SUBJECT: &str = "kagami.supervisor.provider_status";

pub fn heartbeat_subject(worker_id: &str) -> String {
    format!("{HB_SUBJECT_PREFIX}{worker_id}")
}

pub fn worker_command_subject(worker_id: &str) -> String {
    format!("kagami.worker.{worker_id}.cmd")
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Pending,
    Approved,
    Rejected,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum AuthConfig {
    Basic { username: String, password: String },
    Token { token: String },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaStatus {
    Init,
    Syncing,
    Success,
    Failed,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceStatus {
    Ready,
    Syncing,
    Failed,
    Error,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    Git,
    Rsync,
    Http,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProviderReplica {
    pub replica_id: String,
    pub name: String,
    pub kind: ProviderKind,
    pub upstream: String,
    pub status: ReplicaStatus,
    pub interval_secs: Option<u64>,
    pub labels: Option<std::collections::HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Heartbeat {
    pub worker_id: String,
    pub time_stamp: i64,
    pub replicas: Vec<ProviderReplica>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JoinRequest {
    pub worker_id: String,
    pub replicas: Vec<ProviderReplica>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JoinResponse {
    pub accepted: bool,
    pub status: WorkerStatus,
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProviderStatusUpdate {
    pub worker_id: String,
    pub replica_id: String,
    pub status: ReplicaStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddReplicaCommand {
    pub name: String,
    pub kind: ProviderKind,
    pub upstream: String,
    pub interval_secs: Option<u64>,
    pub auth: Option<AuthConfig>,
    pub labels: Option<std::collections::HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SupervisorCommand {
    SyncResource { resource: Option<String> },
    RemoveReplica { replica_id: String },
    Reject { reason: Option<String> },
    AddReplica(AddReplicaCommand),
    Terminate,
    Ping,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommandAck {
    pub worker_id: String,
    pub command: String,
    pub accepted: bool,
    pub detail: Option<String>,
}
