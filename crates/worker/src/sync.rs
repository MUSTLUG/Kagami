use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, anyhow};
use common::{ProviderKind, ProviderReplica, ReplicaStatus};
use tokio::{
    fs,
    io::AsyncWriteExt,
    process::Command,
    sync::{Mutex, RwLock},
    time::sleep,
};
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct SyncManager {
    inner: Arc<SyncManagerInner>,
}

struct SyncManagerInner {
    base_dir: PathBuf,
    state: Arc<RwLock<HashMap<String, ProviderReplica>>>,
    interval_tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    inflight: Mutex<HashSet<String>>,
}

impl SyncManager {
    pub fn new(base_dir: PathBuf, state: Arc<RwLock<HashMap<String, ProviderReplica>>>) -> Self {
        Self {
            inner: Arc::new(SyncManagerInner {
                base_dir,
                state,
                interval_tasks: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashSet::new()),
            }),
        }
    }

    pub async fn bootstrap(&self) {
        let intervals = {
            let guard = self.inner.state.read().await;
            guard
                .values()
                .filter_map(|r| r.interval_secs.map(|secs| (r.replica_id.clone(), secs)))
                .collect::<Vec<_>>()
        };

        for (replica_id, secs) in intervals {
            self.spawn_interval(replica_id, secs).await;
        }
    }

    pub async fn add_replica(&self, replica: ProviderReplica) {
        let interval = replica.interval_secs;
        {
            let mut guard = self.inner.state.write().await;
            guard.insert(replica.replica_id.clone(), replica.clone());
        }

        if let Some(secs) = interval {
            self.spawn_interval(replica.replica_id.clone(), secs).await;
        }
    }

    pub async fn remove_replica(&self, replica_id: &str) -> bool {
        self.stop_interval(replica_id).await;
        let mut guard = self.inner.state.write().await;
        guard.remove(replica_id).is_some()
    }

    pub async fn trigger_sync(&self, resource: Option<String>) -> usize {
        let targets = {
            let guard = self.inner.state.read().await;
            guard
                .values()
                .filter(|replica| {
                    if let Some(target) = &resource {
                        &replica.name == target
                    } else {
                        true
                    }
                })
                .map(|r| r.replica_id.clone())
                .collect::<Vec<_>>()
        };

        if targets.is_empty() {
            return 0;
        }

        for id in targets.clone() {
            self.start_sync(id).await;
        }
        targets.len()
    }

    async fn spawn_interval(&self, replica_id: String, interval_secs: u64) {
        if interval_secs == 0 {
            return;
        }

        let mut tasks = self.inner.interval_tasks.lock().await;
        if let Some(existing) = tasks.remove(&replica_id) {
            existing.abort();
        }

        let task_id = replica_id.clone();
        let manager = self.clone();
        let handle = tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(interval_secs)).await;
                manager.start_sync(task_id.clone()).await;
            }
        });
        tasks.insert(replica_id, handle);
    }

    async fn stop_interval(&self, replica_id: &str) {
        if let Some(handle) = self.inner.interval_tasks.lock().await.remove(replica_id) {
            handle.abort();
        }
    }

    async fn start_sync(&self, replica_id: String) {
        {
            let mut inflight = self.inner.inflight.lock().await;
            if !inflight.insert(replica_id.clone()) {
                debug!(replica_id, "Skip duplicate sync; task already running");
                return;
            }
        }

        self.set_status(&replica_id, ReplicaStatus::Syncing).await;

        let manager = self.clone();
        tokio::spawn(async move {
            let result = manager.sync_once(&replica_id).await;
            manager.finish_sync(&replica_id, result).await;
        });
    }

    async fn finish_sync(&self, replica_id: &str, result: anyhow::Result<()>) {
        let status = match result {
            Ok(_) => ReplicaStatus::Success,
            Err(err) => {
                warn!(replica_id, error = ?err, "Sync failed");
                ReplicaStatus::Failed
            }
        };
        self.set_status(replica_id, status).await;

        let mut inflight = self.inner.inflight.lock().await;
        inflight.remove(replica_id);
    }

    async fn sync_once(&self, replica_id: &str) -> anyhow::Result<()> {
        let replica = {
            let guard = self.inner.state.read().await;
            guard.get(replica_id).cloned()
        }
        .ok_or_else(|| anyhow!("Replica {replica_id} not found"))?;

        let dest = self.replica_dir(&replica.name);

        match replica.kind {
            ProviderKind::Git => self.sync_git(&replica, &dest).await,
            ProviderKind::Rsync => {
                fs::create_dir_all(&dest)
                    .await
                    .context("create rsync target directory")?;
                self.sync_rsync(&replica, &dest).await
            }
            ProviderKind::Http => {
                fs::create_dir_all(&dest)
                    .await
                    .context("create http target directory")?;
                self.sync_http(&replica, &dest).await
            }
            ProviderKind::Unknown => Err(anyhow!("unsupported provider kind")),
        }
    }

    async fn sync_git(&self, replica: &ProviderReplica, dest: &Path) -> anyhow::Result<()> {
        let path = dest.to_string_lossy().to_string();
        let exists = fs::metadata(dest).await.is_ok();

        if exists {
            run_command(
                "git",
                &[
                    "-C",
                    &path,
                    "remote",
                    "set-url",
                    "origin",
                    &replica.upstream,
                ],
            )
            .await?;
            run_command("git", &["-C", &path, "fetch", "--all", "--prune"]).await?;
        } else {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent).await?;
            }
            run_command("git", &["clone", "--mirror", &replica.upstream, &path]).await?;
        }

        Ok(())
    }

    async fn sync_rsync(&self, replica: &ProviderReplica, dest: &Path) -> anyhow::Result<()> {
        let dest_arg = format!("{}/", dest.display());
        run_command("rsync", &["-az", "--delete", &replica.upstream, &dest_arg]).await
    }

    async fn sync_http(&self, replica: &ProviderReplica, dest: &Path) -> anyhow::Result<()> {
        let response = reqwest::Client::new()
            .get(&replica.upstream)
            .send()
            .await
            .context("http request failed")?
            .error_for_status()
            .context("http returned error status")?;

        let filename = guess_filename(&replica.upstream);
        let target = dest.join(filename);
        let mut file = fs::File::create(&target)
            .await
            .with_context(|| format!("create file {}", target.display()))?;

        let body = response.bytes().await.context("read response body")?;
        file.write_all(&body).await?;
        file.flush().await?;
        info!(
            upstream = %replica.upstream,
            path = %target.display(),
            "HTTP sync finished"
        );
        Ok(())
    }

    async fn set_status(&self, replica_id: &str, status: ReplicaStatus) {
        let mut guard = self.inner.state.write().await;
        if let Some(replica) = guard.get_mut(replica_id) {
            replica.status = status;
        }
    }

    fn replica_dir(&self, name: &str) -> PathBuf {
        self.inner.base_dir.join(sanitize_name(name))
    }
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn guess_filename(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if let Some((_, last)) = trimmed.rsplit_once('/') {
        if !last.is_empty() {
            return sanitize_name(last);
        }
    }
    "index.html".to_string()
}

async fn run_command(program: &str, args: &[&str]) -> anyhow::Result<()> {
    debug!(program, ?args, "running sync command");
    let output = Command::new(program).args(args).output().await?;
    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    error!(
        program,
        ?args,
        exit = ?output.status.code(),
        %stdout,
        %stderr,
        "command failed"
    );
    Err(anyhow!(
        "command {} failed with status {:?}",
        program,
        output.status.code()
    ))
}

#[cfg(test)]
mod tests {
    use super::sanitize_name;

    #[test]
    fn sanitize_replaces_disallowed_chars() {
        assert_eq!(sanitize_name("simple"), "simple");
        assert_eq!(sanitize_name("name/with/slash"), "name_with_slash");
        assert_eq!(sanitize_name("../escape?"), "___escape_");
    }
}
