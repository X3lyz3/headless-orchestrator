use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct BrowserConfig {
	pub chromium_path: String,
	pub session_root: PathBuf,
	pub launch_timeout: Duration,
	pub graceful_shutdown_timeout: Duration,
	pub max_sessions: usize,
	pub max_sessions_per_tenant: usize,
	pub idle_ttl: Duration,
	pub hard_ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct SessionCreateOptions {
	pub tenant_id: String,
	pub proxy: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SessionSnapshot {
	pub session_id: String,
	pub tenant_id: String,
	pub ws_endpoint: String,
	pub user_data_dir: String,
	pub created_at_ms: u128,
	pub last_used_ms: u128,
}

#[derive(Debug)]
struct ManagedSession {
	id: String,
	tenant_id: String,
	ws_endpoint: String,
	user_data_dir: PathBuf,
	child: Mutex<Child>,
	created_at: Instant,
	created_epoch_ms: u128,
	last_used: Mutex<Instant>,
	attached_target_ids: Mutex<VecDeque<String>>,
}

impl ManagedSession {
	async fn touch(&self) {
		let mut guard = self.last_used.lock().await;
		*guard = Instant::now();
	}

	async fn last_used(&self) -> Instant {
		*self.last_used.lock().await
	}

	async fn shutdown(&self, graceful_timeout: Duration) -> Result<()> {
		let mut child = self.child.lock().await;
		if let Some(_status) = child.try_wait().context("failed to query child status")? {
			return Ok(());
		}

		child
			.start_kill()
			.context("failed to send kill signal to chromium child")?;

		let wait_result = timeout(graceful_timeout, child.wait()).await;
		match wait_result {
			Ok(wait_status) => {
				wait_status.context("failed to wait for chromium child after kill")?;
				Ok(())
			}
			Err(_) => Err(anyhow!(
				"timeout while waiting chromium child process to exit"
			)),
		}
	}
}

#[derive(Clone)]
pub struct BrowserManager {
	cfg: BrowserConfig,
	sessions: Arc<RwLock<HashMap<String, Arc<ManagedSession>>>>,
}

impl BrowserManager {
	pub fn new(cfg: BrowserConfig) -> Self {
		Self {
			cfg,
			sessions: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub async fn create_session(&self, options: SessionCreateOptions) -> Result<SessionSnapshot> {
		self.ensure_capacity(&options.tenant_id).await?;

		let session_id = Uuid::new_v4().to_string();
		let user_data_dir = self.cfg.session_root.join(&session_id);
		tokio::fs::create_dir_all(&user_data_dir)
			.await
			.with_context(|| format!("failed creating session dir: {}", user_data_dir.display()))?;

		let (child, ws_endpoint) = self.launch_chromium(&user_data_dir, options.proxy.as_deref()).await?;

		let managed = Arc::new(ManagedSession {
			id: session_id.clone(),
			tenant_id: options.tenant_id.clone(),
			ws_endpoint: ws_endpoint.clone(),
			user_data_dir: user_data_dir.clone(),
			child: Mutex::new(child),
			created_at: Instant::now(),
			created_epoch_ms: unix_time_ms(),
			last_used: Mutex::new(Instant::now()),
			attached_target_ids: Mutex::new(VecDeque::new()),
		});

		{
			let mut sessions = self.sessions.write().await;
			sessions.insert(session_id.clone(), managed);
		}

		Ok(SessionSnapshot {
			session_id,
			tenant_id: options.tenant_id,
			ws_endpoint,
			user_data_dir: user_data_dir.display().to_string(),
			created_at_ms: unix_time_ms(),
			last_used_ms: 0,
		})
	}

	pub async fn close_session(&self, session_id: &str) -> Result<bool> {
		let maybe_session = {
			let mut sessions = self.sessions.write().await;
			sessions.remove(session_id)
		};

		if let Some(session) = maybe_session {
			if let Err(err) = session.shutdown(self.cfg.graceful_shutdown_timeout).await {
				warn!(session_id = %session.id, error = %err, "chromium shutdown returned error");
			}
			self.cleanup_session_dir(&session.user_data_dir).await;
			return Ok(true);
		}

		Ok(false)
	}

	pub async fn session_snapshot(&self, session_id: &str) -> Option<SessionSnapshot> {
		let session = {
			let sessions = self.sessions.read().await;
			sessions.get(session_id).cloned()
		}?;

		let elapsed_created = session.created_at.elapsed().as_millis();
		let last_used = session.last_used().await;
		let elapsed_last_used = last_used.elapsed().as_millis();

		Some(SessionSnapshot {
			session_id: session.id.clone(),
			tenant_id: session.tenant_id.clone(),
			ws_endpoint: session.ws_endpoint.clone(),
			user_data_dir: session.user_data_dir.display().to_string(),
			created_at_ms: session.created_epoch_ms,
			last_used_ms: elapsed_last_used,
		})
	}

	pub async fn touch_session(&self, session_id: &str) -> Result<()> {
		let session = {
			let sessions = self.sessions.read().await;
			sessions.get(session_id).cloned()
		}
		.ok_or_else(|| anyhow!("session not found"))?;

		session.touch().await;
		Ok(())
	}

	pub async fn ws_endpoint(&self, session_id: &str) -> Result<String> {
		let session = {
			let sessions = self.sessions.read().await;
			sessions.get(session_id).cloned()
		}
		.ok_or_else(|| anyhow!("session not found"))?;

		session.touch().await;
		Ok(session.ws_endpoint.clone())
	}

	pub async fn add_target(&self, session_id: &str, target_id: String) -> Result<()> {
		let session = {
			let sessions = self.sessions.read().await;
			sessions.get(session_id).cloned()
		}
		.ok_or_else(|| anyhow!("session not found"))?;

		let mut targets = session.attached_target_ids.lock().await;
		targets.push_back(target_id);
		Ok(())
	}

	pub async fn latest_target(&self, session_id: &str) -> Result<Option<String>> {
		let session = {
			let sessions = self.sessions.read().await;
			sessions.get(session_id).cloned()
		}
		.ok_or_else(|| anyhow!("session not found"))?;

		let targets = session.attached_target_ids.lock().await;
		Ok(targets.back().cloned())
	}

	pub async fn active_sessions_count(&self) -> usize {
		self.sessions.read().await.len()
	}

	pub async fn cleanup_expired_sessions(&self) {
		let now = Instant::now();
		let sessions: Vec<(String, Arc<ManagedSession>)> = {
			let guard = self.sessions.read().await;
			guard
				.iter()
				.map(|(id, session)| (id.clone(), Arc::clone(session)))
				.collect()
		};
		let mut expired_ids = Vec::new();

		for (id, session) in sessions {
			let created_elapsed = now.duration_since(session.created_at);
			let last_used = session.last_used().await;
			let idle_elapsed = now.duration_since(last_used);
			if created_elapsed >= self.cfg.hard_ttl || idle_elapsed >= self.cfg.idle_ttl {
				expired_ids.push(id);
			}
		}

		for id in expired_ids {
			if let Err(err) = self.close_session(&id).await {
				warn!(session_id = %id, error = %err, "failed to cleanup expired session");
			} else {
				info!(session_id = %id, "expired session cleaned up");
			}
		}
	}

	pub fn spawn_watchdog(self: Arc<Self>) {
		tokio::spawn(async move {
			loop {
				sleep(Duration::from_secs(5)).await;
				self.cleanup_expired_sessions().await;
			}
		});
	}

	pub async fn shutdown_all(&self) {
		let ids: Vec<String> = {
			let sessions = self.sessions.read().await;
			sessions.keys().cloned().collect()
		};

		for id in ids {
			if let Err(err) = self.close_session(&id).await {
				warn!(session_id = %id, error = %err, "shutdown_all could not close session");
			}
		}
	}

	async fn ensure_capacity(&self, tenant_id: &str) -> Result<()> {
		let sessions = self.sessions.read().await;
		if sessions.len() >= self.cfg.max_sessions {
			return Err(anyhow!("global session limit reached"));
		}

		let tenant_count = sessions.values().filter(|s| s.tenant_id == tenant_id).count();
		if tenant_count >= self.cfg.max_sessions_per_tenant {
			return Err(anyhow!("tenant session limit reached"));
		}

		Ok(())
	}

	async fn launch_chromium(&self, user_data_dir: &Path, proxy: Option<&str>) -> Result<(Child, String)> {
		let mut command = Command::new(&self.cfg.chromium_path);
		command
			.arg("--headless=new")
			.arg("--disable-gpu")
			.arg("--no-first-run")
			.arg("--no-default-browser-check")
			.arg("--remote-debugging-port=0")
			.arg(format!("--user-data-dir={}", user_data_dir.display()))
			.arg("about:blank")
			.stderr(std::process::Stdio::piped())
			.stdout(std::process::Stdio::null())
			.stdin(std::process::Stdio::null());

		if let Some(proxy_server) = proxy {
			command.arg(format!("--proxy-server={proxy_server}"));
		}

		let mut child = command.spawn().context("failed to spawn chromium process")?;
		let stderr = child
			.stderr
			.take()
			.ok_or_else(|| anyhow!("chromium stderr pipe is unavailable"))?;

		let mut reader = BufReader::new(stderr).lines();
		let read_future = async {
			loop {
				match reader.next_line().await {
					Ok(Some(line)) => {
						if let Some(endpoint) = extract_ws_endpoint(&line) {
							return Ok(endpoint);
						}
					}
					Ok(None) => {
						return Err(anyhow!(
							"chromium exited before exposing devtools websocket endpoint"
						));
					}
					Err(err) => {
						return Err(anyhow!(err).context("failed reading chromium stderr"));
					}
				}
			}
		};

		let ws_endpoint = timeout(self.cfg.launch_timeout, read_future)
			.await
			.context("timeout waiting for chromium devtools endpoint")??;

		Ok((child, ws_endpoint))
	}

	async fn cleanup_session_dir(&self, dir: &Path) {
		if let Err(err) = tokio::fs::remove_dir_all(dir).await {
			warn!(path = %dir.display(), error = %err, "failed to cleanup session directory");
		}
	}
}

fn extract_ws_endpoint(line: &str) -> Option<String> {
	let marker = "DevTools listening on ";
	let idx = line.find(marker)?;
	let endpoint = line[idx + marker.len()..].trim();
	if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
		return Some(endpoint.to_string());
	}
	None
}

fn unix_time_ms() -> u128 {
	match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
		Ok(duration) => duration.as_millis(),
		Err(_) => 0,
	}
}
