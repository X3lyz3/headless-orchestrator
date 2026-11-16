use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
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
    pub browser_context_id: String,
    pub user_data_dir: String,
    pub created_at_ms: u128,
    pub last_used_ms: u128,
}
#[derive(Debug)]
struct CdpClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_id: u64,
}

impl CdpClient {
    async fn connect(ws_endpoint: &str) -> Result<Self> {
        let (ws, _) = connect_async(ws_endpoint)
            .await
            .with_context(|| format!("failed to connect to CDP endpoint: {ws_endpoint}"))?;
        Ok(Self { ws, next_id: 1 })
    }

    async fn send_command(
        &mut self,
        method: &str,
        params: Value,
        session_id: Option<&str>,
    ) -> Result<Value> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("CDP id counter overflow"))?;

        let mut payload = json!({
            "id": id,
            "method": method,
            "params": params,
        });

        if let Some(session) = session_id {
            payload["sessionId"] = Value::String(session.to_string());
        }

        self.ws.send(Message::Text(payload.to_string())).await?;

        while let Some(frame) = self.ws.next().await {
            let frame = frame.context("failed reading CDP frame")?;
            if !frame.is_text() {
                continue;
            }

            let value: Value = serde_json::from_str(frame.to_text()?)
                .context("failed to parse CDP response as json")?;
            let response_id = value.get("id").and_then(Value::as_u64);
            if response_id == Some(id) {
                if let Some(err) = value.get("error") {
                    return Err(anyhow!("CDP command failed: {err}"));
                }
                return Ok(value);
            }
        }

        Err(anyhow!("CDP response stream ended before command reply"))
    }
}

#[derive(Debug)]
struct ManagedTenant {
    id: String,
    ws_endpoint: String,
    user_data_dir: PathBuf,
    child: Mutex<Child>,
    cdp: Arc<Mutex<CdpClient>>,
    active_sessions: Mutex<usize>,
    last_used: Mutex<Instant>,
}

impl ManagedTenant {
    async fn touch(&self) {
        let mut last = self.last_used.lock().await;
        *last = Instant::now();
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

#[derive(Debug)]
struct ManagedSession {
    id: String,
    tenant_id: String,
    browser_context_id: String,
    target_id: Mutex<Option<String>>,
    cdp: Arc<Mutex<CdpClient>>,
    created_at: Instant,
    created_epoch_ms: u128,
    last_used: Mutex<Instant>,
}

impl ManagedSession {
    async fn touch(&self) {
        let mut guard = self.last_used.lock().await;
        *guard = Instant::now();
    }

    async fn last_used(&self) -> Instant {
        *self.last_used.lock().await
    }
}

#[derive(Clone)]
pub struct BrowserManager {
    cfg: BrowserConfig,
    sessions: Arc<RwLock<HashMap<String, Arc<ManagedSession>>>>,
    tenants: Arc<RwLock<HashMap<String, Arc<ManagedTenant>>>>,
}

impl BrowserManager {
    pub fn new(cfg: BrowserConfig) -> Self {
        Self {
            cfg,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            tenants: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_session(&self, options: SessionCreateOptions) -> Result<SessionSnapshot> {
        self.ensure_capacity(&options.tenant_id).await?;

        let tenant = self.get_or_launch_tenant(&options.tenant_id, options.proxy.as_deref()).await?;

        let context_response = {
            let mut cdp = tenant.cdp.lock().await;
            cdp.send_command("Target.createBrowserContext", json!({}), None)
                .await
        }
        .context("failed to create browser context")?;

        let browser_context_id = context_response
            .get("result")
            .and_then(|v| v.get("browserContextId"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Target.createBrowserContext missing browserContextId"))?
            .to_string();

        let session_id = Uuid::new_v4().to_string();
        let session = Arc::new(ManagedSession {
            id: session_id.clone(),
            tenant_id: options.tenant_id.clone(),
            browser_context_id: browser_context_id.clone(),
            target_id: Mutex::new(None),
            cdp: Arc::clone(&tenant.cdp),
            created_at: Instant::now(),
            created_epoch_ms: unix_time_ms(),
            last_used: Mutex::new(Instant::now()),
        });

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.clone(), session);
        }

        {
            let mut active = tenant.active_sessions.lock().await;
            *active = active.saturating_add(1);
        }
        tenant.touch().await;

        Ok(SessionSnapshot {
            session_id,
            tenant_id: options.tenant_id,
            ws_endpoint: tenant.ws_endpoint.clone(),
            browser_context_id,
            user_data_dir: tenant.user_data_dir.display().to_string(),
            created_at_ms: unix_time_ms(),
            last_used_ms: 0,
        })
    }

    pub async fn close_session(&self, session_id: &str) -> Result<bool> {
        let maybe_session = {
            let mut sessions = self.sessions.write().await;
            sessions.remove(session_id)
        };

        let Some(session) = maybe_session else {
            return Ok(false);
        };

        {
            let mut cdp = session.cdp.lock().await;
            if let Err(err) = cdp
                .send_command(
                    "Target.disposeBrowserContext",
                    json!({"browserContextId": session.browser_context_id}),
                    None,
                )
                .await
            {
                warn!(session_id = %session.id, error = %err, "failed to dispose browser context");
            }
        }

        if let Some(tenant) = self.get_tenant(&session.tenant_id).await {
            let should_close_tenant = {
                let mut active = tenant.active_sessions.lock().await;
                if *active > 0 {
                    *active -= 1;
                }
                *active == 0
            };
            tenant.touch().await;
            if should_close_tenant {
                self.close_tenant(&session.tenant_id).await;
            }
        }

        Ok(true)
    }

    pub async fn navigate(&self, session_id: &str, url: &str) -> Result<String> {
        let session = self.get_session(session_id).await?;

        let target_response = {
            let mut cdp = session.cdp.lock().await;
            cdp.send_command(
                "Target.createTarget",
                json!({
                    "url": url,
                    "browserContextId": session.browser_context_id,
                }),
                None,
            )
            .await
        }
        .context("Target.createTarget failed")?;

        let target_id = target_response
            .get("result")
            .and_then(|v| v.get("targetId"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Target.createTarget response missing targetId"))?
            .to_string();

        {
            let mut target_guard = session.target_id.lock().await;
            *target_guard = Some(target_id.clone());
        }

        session.touch().await;
        if let Some(tenant) = self.get_tenant(&session.tenant_id).await {
            tenant.touch().await;
        }

        Ok(target_id)
    }

    pub async fn evaluate(&self, session_id: &str, expression: &str) -> Result<Value> {
        let session = self.get_session(session_id).await?;

        let target_id = {
            let maybe_target = session.target_id.lock().await.clone();
            match maybe_target {
                Some(value) => value,
                None => self.navigate(session_id, "about:blank").await?,
            }
        };

        let attach_response = {
            let mut cdp = session.cdp.lock().await;
            cdp.send_command(
                "Target.attachToTarget",
                json!({
                    "targetId": target_id,
                    "flatten": true,
                }),
                None,
            )
            .await
        }
        .context("Target.attachToTarget failed")?;

        let cdp_session_id = attach_response
            .get("result")
            .and_then(|v| v.get("sessionId"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Target.attachToTarget response missing sessionId"))?
            .to_string();

        let eval_response = {
            let mut cdp = session.cdp.lock().await;
            cdp.send_command(
                "Runtime.evaluate",
                json!({
                    "expression": expression,
                    "returnByValue": true,
                    "awaitPromise": true,
                }),
                Some(&cdp_session_id),
            )
            .await
        }
        .context("Runtime.evaluate failed")?;

        let result = eval_response
            .get("result")
            .and_then(|v| v.get("result"))
            .cloned()
            .ok_or_else(|| anyhow!("Runtime.evaluate response missing result"))?;

        session.touch().await;
        if let Some(tenant) = self.get_tenant(&session.tenant_id).await {
            tenant.touch().await;
        }

        Ok(result)
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
        let session_ids: Vec<String> = {
            let sessions = self.sessions.read().await;
            sessions.keys().cloned().collect()
        };

        for id in session_ids {
            if let Err(err) = self.close_session(&id).await {
                warn!(session_id = %id, error = %err, "shutdown_all could not close session");
            }
        }

        let tenant_ids: Vec<String> = {
            let tenants = self.tenants.read().await;
            tenants.keys().cloned().collect()
        };
        for tenant_id in tenant_ids {
            self.close_tenant(&tenant_id).await;
        }
    }

    pub async fn session_snapshot(&self, session_id: &str) -> Option<SessionSnapshot> {
        let session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        }?;

        let tenant = self.get_tenant(&session.tenant_id).await?;
        let last_used = session.last_used().await;

        Some(SessionSnapshot {
            session_id: session.id.clone(),
            tenant_id: session.tenant_id.clone(),
            ws_endpoint: tenant.ws_endpoint.clone(),
            browser_context_id: session.browser_context_id.clone(),
            user_data_dir: tenant.user_data_dir.display().to_string(),
            created_at_ms: session.created_epoch_ms,
            last_used_ms: last_used.elapsed().as_millis(),
        })
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

    async fn get_session(&self, session_id: &str) -> Result<Arc<ManagedSession>> {
        let session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };
        session.ok_or_else(|| anyhow!("session not found"))
    }

    async fn get_tenant(&self, tenant_id: &str) -> Option<Arc<ManagedTenant>> {
        let tenants = self.tenants.read().await;
        tenants.get(tenant_id).cloned()
    }

    async fn get_or_launch_tenant(
        &self,
        tenant_id: &str,
        proxy: Option<&str>,
    ) -> Result<Arc<ManagedTenant>> {
        if let Some(existing) = self.get_tenant(tenant_id).await {
            return Ok(existing);
        }

        let new_tenant = self.launch_tenant(tenant_id, proxy).await?;
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::clone(&new_tenant));
        Ok(Arc::clone(tenant))
    }

    async fn launch_tenant(&self, tenant_id: &str, proxy: Option<&str>) -> Result<Arc<ManagedTenant>> {
        let tenant_root = self.cfg.session_root.join(tenant_id);
        tokio::fs::create_dir_all(&tenant_root)
            .await
            .with_context(|| format!("failed creating tenant dir: {}", tenant_root.display()))?;

        let (child, ws_endpoint) = self.launch_chromium(&tenant_root, proxy).await?;
        let cdp_client = CdpClient::connect(&ws_endpoint).await?;

        Ok(Arc::new(ManagedTenant {
            id: tenant_id.to_string(),
            ws_endpoint,
            user_data_dir: tenant_root,
            child: Mutex::new(child),
            cdp: Arc::new(Mutex::new(cdp_client)),
            active_sessions: Mutex::new(0),
            last_used: Mutex::new(Instant::now()),
        }))
    }

    async fn close_tenant(&self, tenant_id: &str) {
        let maybe_tenant = {
            let mut tenants = self.tenants.write().await;
            tenants.remove(tenant_id)
        };

        let Some(tenant) = maybe_tenant else {
            return;
        };

        let active_sessions = *tenant.active_sessions.lock().await;
        if active_sessions > 0 {
            let mut tenants = self.tenants.write().await;
            tenants.insert(tenant_id.to_string(), tenant);
            return;
        }

        if let Err(err) = tenant.shutdown(self.cfg.graceful_shutdown_timeout).await {
            warn!(tenant_id = %tenant.id, error = %err, "tenant browser shutdown failed");
        }
        self.cleanup_dir(&tenant.user_data_dir).await;
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
                    Err(err) => return Err(anyhow!(err).context("failed reading chromium stderr")),
                }
            }
        };

        let ws_endpoint = timeout(self.cfg.launch_timeout, read_future)
            .await
            .context("timeout waiting for chromium devtools endpoint")??;

        Ok((child, ws_endpoint))
    }

    async fn cleanup_dir(&self, dir: &Path) {
        if let Err(err) = tokio::fs::remove_dir_all(dir).await {
            warn!(path = %dir.display(), error = %err, "failed to cleanup directory");
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
