mod api;
mod browser;

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use api::{ApiConfig, serve_api};
use browser::{BrowserConfig, BrowserManager};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let browser_cfg = load_browser_config().context("failed to load browser configuration")?;
    let api_cfg = load_api_config().context("failed to load api configuration")?;

    tokio::fs::create_dir_all(&browser_cfg.session_root)
        .await
        .with_context(|| {
            format!(
                "failed creating session root directory: {}",
                browser_cfg.session_root.display()
            )
        })?;

    let manager = Arc::new(BrowserManager::new(browser_cfg));
    Arc::clone(&manager).spawn_watchdog();

    let shutdown_manager = Arc::clone(&manager);
    let shutdown_signal = async move {
        if let Err(err) = signal::ctrl_c().await {
            error!(error = %err, "failed to listen for Ctrl+C");
        }
        info!("shutdown signal received");
        shutdown_manager.shutdown_all().await;
        info!("all browser sessions terminated");
    };

    tokio::select! {
        server_result = serve_api(api_cfg, Arc::clone(&manager)) => {
            server_result
        }
        _ = shutdown_signal => {
            Ok(())
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn load_api_config() -> Result<ApiConfig> {
    let host = env::var("ORCH_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("ORCH_PORT").unwrap_or_else(|_| "9001".to_string());
    let api_key = env::var("ORCH_API_KEY").unwrap_or_else(|_| "dev-api-key".to_string());
    let timeout_ms = env::var("ORCH_COMMAND_TIMEOUT_MS")
        .unwrap_or_else(|_| "15000".to_string())
        .parse::<u64>()
        .context("ORCH_COMMAND_TIMEOUT_MS must be u64")?;

    Ok(ApiConfig {
        bind_addr: format!("{host}:{port}"),
        api_key,
        command_timeout: Duration::from_millis(timeout_ms),
    })
}

fn load_browser_config() -> Result<BrowserConfig> {
    let chromium_path = env::var("ORCH_CHROMIUM_PATH").unwrap_or_else(|_| "chrome".to_string());
    let session_root = env::var("ORCH_SESSION_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./.orchestrator-sessions"));

    let launch_timeout_ms = parse_env_u64("ORCH_LAUNCH_TIMEOUT_MS", 15_000)?;
    let graceful_timeout_ms = parse_env_u64("ORCH_GRACEFUL_SHUTDOWN_TIMEOUT_MS", 5_000)?;
    let max_sessions = parse_env_usize("ORCH_MAX_SESSIONS", 100)?;
    let max_sessions_per_tenant = parse_env_usize("ORCH_MAX_SESSIONS_PER_TENANT", 20)?;
    let idle_ttl_sec = parse_env_u64("ORCH_IDLE_TTL_SEC", 120)?;
    let hard_ttl_sec = parse_env_u64("ORCH_HARD_TTL_SEC", 900)?;

    Ok(BrowserConfig {
        chromium_path,
        session_root,
        launch_timeout: Duration::from_millis(launch_timeout_ms),
        graceful_shutdown_timeout: Duration::from_millis(graceful_timeout_ms),
        max_sessions,
        max_sessions_per_tenant,
        idle_ttl: Duration::from_secs(idle_ttl_sec),
        hard_ttl: Duration::from_secs(hard_ttl_sec),
    })
}

fn parse_env_u64(key: &str, default: u64) -> Result<u64> {
    match env::var(key) {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("{key} must be u64")),
        Err(_) => Ok(default),
    }
}

fn parse_env_usize(key: &str, default: usize) -> Result<usize> {
    match env::var(key) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("{key} must be usize")),
        Err(_) => Ok(default),
    }
}
