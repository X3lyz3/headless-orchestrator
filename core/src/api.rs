use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tracing::{info, warn};

use crate::browser::{BrowserManager, SessionCreateOptions};

const PROTOCOL_VERSION: &str = "1.0";

#[derive(Clone, Debug)]
pub struct ApiConfig {
    pub bind_addr: String,
    pub api_key: String,
    pub command_timeout: Duration,
}

#[derive(Deserialize, Debug)]
struct ClientEnvelope {
    protocol_version: String,
    request_id: String,
    action: String,
    #[serde(default)]
    payload: Value,
}

#[derive(Serialize, Debug)]
struct ServerEnvelope {
    protocol_version: &'static str,
    request_id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ApiErrorBody>,
}

#[derive(Serialize, Debug)]
struct EventEnvelope {
    protocol_version: &'static str,
    event: String,
    session_id: String,
    data: Value,
}

#[derive(Serialize, Debug)]
struct ApiErrorBody {
    code: &'static str,
    message: String,
}

pub async fn serve_api(cfg: ApiConfig, browser: Arc<BrowserManager>) -> Result<()> {
    let listener = TcpListener::bind(&cfg.bind_addr)
        .await
        .with_context(|| format!("failed to bind ws listener at {}", cfg.bind_addr))?;

    info!(bind = %cfg.bind_addr, "ws api started");

    loop {
        let (stream, peer_addr) = listener.accept().await.context("accept failed")?;
        let browser_clone = Arc::clone(&browser);
        let cfg_clone = cfg.clone();

        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, browser_clone, cfg_clone).await {
                warn!(peer = %peer_addr, error = %err, "connection finished with error");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    browser: Arc<BrowserManager>,
    cfg: ApiConfig,
) -> Result<()> {
    let expected_api_key = cfg.api_key.clone();
    let ws_stream = accept_hdr_async(stream, move |req: &Request, response: Response| {
        let is_valid = req
            .headers()
            .get("x-api-key")
            .and_then(|h| h.to_str().ok())
            .map(|v| v == expected_api_key)
            .unwrap_or(false);

        if is_valid {
            Ok(response)
        } else {
            match tokio_tungstenite::tungstenite::http::Response::builder()
                .status(tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED)
                .body(Some("unauthorized: missing/invalid x-api-key".to_string()))
            {
                Ok(error_response) => Err(error_response),
                Err(_) => Err(tokio_tungstenite::tungstenite::http::Response::new(Some(
                    "unauthorized".to_string(),
                ))),
            }
        }
    })
    .await
    .context("websocket handshake failed")?;

    let (mut sink, mut stream) = ws_stream.split();

    while let Some(msg) = stream.next().await {
        let msg = msg.context("error reading websocket frame")?;
        if msg.is_close() {
            break;
        }

        if !msg.is_text() {
            continue;
        }

        let text = msg.to_text().context("non-text websocket message")?;
        let request: ClientEnvelope = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(err) => {
                let response = build_error_response(
                    "unknown",
                    "ValidationError",
                    format!("invalid json payload: {err}"),
                );
                sink.send(Message::Text(to_json(response)?)).await?;
                continue;
            }
        };

        if request.protocol_version != PROTOCOL_VERSION {
            let response = build_error_response(
                &request.request_id,
                "ValidationError",
                "unsupported protocol version".to_string(),
            );
            sink.send(Message::Text(to_json(response)?)).await?;
            continue;
        }

        let action_result = timeout(
            cfg.command_timeout,
            route_action(&request.action, request.payload.clone(), Arc::clone(&browser)),
        )
        .await;

        match action_result {
            Ok(Ok((data, maybe_event))) => {
                let response = ServerEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    request_id: request.request_id.clone(),
                    status: "ok".to_string(),
                    data: Some(data),
                    error: None,
                };
                sink.send(Message::Text(to_json(response)?)).await?;

                if let Some(event) = maybe_event {
                    sink.send(Message::Text(to_json(event)?)).await?;
                }
            }
            Ok(Err(err)) => {
                let response = build_error_response(
                    &request.request_id,
                    "InternalError",
                    err.to_string(),
                );
                sink.send(Message::Text(to_json(response)?)).await?;
            }
            Err(_) => {
                let response = build_error_response(
                    &request.request_id,
                    "TimeoutError",
                    "command timeout".to_string(),
                );
                sink.send(Message::Text(to_json(response)?)).await?;
            }
        }
    }

    Ok(())
}

async fn route_action(
    action: &str,
    payload: Value,
    browser: Arc<BrowserManager>,
) -> Result<(Value, Option<EventEnvelope>)> {
    match action {
        "health.ping" => {
            let data = json!({
                "status": "pong",
                "active_sessions": browser.active_sessions_count().await,
            });
            Ok((data, None))
        }
        "session.create" => {
            let tenant_id = payload
                .get("tenant_id")
                .and_then(Value::as_str)
                .unwrap_or("default")
                .to_string();
            let proxy = payload
                .get("proxy")
                .and_then(Value::as_str)
                .map(ToString::to_string);

            let snapshot = browser
                .create_session(SessionCreateOptions { tenant_id, proxy })
                .await
                .context("session.create failed")?;

            let data = serde_json::to_value(&snapshot).context("serialize session snapshot failed")?;
            let event = EventEnvelope {
                protocol_version: PROTOCOL_VERSION,
                event: "session.created".to_string(),
                session_id: snapshot.session_id.clone(),
                data: json!({ "tenant_id": snapshot.tenant_id }),
            };

            Ok((data, Some(event)))
        }
        "session.close" => {
            let session_id = payload
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("session_id is required"))?;

            let closed = browser.close_session(session_id).await?;
            let data = json!({
                "session_id": session_id,
                "closed": closed,
            });

            let event = EventEnvelope {
                protocol_version: PROTOCOL_VERSION,
                event: "session.closed".to_string(),
                session_id: session_id.to_string(),
                data: json!({ "closed": closed }),
            };

            Ok((data, Some(event)))
        }
        "session.navigate" => {
            let session_id = payload
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("session_id is required"))?;
            let url = payload
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("url is required"))?;

            let target_id = browser.navigate(session_id, url).await?;
            let data = json!({
                "session_id": session_id,
                "url": url,
                "target_id": target_id,
            });

            Ok((data, None))
        }
        "session.eval" => {
            let session_id = payload
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("session_id is required"))?;
            let expression = payload
                .get("expression")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("expression is required"))?;

            let eval_result = browser.evaluate(session_id, expression).await?;
            let data = json!({
                "session_id": session_id,
                "result": eval_result,
            });

            Ok((data, None))
        }
        _ => Err(anyhow!("unknown action: {action}")),
    }
}

fn build_error_response(request_id: &str, code: &'static str, message: String) -> ServerEnvelope {
    ServerEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request_id.to_string(),
        status: "error".to_string(),
        data: None,
        error: Some(ApiErrorBody { code, message }),
    }
}

fn to_json<T: Serialize>(value: T) -> Result<String> {
    serde_json::to_string(&value).context("serialization failed")
}
