# Headless Orchestrator – Python SDK

An asynchronous Python client for the Headless Browser Orchestrator (the Rust core in `core/`). It talks to the orchestrator over WebSocket and lets you create isolated browser sessions, navigate, run JavaScript and close sessions from your own code.

## Requirements

- Python 3.10 or newer
- A running orchestrator (see the [main README](../../README.md#run-the-rust-core))
- The API key configured in the orchestrator (`ORCH_API_KEY`)

## Installation

If you have not downloaded the project yet:

```bash
git clone https://github.com/<your-username>/<your-repository>.git
cd <your-repository>/sdk/python
```

Or download the ZIP from GitHub (**Code → Download ZIP**), unpack it and open `sdk/python`.

Then install in editable mode with the development dependencies (pytest, pytest-asyncio):

```bash
python -m pip install -e ".[dev]"
```

For runtime use only, without test dependencies:

```bash
python -m pip install -e .
```

## Quick start

```python
import asyncio

from client import ClientConfig, OrchestratorClient


async def main() -> None:
    cfg = ClientConfig(url="ws://127.0.0.1:9001", api_key="dev-api-key")

    async with OrchestratorClient(cfg) as client:
        print(await client.health_ping())

        session = await client.create_session(tenant_id="acme")
        session_id = session["session_id"]

        await client.navigate(session_id, "https://example.com")
        print(await client.evaluate(session_id, "document.title"))

        await client.close_session(session_id)


if __name__ == "__main__":
    asyncio.run(main())
```

## API overview

| Call | What it does |
|---|---|
| `OrchestratorClient(cfg)` | Create a client. Use it as an async context manager (`async with`) so the connection is closed for you. |
| `await client.health_ping()` | Check that the server is alive; returns the number of active sessions. |
| `await client.create_session(tenant_id=...)` | Create an isolated browser session; returns a dict with `session_id` and other details. |
| `await client.navigate(session_id, url)` | Open a URL in the session. |
| `await client.evaluate(session_id, expression)` | Run a JavaScript expression and return its result. |
| `await client.close_session(session_id)` | Close the session and free its resources. |
| `await client.send_command(action, payload)` | Low-level call for any protocol action. |

## Errors

- `ProtocolError` – malformed server response, or a response whose `request_id` does not match the request.
- `CommandError` – the server answered with an error status (for example `ValidationError`, `TimeoutError`, `InternalError`).

## Running the tests

From the repository root:

```bash
python -m pytest -q -c sdk/python/pyproject.toml sdk/python/tests
```

The tests use a local fake WebSocket server; they do not need Chromium or the Rust core.

## Troubleshooting

- **Handshake rejected (HTTP 401):** the API key in `ClientConfig` does not match `ORCH_API_KEY` on the server.
- **Connection refused:** the Rust core is not running, or the host/port is different from `ORCH_HOST` / `ORCH_PORT`.
- **`TimeoutError` on `create_session`:** Chromium could not start in time. Check `ORCH_CHROMIUM_PATH` and consider raising `ORCH_LAUNCH_TIMEOUT_MS`.
