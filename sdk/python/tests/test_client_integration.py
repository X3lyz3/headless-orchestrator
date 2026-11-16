from __future__ import annotations

import json

import pytest
import websockets

from client import ClientConfig, OrchestratorClient


@pytest.mark.asyncio
async def test_client_can_connect_and_send_command() -> None:
	async def handler(ws: websockets.WebSocketServerProtocol) -> None:
		request_raw = await ws.recv()
		request = json.loads(request_raw)
		response = {
			"protocol_version": "1.0",
			"request_id": request["request_id"],
			"status": "ok",
			"data": {
				"status": "pong",
				"active_sessions": 0,
			},
		}
		await ws.send(json.dumps(response))

	async with websockets.serve(handler, "127.0.0.1", 0) as server:
		sock = server.sockets[0]
		host, port = sock.getsockname()[:2]

		cfg = ClientConfig(url=f"ws://{host}:{port}", api_key="dev-api-key")
		async with OrchestratorClient(cfg) as client:
			response = await client.health_ping()

	assert response["status"] == "pong"
