from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from typing import Any

import websockets
from websockets.client import WebSocketClientProtocol


class OrchestratorError(RuntimeError):
	"""Base exception for SDK-level errors."""


class ProtocolError(OrchestratorError):
	"""Raised when protocol invariants are violated."""


class CommandError(OrchestratorError):
	"""Raised when orchestrator returns application-level error."""


@dataclass(slots=True)
class ClientConfig:
	url: str = "ws://127.0.0.1:9001"
	api_key: str = "dev-api-key"
	protocol_version: str = "1.0"
	timeout_seconds: float = 15.0


class OrchestratorClient:
	def __init__(self, config: ClientConfig | None = None) -> None:
		self._config = config or ClientConfig()
		self._ws: WebSocketClientProtocol | None = None
		self._lock = asyncio.Lock()

	async def __aenter__(self) -> "OrchestratorClient":
		await self.connect()
		return self

	async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
		await self.close()

	async def connect(self) -> None:
		if self._ws is not None:
			return
		self._ws = await websockets.connect(
			self._config.url,
			extra_headers={"x-api-key": self._config.api_key},
			open_timeout=self._config.timeout_seconds,
		)

	async def close(self) -> None:
		if self._ws is None:
			return
		await self._ws.close()
		self._ws = None

	async def health_ping(self) -> dict[str, Any]:
		return await self.send_command("health.ping", {})

	async def create_session(
		self,
		tenant_id: str = "default",
		proxy: str | None = None,
	) -> dict[str, Any]:
		payload: dict[str, Any] = {"tenant_id": tenant_id}
		if proxy is not None:
			payload["proxy"] = proxy
		return await self.send_command("session.create", payload)

	async def navigate(self, session_id: str, url: str) -> dict[str, Any]:
		return await self.send_command(
			"session.navigate",
			{
				"session_id": session_id,
				"url": url,
			},
		)

	async def evaluate(self, session_id: str, expression: str) -> dict[str, Any]:
		return await self.send_command(
			"session.eval",
			{
				"session_id": session_id,
				"expression": expression,
			},
		)

	async def close_session(self, session_id: str) -> dict[str, Any]:
		return await self.send_command("session.close", {"session_id": session_id})

	async def send_command(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
		async with self._lock:
			if self._ws is None:
				await self.connect()
			if self._ws is None:
				raise ProtocolError("websocket connection was not established")

			request_id = str(uuid.uuid4())
			envelope = {
				"protocol_version": self._config.protocol_version,
				"request_id": request_id,
				"action": action,
				"payload": payload,
			}

			await self._ws.send(json.dumps(envelope))
			response = await asyncio.wait_for(
				self._ws.recv(),
				timeout=self._config.timeout_seconds,
			)

			parsed = self._parse_response(response)
			if "event" in parsed:
				response = await asyncio.wait_for(
					self._ws.recv(),
					timeout=self._config.timeout_seconds,
				)
				parsed = self._parse_response(response)

			response_request_id = parsed.get("request_id")
			if response_request_id != request_id:
				raise ProtocolError(
					f"response request_id mismatch: expected {request_id}, got {response_request_id}"
				)

			status = parsed.get("status")
			if status == "ok":
				data = parsed.get("data")
				if isinstance(data, dict):
					return data
				raise ProtocolError("expected object in response data")

			error = parsed.get("error")
			if isinstance(error, dict):
				code = error.get("code", "UnknownError")
				message = error.get("message", "unknown error")
				raise CommandError(f"{code}: {message}")

			raise ProtocolError("response contains invalid error payload")

	@staticmethod
	def _parse_response(raw: Any) -> dict[str, Any]:
		if not isinstance(raw, str):
			raise ProtocolError("received non-text websocket frame")
		try:
			parsed = json.loads(raw)
		except json.JSONDecodeError as exc:
			raise ProtocolError(f"invalid json response: {exc}") from exc
		if not isinstance(parsed, dict):
			raise ProtocolError("expected object in json response")
		return parsed
