import json
from typing import Any, cast

import pytest

from client import ClientConfig, CommandError, OrchestratorClient, ProtocolError


@pytest.mark.asyncio
async def test_parse_response_rejects_non_json() -> None:
    with pytest.raises(ProtocolError):
        OrchestratorClient._parse_response("not-json")


@pytest.mark.asyncio
async def test_send_command_maps_server_error() -> None:
    client = OrchestratorClient(ClientConfig(url="ws://test"))

    class FakeWS:
        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            return json.dumps(
                {
                    "protocol_version": "1.0",
                    "request_id": "different-id",
                    "status": "error",
                    "error": {"code": "ValidationError", "message": "bad payload"},
                }
            )

    client._ws = cast(Any, FakeWS())

    with pytest.raises(ProtocolError):
        await client.send_command("health.ping", {})


@pytest.mark.asyncio
async def test_send_command_raises_command_error_with_matching_id(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_id = "123e4567-e89b-12d3-a456-426614174000"

    class _FixedUUID:
        def __str__(self) -> str:
            return fixed_id

    monkeypatch.setattr("client.uuid.uuid4", lambda: _FixedUUID())

    client = OrchestratorClient(ClientConfig(url="ws://test"))

    class FakeWS:
        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            return json.dumps(
                {
                    "protocol_version": "1.0",
                    "request_id": fixed_id,
                    "status": "error",
                    "error": {"code": "ValidationError", "message": "bad payload"},
                }
            )

    client._ws = cast(Any, FakeWS())

    with pytest.raises(CommandError):
        await client.send_command("health.ping", {})
