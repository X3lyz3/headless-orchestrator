# Headless Orchestrator Python SDK

Асинхронний Python SDK для взаємодії з Rust-оркестратором через WebSocket.

## Встановлення

```powershell
python -m pip install -e .[dev]
```

## Мінімальний приклад

```python
import asyncio
from client import ClientConfig, OrchestratorClient


async def main() -> None:
    cfg = ClientConfig(url="ws://127.0.0.1:9001", api_key="dev-api-key")
    async with OrchestratorClient(cfg) as client:
        print(await client.health_ping())


if __name__ == "__main__":
    asyncio.run(main())
```
