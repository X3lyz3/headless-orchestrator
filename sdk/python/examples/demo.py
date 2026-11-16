import asyncio
from client import ClientConfig, OrchestratorClient

async def main() -> None:
    cfg = ClientConfig(url="ws://127.0.0.1:9001", api_key="dev-api-key")
    async with OrchestratorClient(cfg) as client:
        print("Ping:", await client.health_ping())
        
        session = await client.create_session(tenant_id="client_1")
        session_id = session["session_id"]
        print("Session created:", session_id)
        
        # 3. Відкриття сторінки
        await client.navigate(session_id, "https://example.com")
        
        # Чекаємо завантаження сторінки (і паралельно дивимось у вікно браузера)
        await asyncio.sleep(5)
        
        title = await client.evaluate(session_id, "document.title")
        print("Page title:", title)
        
        # Ще трохи часу подивитися, якщо треба
        await asyncio.sleep(5)
        
        # 4. Закриття сесії
        await client.close_session(session_id)

if __name__ == "__main__":
    asyncio.run(main())
    