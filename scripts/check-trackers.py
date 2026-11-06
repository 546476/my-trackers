import asyncio
import aiohttp
import time
from typing import List, Set
from urllib.parse import urlparse

# Настройки
TIMEOUT = 15  # секунд
CONCURRENT_REQUESTS = 20
CHECK_INTERVAL = 3600  # 1 час — можно настроить в cron

# Список трекеров из trackers.txt
TRACKERS_FILE = "trackers.txt"
OUTPUT_FILE = "results.json"

# Форматы ответов, которые можно ожидать
EXPECTED_RESPONSES = {
    "announce", "scrape", "peers", "info_hash", "list", "trackers"
}

async def check_tracker(session: aiohttp.ClientSession, tracker_url: str) -> dict:
    start_time = time.time()
    try:
        async with session.get(tracker_url, timeout=TIMEOUT) as response:
            if response.status == 200:
                content = await response.text()
                # Проверяем, что ответ содержит ожидаемые ключевые слова
                lower_content = content.lower()
                found = any(resp in lower_content for resp in EXPECTED_RESPONSES)
                return {
                    "tracker": tracker_url,
                    "status": "online",
                    "response_time": time.time() - start_time,
                    "content_length": len(content),
                    "found_keywords": found,
                }
            else:
                return {
                    "tracker": tracker_url,
                    "status": f"HTTP {response.status}",
                    "response_time": time.time() - start_time,
                    "content_length": 0,
                    "found_keywords": False,
                }
    except asyncio.TimeoutError:
        return {
            "tracker": tracker_url,
            "status": "timeout",
            "response_time": time.time() - start_time,
            "content_length": 0,
            "found_keywords": False,
        }
    except Exception as e:
        return {
            "tracker": tracker_url,
            "status": f"error: {str(e)}",
            "response_time": time.time() - start_time,
            "content_length": 0,
            "found_keywords": False,
        }

async def main():
    # Читаем список трекеров
    with open(TRACKERS_FILE, "r", encoding="utf-8") as f:
        trackers = [line.strip() for line in f if line.strip()]

    # Убираем дубликаты
    unique_trackers = list(set(trackers))

    # Создаем сессию
    async with aiohttp.ClientSession() as session:
        # Выполняем проверки параллельно
        tasks = [check_tracker(session, url) for url in unique_trackers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Сохраняем результаты
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        import json
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Выводим статистику
    online = sum(1 for r in results if r["status"] == "online")
    timeout = sum(1 for r in results if r["status"] == "timeout")
    errors = sum(1 for r in results if r["status"].startswith("error"))
    total = len(results)

    print(f"Проверено {total} трекеров")
    print(f"Доступны: {online} ({online/total*100:.1f}%)")
    print(f"Таймаут: {timeout}")
    print(f"Ошибки: {errors}")
    print(f"Результаты сохранены в {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
