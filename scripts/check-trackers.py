import asyncio
import aiohttp
import re
            # Проверяем наличие ключевых слов
            for keyword in KEYWORDS:
                if keyword in lower_content:
                    return True
            # Если есть хотя бы 10 символов — считаем, что трекер работает
            return len(content.strip()) > 10
    except Exception:
        return False

async def main():
    # Читаем список трекеров из trackers.txt
    with open("trackers.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()
    trackers = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]

    # Проверяем каждый трекер
    async with aiohttp.ClientSession() as session:
        tasks = [check_tracker(session, url) for url in trackers]
        results = await asyncio.gather(*tasks)

    # Собираем рабочие трекеры
    working_trackers = [url for url, success in zip(trackers, results) if success]
    working_trackers.sort()

    # Записываем результат в trackers.txt
    with open("trackers.txt", "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(working_trackers))
