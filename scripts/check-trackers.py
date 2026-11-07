import asyncio
import aiohttp

KEYWORDS = {
    "announce",
    "peers",
    "info_hash",
    "completed",
    "downloaded",
    "seeders",
    "leechers",
}

async def check_tracker(session: aiohttp.ClientSession, tracker_url: str) -> bool:
    try:
        async with session.get(
            tracker_url,
            timeout=aiohttp.ClientTimeout(
                total=10,
                connect=5,
                sock_connect=5,
                sock_read=5,
            ),
        ) as response:
            if response.status != 200:
                return False

            content = await response.text(errors="ignore")
            lower_content = content.lower()

            for keyword in KEYWORDS:
                if keyword in lower_content:
                    return True

            return len(content.strip()) > 10

    except Exception:
        return False

async def main():
    with open("trackers.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()

    trackers = [
        line.strip()
        for line in lines
        if line.strip() and not line.strip().startswith("#")
    ]

    udp_trackers = [
        url for url in trackers if url.lower().startswith("udp://")
    ]

    http_trackers = [
        url for url in trackers if not url.lower().startswith("udp://")
    ]

    semaphore = asyncio.Semaphore(110)

    async def check_with_limit(url):
        async with semaphore:
            return await check_tracker(session, url)

    connector = aiohttp.TCPConnector(
        limit=110,
        limit_per_host=8,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; tracker-checker)"
        },
    ) as session:
        tasks = [check_with_limit(url) for url in http_trackers]
        results = await asyncio.gather(*tasks)

    working_http_trackers = [
        url
        for url, success in zip(http_trackers, results)
        if success
    ]

    working_http_trackers.sort()

    final_trackers = udp_trackers + working_http_trackers

    with open("trackers.txt", "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(final_trackers) + "\n")

    print("UDP trackers kept:", len(udp_trackers))
    print("HTTP trackers checked:", len(http_trackers))
    print("HTTP trackers passed:", len(working_http_trackers))
    print("HTTP trackers removed:", len(http_trackers) - len(working_http_trackers))
    print("Total saved:", len(final_trackers))

if __name__ == "__main__":
    asyncio.run(main())
