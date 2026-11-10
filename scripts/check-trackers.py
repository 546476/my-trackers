import asyncio
import gzip
import io
import json
import re
import socket
import struct
import urllib.parse
import urllib.request
from urllib.parse import urljoin, urlparse

import aiohttp

from get_trackers import urls as SOURCE_URLS

HEADERS = {
    "User-Agent": "qBittorrentTrackerUpdater/1.0 (+https://github.com/546476/my-trackers)",
    "Accept": "text/plain, text/html, application/json, */*",
    "Accept-Encoding": "gzip",
}

TRACKER_RE = re.compile(r"(?:udp|https?)://[^\s<>\"']+", re.IGNORECASE)

PRIVATE_RE = re.compile(
    r"(?:passkey|authkey|token|api_key|apikey|secret)=",
    re.IGNORECASE,
)

DOWNLOAD_LINK_RE = re.compile(
    r"https?://[^\s<>\"']+\.(?:txt|list)(?:\?[^\s<>\"']*)?",
    re.IGNORECASE,
)

OUTPUT_FILE = "trackers.txt"
FETCH_TIMEOUT = 25
CHECK_TIMEOUT = 8
MAX_CONCURRENT_CHECKS = 100


def extract_trackers(text):
    found = set()

    for tracker in TRACKER_RE.findall(text):
        tracker = tracker.strip().rstrip(".,;)]}\"'")

        parsed = urlparse(tracker)

        if parsed.scheme.lower() not in ("udp", "http", "https"):
            continue

        if not parsed.hostname:
            continue

        if PRIVATE_RE.search(tracker):
            continue

        found.add(tracker)

    return found


def json_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from json_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from json_strings(item)


async def fetch_text(session, url):
    async with session.get(
        url,
        headers=HEADERS,
        timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
    ) as response:
        response.raise_for_status()
        data = await response.read()

    if data[:2] == b"\x1f\x8b":
        try:
            data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
        except OSError:
            pass

    charset = response.get_encoding() or "utf-8"
    return data.decode(charset, errors="ignore")


async def process_source(session, url, trackers, failed, depth=0):
    try:
        text = await fetch_text(session, url)
        trackers.update(extract_trackers(text))

        stripped = text.lstrip()

        if stripped.startswith(("{", "[")):
            try:
                data = json.loads(text)

                for value in json_strings(data):
                    trackers.update(extract_trackers(value))
            except (json.JSONDecodeError, TypeError):
                pass

        if depth == 0 and stripped.startswith("<"):
            links = set()

            for link in DOWNLOAD_LINK_RE.findall(text):
                absolute = urljoin(url, link)

                if urlparse(absolute).netloc:
                    links.add(absolute)

            for link in list(links)[:10]:
                await process_source(session, link, trackers, failed, depth=1)

    except Exception as error:
        failed.append(f"{url} - {error}")
        print(f"FAILED: {url}: {error}")


async def check_http_tracker(session, semaphore, tracker):
    async with semaphore:
        try:
            async with session.get(
                tracker,
                timeout=aiohttp.ClientTimeout(total=CHECK_TIMEOUT),
                allow_redirects=False,
            ) as response:
                return 200 <= response.status < 300
        except Exception:
            return False


async def check_udp_tracker(semaphore, tracker):
    async with semaphore:
        parsed = urlparse(tracker)
        host = parsed.hostname
        port = parsed.port or 80

        try:
            loop = asyncio.get_running_loop()
            address = await loop.getaddrinfo(host, port, type=socket.SOCK_DGRAM)
            _, _, _, _, target = address[0]

            transport, protocol = await loop.create_datagram_endpoint(
                lambda: asyncio.DatagramProtocol(),
                remote_addr=target,
            )

            try:
                transaction_id = 0x41727101980
                request = struct.pack(
                    "!QII",
                    0x41727101980,
                    0,
                    transaction_id,
                )

                future = loop.create_future()

                def datagram_received(data, addr):
                    if not future.done():
                        future.set_result(data)

                protocol.datagram_received = datagram_received

                transport.sendto(request)
                response = await asyncio.wait_for(future, CHECK_TIMEOUT)

                if len(response) < 16:
                    return False

                action, received_transaction_id = struct.unpack(
                    "!II",
                    response[:8],
                )

                return (
                    action == 0
                    and received_transaction_id == transaction_id
                )
            finally:
                transport.close()

        except Exception:
            return False


async def main():
    trackers = set()
    failed = []

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CHECKS)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            process_source(session, url, trackers, failed)
            for url in SOURCE_URLS
        ]

        await asyncio.gather(*tasks)

    http_trackers = sorted(
        tracker
        for tracker in trackers
        if tracker.startswith(("http://", "https://"))
    )

    udp_trackers = sorted(
        tracker
        for tracker in trackers
        if tracker.startswith("udp://")
    )

    print(f"Collected {len(trackers)} unique trackers")
    print(f"HTTP/HTTPS: {len(http_trackers)}")
    print(f"UDP: {len(udp_trackers)}")
    print(f"Failed sources: {len(failed)}")

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=MAX_CONCURRENT_CHECKS)
    ) as session:
        http_tasks = [
            check_http_tracker(session, semaphore, tracker)
            for tracker in http_trackers
        ]

        udp_tasks = [
            check_udp_tracker(semaphore, tracker)
            for tracker in udp_trackers
        ]

        http_results, udp_results = await asyncio.gather(
            asyncio.gather(*http_tasks),
            asyncio.gather(*udp_tasks),
        )

    working_http_trackers = [
        tracker
        for tracker, is_working in zip(http_trackers, http_results)
        if is_working
    ]

    working_udp_trackers = [
        tracker
        for tracker, is_working in zip(udp_trackers, udp_results)
        if is_working
    ]

    final_trackers = sorted(working_udp_trackers + working_http_trackers)

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as file:
        file.write("\n".join(final_trackers))

        if final_trackers:
            file.write("\n")

    print(f"Saved {len(final_trackers)} working trackers")
    print(f"HTTP/HTTPS working: {len(working_http_trackers)}")
    print(f"UDP working: {len(working_udp_trackers)}")


if __name__ == "__main__":
    asyncio.run(main())
