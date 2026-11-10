import asyncio
import gzip
import io
import json
import random
import re
import socket
import struct
import urllib.error
import urllib.request
from urllib.parse import urljoin, urlparse

HEADERS = {
    "User-Agent": "qBittorrentTrackerUpdater/1.0 (+https://github.com/546476/my-trackers)",
    "Accept": "text/plain, text/html, application/json, */*",
    "Accept-Encoding": "gzip",
}

# [^\s]+ вместо [^\\s]+
TRACKER_RE = re.compile(r"(?:udp|http|https)://[^\s]+", re.IGNORECASE)

PRIVATE_RE = re.compile(
    r"(?:passkey|authkey|token|api_key|apikey|secret)=",
    re.IGNORECASE,
)

# \S+ и \. вместо S+ и .
DOWNLOAD_LINK_RE = re.compile(
    r"https?://\S+\.(?:txt|list)\b",
    re.IGNORECASE,
)

urls = [
    "https://ngosang.github.io/trackerslist/trackers_all.txt",
    "https://cdn.jsdelivr.net/gh/ngosang/trackerslist@master/trackers_all.txt",
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all.txt",
    "https://raw.githubusercontent.com/00xc0d3/BitTorrent-Tracker-List/refs/heads/main/trackers_all.txt",
    "https://mokk731.github.io/txt/trackerslist.txt",
    "https://raw.githubusercontent.com/chenjia404/CnTrackersList/master/all.txt",
    "https://raw.githubusercontent.com/hezhijie0327/Trackerslist/main/trackerslist_tracker.txt",
    "https://raw.githubusercontent.com/kkkgo/all-tracker-list/main/trackerslist.txt",
    "https://raw.githubusercontent.com/dr5hx/tracker_merge/master/all.txt",
    "https://raw.githubusercontent.com/jhhd88/trackerslist/refs/heads/master/trackerslist.txt",
    "https://newtrackon.com/api/stable",
    "https://cf.trackerslist.com/all.txt",
    "https://trackers.run/s/rw_ws_up_hp_hs_v4_v6.txt",
    "https://torrends.to/torrent-tracker-list/?download=latest",
]

trackers = set()
failed = []

def fetch(url):
    request = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=25) as response:
        data = response.read()
        encoding = (response.headers.get("Content-Encoding") or "").lower()
        if encoding == "gzip" or data[:2] == b"\x1f\x8b":
            try:
                data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
            except OSError:
                pass
        charset = response.headers.get_content_charset() or "utf-8"
        return data.decode(charset, errors="ignore")

def extract_trackers(text):
    found = set()
    for tracker in TRACKER_RE.findall(text):
        tracker = tracker.strip()
        if "://" not in tracker:
            continue
        if " " in tracker:
            continue
        if not tracker.lower().startswith(("udp://", "http://", "https://")):
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

def process_url(url, depth=0):
    try:
        text = fetch(url)
        trackers.update(extract_trackers(text))

        stripped = text.lstrip()
        if stripped.startswith(("{", "[")):
            try:
                data = json.loads(text)
                for value in json_strings(data):
                    trackers.update(extract_trackers(value))
            except json.JSONDecodeError:
                pass

        if depth == 0 and ("<a " in text.lower() or "<html" in text.lower()):
            links = set()
            for link in DOWNLOAD_LINK_RE.findall(text):
                absolute = urljoin(url, link)
                if urlparse(absolute).netloc:
                    links.add(absolute)
            for link in list(links)[:10]:
                process_url(link, depth=1)
    except Exception as error:
        failed.append(f"{url} - {error}")
        print(f"FAILED: {url}: {error}")

ов (sync, выполняются в пуле потоков) -------

def _check_http_sync(url, timeout=10):
    """Шлём announce-запрос. Любой ответ (даже HTTPError) = сервер жив."""
    try:
        info_hash = "".join(f"%{i:02x}" for i in range(20))
        peer_id = "%2DTR2940%2D123456789012"
        sep = "&" if "?" in url else "?"
        test_url = (
            f"{url}{sep}info_hash={info_hash}&peer_id={peer_id}"
            "&port=6881&uploaded=0&downloaded=0&left=0&compact=1&event=started"
        )
        request = urllib.request.Request(test_url, headers=HEADERS)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read(64)
            return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False

def _check_udp_sync(url, timeout=5):
    """UDP connect-запрос: protocol_id 0x41727101980 + action 0 + txn."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or 80
        if not host:
            return False

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout)
        try:
            txn = random.randint(0, 0xFFFFFFFF)
            packet = struct.pack(">QII", 0x41727101980, 0, txn)
            sock.sendto(packet, (host, port))
            data, _ = sock.recvfrom(1024)
            if len(data) >= 16:
                action, rtxn, _conn_id = struct.unpack(">IIQ", data[:16])
                return action == 0 and rtxn == txn
            return False
        finally:
            sock.close()
    except Exception:
        return False

async def check_http_with_limit(sem, url):
    async with sem:
        return await asyncio.to_thread(_check_http_sync, url)

async def check_udp_with_limit(sem, url):
    async with sem:
        return await asyncio.to_thread(_check_udp_sync, url)

# ------------------------- Основной сценарий -------------------------

async def main_async():
    # 1. Синхронно (в отдельном потоке) собираем трекеры из всех источников
    await asyncio.to_thread(lambda: [process_url(u) for u in urls])

    print(f"Собрано трекеров: {len(trackers)}")

    udp_trackers = sorted({t for t in trackers if t.lower().startswith("udp://")})
    http_trackers = sorted(
        {t for t in trackers if t.lower().startswith(("http://", "https://"))}
    )

    print(f"Проверяем UDP: {len(udp_trackers)}")
    print(f"Проверяем HTTP: {len(http_trackers)}")

    http_sem = asyncio.Semaphore(50)
    udp_sem = asyncio.Semaphore(200)

    http_tasks = [check_http_with_limit(http_sem, url) for url in http_trackers]
    udp_tasks = [check_udp_with_limit(udp_sem, url) for url in udp_trackers]

    http_results, udp_results = await asyncio.gather(
        asyncio.gather(*http_tasks),
        asyncio.gather(*udp_tasks),
    )

    # zip-сопоставление гарантирует отсутствие рассинхрона url <-> результат
    working_http_trackers = [
        url for url, ok in zip(http_trackers, http_results) if ok
    ]
    working_udp_trackers = [
        url for url, ok in zip(udp_trackers, udp_results) if ok
    ]

    working_http_trackers.sort()
    working_udp_trackers.sort()

    final_trackers = working_udp_trackers + working_http_trackers

    with open("trackers.txt", "w", encoding="utf-8") as f:
        for t in final_trackers:
            f.write(t + "\n")

    print(f"Рабочих UDP: {len(working_udp_trackers)}")
    print(f"Рабочих HTTP: {len(working_http_trackers)}")
    print(f"Всего рабочих: {len(final_trackers)}")
    if failed:
        print(f"Не удалось скачать источников: {len(failed)}")

if __name__ == "__main__":
    asyncio.run(main_async())
