import gzip
import io
import json
import re
import urllib.request
from urllib.parse import urljoin, urlparse

HEADERS = {
    "User-Agent": "qBittorrentTrackerUpdater/1.0 (+https://github.com/546476/my-trackers)",
    "Accept": "text/plain, text/html, application/json, */*",
    "Accept-Encoding": "gzip",
}

# ВАЖНО: [^\s] (один обратный слеш), а не [^\\s]
TRACKER_RE = re.compile(r"(?:udp|http|https)://[^\s\"'<>]+", re.IGNORECASE)

PRIVATE_RE = re.compile(
    r"(?:passkey|authkey|token|api_key|apikey|secret)=",
    re.IGNORECASE,
)

# \S+ и экранированная точка
DOWNLOAD_LINK_RE = re.compile(
    r"https?://[^\s\"'<>]+\.(?:txt|list)(?:\?[^\s\"'<>]*)?",
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
    "https://tracker.adysec.com/trackers_best.txt",
    "https://gist.githubusercontent.com/dujiulun/e90ae9e835d8e41ddc63/raw/e78883869a308395a1bf5d7296d65a3893a5bae6/BitTorrent-Trackers.txt",
    "https://torrends.to/torrent-tracker-list/?download=latest",
    "https://newtrackon.com/api/stable",
    "https://raw.githubusercontent.com/jhhd88/trackerslist/refs/heads/master/trackerslist.txt",
    "https://raw.githubusercontent.com/kkkgo/all-tracker-list/refs/heads/main/trackerslist.txt",
    "https://tinytorrent.net/best-torrent-tracker-list-updated/",
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt",
    "https://raw.githubusercontent.com/kkkgo/all-tracker-list/main/trackerslist.txt",
    "https://raw.githubusercontent.com/cdryzun/trackers/main/trackers.txt",
    "https://raw.githubusercontent.com/cdryzun/trackers/main/trackers_v6.txt",
    "https://raw.githubusercontent.com/adysec/tracker/main/trackers_best.txt",
    "https://newtrackon.com/list",
    "https://raw.githubusercontent.com/scriptzteam/BitTorrent-Tracker-List/refs/heads/main/trackers_best.txt",
    "",
    "",
    "",
    "",
    "",
    
    "https://torrends.to/torrent-tracker-list/?download=latest",
    "https://gist.githubusercontent.com/dujiulun/e90ae9e835d8e41ddc63/raw/e78883869a308395a1bf5d7296d65a3893a5bae6/BitTorrent-Trackers.txt",
    "https://tracker.adysec.com/trackers_best.txt"
]

trackers = set()
failed = []

TRAILING_JUNK = ".,;:)]}\"'"

def fetch(url):
    request = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=25) as response:
        data = response.read()
        content_encoding = (response.headers.get("Content-Encoding") or "").lower()
        charset = response.headers.get_content_charset() or "utf-8"

    if content_encoding == "gzip" or data[:2] == b"\x1f\x8b":
        try:
            data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
        except OSError:
            pass

    return data.decode(charset, errors="ignore")

def normalize(tracker):
    tracker = tracker.strip().strip(TRAILING_JUNK)
    if not tracker:
        return None
    if any(ch.isspace() for ch in tracker):
        return None
    if not tracker.lower().startswith(("udp://", "http://", "https://")):
        return None
    parsed = urlparse(tracker)

    if tracker.lower().endswith((".txt", ".list")) or any(
        domain in parsed.netloc.lower()
        for domain in ("github.com", "jsdelivr.net", "ngosang.github.io")
    ):
        return None
    if PRIVATE_RE.search(tracker):
        return None
    host = tracker.split("://", 1)[1]
    if not host:
        return None
    return tracker

def extract_trackers(text):
    found = set()
    for candidate in TRACKER_RE.findall(text):
        tracker = normalize(candidate)
        if tracker:
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

        if depth == 0 and stripped.startswith("<"):
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

for url in urls:
    process_url(url)

with open("trackers.txt", "w", encoding="utf-8", newline="\n") as file:
    file.write("\n".join(sorted(trackers)))
    if trackers:
        file.write("\n")

with open("failed-sources.txt", "w", encoding="utf-8", newline="\n") as file:
    file.write("\n".join(failed))

print(f"Saved {len(trackers)} trackers")
print(f"Failed sources: {len(failed)}")
