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

TRACKER_RE = re.compile(
    r"^(udp|http|https)://S+$",
    re.IGNORECASE,
)

PRIVATE_RE = re.compile(
    r"(?:passkey|authkey|token|api_key|apikey|secret)=",
    re.IGNORECASE,
)

DOWNLOAD_LINK_RE = re.compile(
    r"https?://S+.(?:txt|list)",
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
    "https://torrends.to/torrent-tracker-list/?download=latest"
]

trackers = set()
failed = []


def fetch(url):
    request = urllib.request.Request(url, headers=HEADERS)

    with urllib.request.urlopen(request, timeout=25) as response:
        data = response.read()

    encoding = (response.headers.get("Content-Encoding") or "").lower()

    if encoding == "gzip" or data[:2] == b"\u001F":
        try:
            data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
        except OSError:
            pass

    charset = response.headers.get_content_charset() or "utf-8"
    return data.decode(charset, errors="ignore")


def extract_trackers(text):
    found = set()

    for tracker in TRACKER_RE.findall(text):
        tracker = tracker.strip().rstrip(".,;:)")

        if not PRIVATE_RE.search(tracker):
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

        if depth == 0 and ("<a " in text.lower() or "html" in text.lower()):
            links = set()

            for link in DOWNLOAD_LINK_RE.findall(text):
                absolute = urljoin(url, link)

                if urlparse(absolute).netloc:
                    links.add(absolute)

            for link in list(links)[:10]:
                process_url(link, depth=1)

    except Exception as error:
        failed.append(url + " - " + str(error))
        print("FAILED: " + url + ": " + str(error))


for url in urls:
    process_url(url)

with open("trackers.txt", "w", encoding="utf-8") as file:
    file.write("".join(sorted(trackers)) + "")

with open("failed-sources.txt", "w", encoding="utf-8") as file:
    file.write("".join(failed) + "")

print("Saved " + str(len(trackers)) + " trackers")
print("Failed sources: " + str(len(failed)))

with open("failed-sources.txt", "w", encoding="utf-8") as file:
    file.write("".join(failed) + "")

print("Saved " + str(len(trackers)) + " trackers")
print("Failed sources: " + str(len(failed)))
