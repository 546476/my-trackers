import asyncio
import aiohttp
import random
import struct
from urllib.parse import urlparse

HTTP_CONCURRENCY = 110
HTTP_LIMIT_PER_HOST = 8
HTTP_TIMEOUT = 10

UDP_CONCURRENCY = 100
UDP_TIMEOUT = 10
UDP_ATTEMPTS = 3

MAGIC_PROTOCOL_ID = 0x41727101980
ACTION_CONNECT = 0
ACTION_SCRAPE = 2
ACTION_ERROR = 3

FAKE_INFO_HASH = b"" * 20
FAKE_PEER_ID = b"-qB0000-" + b"0" * 12

BENCODE_SIGNS = ("interval", "peers")


async def check_http_tracker(
    session: aiohttp.ClientSession,
    tracker_url: str,
) -> bool:
    try:
        params = {
            "info_hash": FAKE_INFO_HASH,
            "peer_id": FAKE_PEER_ID,
            "port": 6881,
            "uploaded": 0,
            "downloaded": 0,
            "left": 0,
            "event": "started","compact": 1,
    }
        async with session.get(
            tracker_url, params=params,
            
            timeout=aiohttp.ClientTimeout(
                total=HTTP_TIMEOUT,
                connect=5,
                sock_connect=5,
                sock_read=5,
            ),
            allow_redirects=True,
        ) as response:
            if response.status < 200 or response.status >= 300:
                return False

            content = await response.read()
            stripped = content.strip()
            lower_content = stripped.lower()

            if b"failure reason" in lower_content:
                return False

            if not (stripped.startswith(b"d") and stripped.endswith(b"e")):
                return False

            if b"interval" not in lower_content:
                return False

            if b"peers" not in lower_content:
                return False

            return True

    except Exception:
        return False


class UDPTrackerProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport = None
        self.response = None
        self.response_future = None

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        self.response = data

        if self.response_future and not self.response_future.done():
            self.response_future.set_result(data)

    def error_received(self, exc: Exception) -> None:
        if self.response_future and not self.response_future.done():
            self.response_future.set_exception(exc)

    async def wait_response(self) -> bytes:
        loop = asyncio.get_running_loop()
        self.response_future = loop.create_future()
        return await self.response_future


def parse_udp_tracker(url: str) -> tuple[str, int]:
    parsed = urlparse(url)

    if parsed.scheme.lower() != "udp":
        raise ValueError("Not a UDP tracker URL")

    host = parsed.hostname
    port = parsed.port

    if not host or not port:
        raise ValueError("Invalid UDP tracker URL")

    return host, port


def create_connect_request() -> tuple[bytes, int]:
    transaction_id = random.getrandbits(32)

    request = struct.pack(
        ">QII",
        MAGIC_PROTOCOL_ID,
        ACTION_CONNECT,
        transaction_id,
    )

    return request, transaction_id


def parse_connect_response(
    response: bytes,
    transaction_id: int,
) -> int:
    if len(response) < 16:
        raise ValueError("Connect response is too short")

    action, received_transaction_id, connection_id = struct.unpack(
        ">IIQ",
        response[:16],
    )

    if action != ACTION_CONNECT:
        raise ValueError("Unexpected connect response action")

    if received_transaction_id != transaction_id:
        raise ValueError("Connect transaction ID mismatch")

    return connection_id


def create_scrape_request(connection_id: int) -> tuple[bytes, int]:
    transaction_id = random.getrandbits(32)

    request = struct.pack(
        ">QII",
        connection_id,
        ACTION_SCRAPE,
        transaction_id,
    ) + FAKE_INFO_HASH

    return request, transaction_id


def parse_scrape_response(
    response: bytes,
    transaction_id: int,
) -> bool:
    if len(response) < 8:
        raise ValueError("Scrape response is too short")

    action, received_transaction_id = struct.unpack(
        ">II",
        response[:8],
    )

    if action == ACTION_ERROR:
        return False

    if action != ACTION_SCRAPE:
        raise ValueError("Unexpected scrape response action")

    if received_transaction_id != transaction_id:
        raise ValueError("Scrape transaction ID mismatch")

    return True


async def check_udp_tracker_once(
    tracker_url: str,
    timeout: float,
) -> bool:
    try:
        host, port = parse_udp_tracker(tracker_url)
    except Exception:
        return False

    loop = asyncio.get_running_loop()

    try:
        transport, protocol = await asyncio.wait_for(
            loop.create_datagram_endpoint(
                lambda: UDPTrackerProtocol(),
                remote_addr=(host, port),
            ),
            timeout=timeout,
        )
    except Exception:
        return False

    try:
        connect_request, connect_transaction_id = create_connect_request()

        transport.sendto(connect_request)

        try:
            connect_response = await asyncio.wait_for(
                protocol.wait_response(),
                timeout=timeout,
            )
        except Exception:
            return False

        try:
            connection_id = parse_connect_response(
                connect_response,
                connect_transaction_id,
            )
        except Exception:
            return False

        scrape_request, scrape_transaction_id = create_scrape_request(
            connection_id,
        )

        transport.sendto(scrape_request)

        try:
            scrape_response = await asyncio.wait_for(
                protocol.wait_response(),
                timeout=timeout,
            )
        except Exception:
            return False

        try:
            return parse_scrape_response(
                scrape_response,
                scrape_transaction_id,
            )
        except Exception:
            return False

    finally:
        if transport and not transport.is_closing():
            transport.close()


async def check_udp_tracker(
    tracker_url: str,
    timeout: float,
    attempts: int,
) -> bool:
    for _ in range(attempts):
        if await check_udp_tracker_once(tracker_url, timeout):
            return True

    return False


async def main() -> None:
    reasons = {
        "status": 0,
        "no_bencode": 0,
        "failure_reason": 0,
        "no_interval": 0,
        "no_peers": 0,
        "exception": 0,
    }

    with open("trackers.txt", "r", encoding="utf-8") as file:
        lines = file.readlines()

    trackers = [
        line.strip()
        for line in lines
        if line.strip() and not line.strip().startswith("#")
    ]

    udp_trackers = [
        url
        for url in trackers
        if url.lower().startswith("udp://")
    ]

    http_trackers = [
        url
        for url in trackers
        if not url.lower().startswith("udp://")
    ]

    http_semaphore = asyncio.Semaphore(HTTP_CONCURRENCY)
    udp_semaphore = asyncio.Semaphore(UDP_CONCURRENCY)

    connector = aiohttp.TCPConnector(
        limit=HTTP_CONCURRENCY,
        limit_per_host=HTTP_LIMIT_PER_HOST,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; tracker-checker)"
        },
    ) as session:
        async def check_http_with_limit(url: str) -> bool:
            async with http_semaphore:
                return await check_http_tracker(session, url)

        async def check_udp_with_limit(url: str) -> bool:
            async with udp_semaphore:
                return await check_udp_tracker(
                    url,
                    UDP_TIMEOUT,
                    UDP_ATTEMPTS,
                )

        http_tasks = [
            check_http_with_limit(url)
            for url in http_trackers
        ]

        udp_tasks = [
            check_udp_with_limit(url)
            for url in udp_trackers
        ]

        http_results, udp_results = await asyncio.gather(
            asyncio.gather(*http_tasks),
            asyncio.gather(*udp_tasks),
        )

    working_http_trackers = [
        url
        for url, is_working in zip(http_trackers, http_results)
        if is_working
    ]

    working_udp_trackers = [
        url
        for url, is_working in zip(udp_trackers, udp_results)
        if is_working
    ]

    working_http_trackers.sort()
    working_udp_trackers.sort()

    final_trackers = working_udp_trackers + working_http_trackers
    
    print("HTTP rejection reasons:")
    for reason, count in reasons.items():
        print(f"{reason}: {count}")

    with open("trackers.txt", "w", encoding="utf-8", newline="\n") as file:
      file.write("\n".join(sorted(trackers)))
      if trackers:
          file.write("\n")
          
    print("HTTP trackers checked:", len(http_trackers))
    print("HTTP trackers passed:", len(working_http_trackers))
    print("HTTP trackers removed:", len(http_trackers) - len(working_http_trackers))

    print("UDP trackers checked:", len(udp_trackers))
    print("UDP trackers passed:", len(working_udp_trackers))
    print("UDP trackers removed:", len(udp_trackers) - len(working_udp_trackers))

    print("Total saved:", len(final_trackers))


if __name__ == "__main__":
    asyncio.run(main())
