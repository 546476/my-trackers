import asyncio
import re
import socket
import struct
from urllib.parse import urlparse

import aiohttp

OUTPUT_FILE = "trackers.txt"

HTTP_CHECK_TIMEOUT = 10
HTTP_CONNECT_TIMEOUT = 5
HTTP_READ_TIMEOUT = 5
UDP_CHECK_TIMEOUT = 10

MAX_HTTP_CONCURRENT_CHECKS = 150
MAX_UDP_CONCURRENT_CHECKS = 150

async def check_http_tracker(session, semaphore, tracker):
    async with semaphore:
        for attempt in range(2):
            try:
                async with session.get(
                    tracker,
                    timeout=aiohttp.ClientTimeout(
                        total=HTTP_CHECK_TIMEOUT,
                        connect=HTTP_CONNECT_TIMEOUT,
                        sock_read=HTTP_READ_TIMEOUT,
                        ),
                        allow_redirects=True,
                ) as response:
                    if 200 <= response.status < 300:
                        return True

                    if response.status in (429, 500, 502, 503, 504):
                        await asyncio.sleep(1)
                        continue

                    return False

            except Exception:
                if attempt == 0:
                    await asyncio.sleep(1)
                    continue

                return False

        return False

class UDPTrackerProtocol(asyncio.DatagramProtocol):
    def __init__(self, future):
        self.future = future

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if not self.future.done():
            self.future.set_result(data)

    def error_received(self, exc):
        if not self.future.done():
            self.future.set_exception(exc)

async def check_udp_tracker(semaphore, tracker):
    async with semaphore:
        for attempt in range(2):
            parsed = urlparse(tracker)
            host = parsed.hostname
            port = parsed.port or 80

            try:
                loop = asyncio.get_running_loop()
                address = await loop.getaddrinfo(
                    host,
                    port,
                    type=socket.SOCK_DGRAM,
                )
                _, _, _, _, target = address[0]

                future = loop.create_future()
                transport, protocol = await loop.create_datagram_endpoint(
                    lambda: UDPTrackerProtocol(future),
                    remote_addr=target,
                )

                try:
                    transaction_id = 0x12345678
                    request = struct.pack(
                        "!QII",
                        0x41727101980,
                        0,
                        transaction_id,
                    )

                    transport.sendto(request)
                    response = await asyncio.wait_for(future, UDP_CHECK_TIMEOUT)

                    if len(response) < 16:
                        continue

                    action, received_transaction_id = struct.unpack(
                        "!II",
                        response[:8],
                    )

                    if (
                        action == 0
                        and received_transaction_id == transaction_id
                    ):
                        return True

                finally:
                    transport.close()

            except Exception:
                if attempt == 0:
                    await asyncio.sleep(1)
                    continue

                return False

        return False


async def main():
    trackers = set()
    failed = []
    
    http_semaphore = asyncio.Semaphore(MAX_HTTP_CONCURRENT_CHECKS)
    udp_semaphore = asyncio.Semaphore(MAX_UDP_CONCURRENT_CHECKS)

    with open("trackers.txt", "r", encoding="utf-8") as file:
        trackers = {
            line.strip()
            for line in file
            if line.strip()
        }

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
        connector=aiohttp.TCPConnector(limit=MAX_HTTP_CONCURRENT_CHECKS)
    ) as session:
        http_tasks = [
            check_http_tracker(session, http_semaphore, tracker)
            for tracker in http_trackers
        ]

        udp_tasks = [
            check_udp_tracker(udp_semaphore, tracker)
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
