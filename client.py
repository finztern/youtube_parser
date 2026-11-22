"""youtubei/v1/next client (curl_cffi through a local SOCKS port)."""
from __future__ import annotations

from curl_cffi.requests import AsyncSession

HEADERS = {"Accept-Language": "en-US,en;q=0.9", "Cookie": "SOCS=CAI; CONSENT=PENDING+987"}
WEB_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"  # public key of the web client
WEB_VERSION = "2.20241120.01.00"
NEXT_URL = f"https://www.youtube.com/youtubei/v1/next?key={WEB_KEY}&prettyPrint=false"
POST_HEADERS = {**HEADERS, "Origin": "https://www.youtube.com", "X-Youtube-Client-Name": "1", "X-Youtube-Client-Version": WEB_VERSION}
CLIENT = {"clientName": "WEB", "clientVersion": WEB_VERSION, "hl": "en", "gl": "US"}


class YtError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"http {status}")
        self.status = status


def new_session(proxy: str | None, timeout: float = 20) -> AsyncSession:
    proxies = {"http": proxy, "https": proxy} if proxy else None
    return AsyncSession(impersonate="chrome", timeout=timeout, proxies=proxies)


async def post_next(session: AsyncSession, payload: dict) -> tuple[str, int]:
    """(response text, size in bytes). Raises YtError on a non-200 status."""
    r = await session.post(NEXT_URL, json={"context": {"client": CLIENT}, **payload}, headers=POST_HEADERS)
    if r.status_code != 200:
        raise YtError(r.status_code)
    return r.text, len(r.content)
