import asyncio
from typing import Callable, Awaitable

import msgspec
from websockets.exceptions import ConnectionClosed
from websockets.asyncio.client import connect
from websockets.asyncio.connection import Connection

from gsuid_core.logger import logger

from .models import ObEvent

msgjson = msgspec.json

EventCallback = Callable[[ObEvent], Awaitable[None]]
Hook = Callable[[], Awaitable[None]]

_OPEN_TIMEOUT = 15.0
_CALL_TIMEOUT = 30.0
_ECHO_PREFIX = "gscore-"
_PING_INTERVAL = 20


class OneBotApi:
    def __init__(self, on_event: EventCallback, token: str = "") -> None:
        self._on_event = on_event
        self._token = token
        self._conn: Connection | None = None
        self._pending: dict[str, asyncio.Future[dict[str, object]]] = {}
        self._write_lock = asyncio.Lock()
        self._seq = 0

    @property
    def connected(self) -> bool:
        return self._conn is not None

    async def open(self, url: str) -> None:
        headers = {"Authorization": f"Bearer {self._token}"} if self._token else None
        self._conn = await connect(
            url,
            additional_headers=headers,
            proxy=None,
            ping_interval=_PING_INTERVAL,
            ping_timeout=_PING_INTERVAL,
            open_timeout=_OPEN_TIMEOUT,
            max_size=None,
        )

    def adopt(self, conn: Connection) -> None:
        self._conn = conn

    async def serve_connection(self, on_ready: Hook, on_closed: Hook) -> None:
        conn = self._conn
        if conn is None:
            raise RuntimeError("OneBotApi 尚未绑定连接")

        reader = asyncio.create_task(self._read_loop(conn), name="OneBotUID.reader")
        try:
            await on_ready()
            await reader
        finally:
            if not reader.done():
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            self._conn = None
            self._fail_pending()
            await on_closed()

    async def close(self) -> None:
        conn = self._conn
        if conn is not None:
            await conn.close()

    async def call(
        self,
        action: str,
        params: dict[str, object] | None = None,
        timeout: float = _CALL_TIMEOUT,
    ) -> dict[str, object] | None:
        conn = self._conn
        if conn is None:
            return None

        self._seq += 1
        echo = f"{_ECHO_PREFIX}{self._seq}"
        future: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()
        self._pending[echo] = future
        frame = msgjson.encode({"action": action, "params": params or {}, "echo": echo})
        try:
            async with self._write_lock:
                await conn.send(frame)
            return await asyncio.wait_for(future, timeout)
        except TimeoutError:
            logger.warning(f"[OneBotUID] 接口 {action} 超时未返回")
            return None
        except ConnectionClosed as exc:
            logger.warning(f"[OneBotUID] 接口 {action} 发送失败: {exc}")
            return None
        except OSError as exc:
            logger.warning(f"[OneBotUID] 接口 {action} 网络异常: {exc}")
            return None
        finally:
            self._pending.pop(echo, None)
            if not future.done():
                future.cancel()

    async def _read_loop(self, conn: Connection) -> None:
        async for raw in conn:
            await self._handle_frame(raw)

    async def _handle_frame(self, raw: str | bytes) -> None:
        try:
            payload = msgjson.decode(raw)
        except msgspec.DecodeError as exc:
            logger.warning(f"[OneBotUID] 无法解析的帧，已跳过: {exc}")
            return

        if not isinstance(payload, dict):
            return

        if isinstance(payload.get("post_type"), str):
            await self._dispatch_event(payload)
            return

        echo = payload.get("echo")
        if echo is not None:
            self._resolve(str(echo), payload)

    async def _dispatch_event(self, payload: dict[str, object]) -> None:
        try:
            event = msgspec.convert(payload, ObEvent)
        except msgspec.ValidationError as exc:
            logger.warning(f"[OneBotUID] 事件字段不符合预期，已跳过: {exc}")
            return
        await self._on_event(event)

    def _resolve(self, echo: str, payload: dict[str, object]) -> None:
        future = self._pending.pop(echo, None)
        if future is None or future.done():
            return
        future.set_result(payload)

    def _fail_pending(self) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        for future in pending:
            if not future.done():
                future.set_result({})


def response_data(resp: dict[str, object] | None) -> dict[str, object] | None:
    if resp is None:
        return None
    data = resp.get("data")
    return data if isinstance(data, dict) else None


def response_failed(resp: dict[str, object] | None) -> bool:
    if resp is None:
        return True
    retcode = resp.get("retcode")
    if isinstance(retcode, int) and retcode not in (0, 1):
        message = resp.get("wording") or resp.get("message") or ""
        logger.warning(f"[OneBotUID] 接口返回失败 retcode={retcode} {message}")
        return True
    return False
