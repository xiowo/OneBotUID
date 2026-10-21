import asyncio
from typing import Callable, Optional, Awaitable

from starlette.types import Scope, Message
from starlette.websockets import WebSocket

from gsuid_core.bot import _Bot
from gsuid_core.gss import gss
from gsuid_core.logger import logger
from gsuid_core.models import MessageReceive
from gsuid_core.shutdown import shutdown_event


def _build_scope(bot_id: str) -> Scope:
    path = f"/ws/{bot_id}"
    return {
        "type": "websocket",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "scheme": "ws",
        "server": ("127.0.0.1", 0),
        "client": ("127.0.0.1", 0),
        "root_path": "",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "subprotocols": [],
        "state": {},
    }


class CoreSocket:
    def __init__(self, bot_id: str, on_send: Callable[[bytes], Awaitable[None]]) -> None:
        self._on_send = on_send
        self._closed = asyncio.Event()
        self._handshaked = False
        self.websocket = WebSocket(_build_scope(bot_id), self._receive, self._send)

    async def _receive(self) -> Message:
        if not self._handshaked:
            self._handshaked = True
            return {"type": "websocket.connect"}
        await self._closed.wait()
        return {"type": "websocket.disconnect", "code": 1000}

    async def _send(self, message: Message) -> None:
        if message["type"] != "websocket.send":
            return
        payload = message.get("bytes")
        if isinstance(payload, (bytes, bytearray)):
            await self._on_send(bytes(payload))
            return
        text = message.get("text")
        if isinstance(text, str):
            await self._on_send(text.encode())

    def shutdown(self) -> None:
        self._closed.set()


class CoreBridge:
    def __init__(self, route_bot_id: str) -> None:
        from gsuid_core.handler import _INBOUND_SLOTS

        self.route_bot_id = route_bot_id
        self._slots = asyncio.Semaphore(_INBOUND_SLOTS)
        self._bot: Optional[_Bot] = None
        self._socket: Optional[CoreSocket] = None
        self._process: Optional[asyncio.Task[None]] = None
        self._process_bot: Optional[_Bot] = None

    async def attach(self, sender: Callable[[bytes], Awaitable[None]]) -> CoreSocket:
        socket = CoreSocket(self.route_bot_id, sender)
        self._bot = await gss.connect(socket.websocket, self.route_bot_id)
        self._socket = socket
        self._ensure_process(self._bot)
        logger.info(f"[OneBotUID] 已注册到核心，连接标识 {self.route_bot_id}")
        return socket

    async def detach(self, socket: CoreSocket) -> None:
        if self._socket is not socket:
            return
        self._socket = None
        socket.shutdown()
        await gss.disconnect(self.route_bot_id, socket.websocket)
        logger.info("[OneBotUID] 已从核心注销该连接")

    async def shutdown(self) -> None:
        socket = self._socket
        if socket is not None:
            await self.detach(socket)
        process = self._process
        self._process = None
        self._process_bot = None
        if process is not None and not process.done():
            process.cancel()
            try:
                await process
            except asyncio.CancelledError:
                pass
        self._bot = None

    async def push(self, msg: MessageReceive) -> None:
        bot = self._bot
        if bot is None:
            return
        if bot.resolve_recall(msg):
            return

        from gsuid_core.handler import run_inbound_event

        bot._add_bg_task(asyncio.create_task(run_inbound_event(bot, msg, self._slots)))

    def _ensure_process(self, bot: _Bot) -> None:
        if self._process is not None and not self._process.done() and self._process_bot is bot:
            return
        if self._process is not None and not self._process.done():
            self._process.cancel()
        self._process_bot = bot
        self._process = asyncio.create_task(
            bot._process(shutdown_event),
            name=f"OneBotUID.process.{bot.bot_id}",
        )
