import time
import asyncio
from typing import Optional
from urllib.parse import parse_qs, urlsplit

from websockets.exceptions import InvalidURI, ConnectionClosed, InvalidHandshake
from websockets.asyncio.server import Server, ServerConnection, serve

import gsuid_core
from gsuid_core.logger import logger
from gsuid_core.shutdown import shutdown_event

from .config import Settings, load_links
from .models import ObEvent, sid
from .inbound import InboundContext, event_to_receive
from .outbound import OutboundSender
from .onebot_api import OneBotApi, response_data
from .core_bridge import CoreBridge, CoreSocket

_QUEUE_MAX = 1000
_WATCH_INTERVAL = 2.0
_RUNTIME_SLOT = "_onebot_adapter_runtime"
RUNTIME_TAG = "onebot-adapter-runtime"


class LinkRunner:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._supervisor: Optional[asyncio.Task[None]] = None
        self._worker: Optional[asyncio.Task[None]] = None
        self._queue: asyncio.Queue[tuple[OneBotApi, ObEvent]] = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._api: Optional[OneBotApi] = None
        self._apis: dict[str, OneBotApi] = {}
        self._core_socket: Optional[CoreSocket] = None
        self._attach_lock = asyncio.Lock()
        self._bridge = CoreBridge(settings.route_bot_id)
        self._sender = OutboundSender(
            pick_api=self._pick_api,
            push=self._bridge.push,
            settings=settings,
            self_id=lambda: self._self_id,
            nickname=lambda: self._self_nickname,
        )
        self._wake = asyncio.Event()
        self._connected_at: Optional[float] = None
        self._self_id = ""
        self._self_nickname = ""
        self._attempts = 0
        self._last_error = ""
        self._server: Optional[Server] = None

    async def start(self) -> None:
        if self._supervisor is not None and not self._supervisor.done():
            return
        self._worker = asyncio.create_task(
            self._consume_events(),
            name=f"OneBotUID.events.{self.settings.route_bot_id}",
        )
        self._supervisor = asyncio.create_task(
            self._supervise(),
            name=f"OneBotUID.link.{self.settings.route_bot_id}",
        )

    async def stop(self) -> None:
        supervisor = self._supervisor
        self._supervisor = None
        if supervisor is not None and not supervisor.done():
            supervisor.cancel()
        self._close_server()
        if supervisor is not None and not supervisor.done():
            try:
                await supervisor
            except asyncio.CancelledError:
                pass

        worker = self._worker
        self._worker = None
        if worker is not None and not worker.done():
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass

        api = self._api
        self._api = None
        if api is not None:
            await api.close()

        for item in list(self._apis.values()):
            await item.close()
        self._apis.clear()
        self._core_socket = None

        await self._bridge.shutdown()
        logger.info(f"[OneBotUID] 连接 {self.settings.label} 已停止")

    async def force_reconnect(self) -> str:
        self._wake.set()
        if self.settings.is_server:
            apis = list(self._apis.values())
            for item in apis:
                if item.connected:
                    await item.close()
            if apis:
                return f"已断开 {len(apis)} 个协议端连接，等待它们重新接入"
            return f"正在监听 {self.settings.endpoint}，等待协议端接入"

        api = self._api
        if api is not None and api.connected:
            await api.close()
            return "已断开当前连接，正在重连"
        return "已触发重连，等待协议端就绪"

    @property
    def title(self) -> str:
        return self._self_id or self.settings.label

    def describe(self) -> str:
        settings = self.settings
        if self._connected_at is not None:
            state = "已连接"
        else:
            state = "未连接"

        lines = [
            f"状态: {state}",
            f"连接方式: {settings.connection_mode}",
            f"连接标识: {settings.route_bot_id}",
            f"平台标识: {settings.platform_bot_id}",
        ]
        if settings.is_server:
            lines.append(f"已接入协议端: {len(self._apis)} 个")
        if self._connected_at is not None:
            lines.append(f"在线时长: {_format_duration(time.time() - self._connected_at)}")
        lines.append(f"连续失败: {self._attempts} 次")
        if self._last_error:
            lines.append(f"最近错误: {self._last_error}")
        return "\n".join(lines)

    async def _supervise(self) -> None:
        while not shutdown_event.is_set():
            failed = False
            try:
                if self.settings.is_server:
                    await self._run_server()
                else:
                    await self._run_client()
            except asyncio.CancelledError:
                raise
            except (OSError, ConnectionClosed, InvalidHandshake, InvalidURI, TimeoutError) as exc:
                failed = True
                self._last_error = _error_text(exc, self.settings)
                logger.warning(f"[OneBotUID] {self.settings.label} {self._last_error}（{self.settings.endpoint}）")
            except Exception as exc:
                failed = True
                self._last_error = f"运行异常: {exc}"
                logger.exception(f"[OneBotUID] {self.settings.label} 运行时发生异常: {exc}")
            finally:
                self._connected_at = None

            if shutdown_event.is_set():
                break

            if failed:
                self._attempts += 1
                if self.settings.max_reconnect and self._attempts > self.settings.max_reconnect:
                    self._last_error = f"连续 {self._attempts} 次失败，已达上限，停止重连"
                    logger.error(f"[OneBotUID] {self.settings.label} {self._last_error}")
                    break

            await self._sleep(float(self.settings.reconnect_interval))

    async def _run_client(self) -> None:
        api = OneBotApi(token=self.settings.access_token)
        api.bind(lambda event: self._submit(api, event))

        async def on_ready() -> None:
            await self._on_connected(api, self.settings.ws_url)

        async def on_closed() -> None:
            self._release(api, "与协议端的连接已断开")

        try:
            await api.open(self.settings.ws_url)
            await self._attach_core()
            self._api = api
            await api.serve_connection(on_ready, on_closed)
        finally:
            self._release(api, "与协议端的连接已断开")
            await api.close()
            await self._detach_core_if_idle()

    async def _run_server(self) -> None:
        async def handler(conn: ServerConnection) -> None:
            await self._handle_server(conn)

        server = await serve(handler, self.settings.listen_host, self.settings.listen_port, max_size=None)
        self._server = server
        self._last_error = ""
        logger.success(f"[OneBotUID] {self.settings.label} 正在监听 {self.settings.endpoint}，等待协议端接入")
        try:
            await asyncio.Event().wait()
        finally:
            self._server = None
            server.close()

    def _close_server(self) -> None:
        server = self._server
        self._server = None
        if server is not None:
            server.close()

    async def _handle_server(self, conn: ServerConnection) -> None:
        if not _server_authorized(conn, self.settings.access_token):
            logger.warning(f"[OneBotUID] {self.settings.label} WS服务器 连接鉴权失败，已拒绝")
            await conn.close(code=1008, reason="invalid token")
            return

        api = OneBotApi(token=self.settings.access_token)
        api.bind(lambda event: self._submit(api, event))
        api.adopt(conn)
        await self._attach_core()

        async def on_ready() -> None:
            await self._register_server_api(api, f"{self.settings.endpoint} 上接入的协议端")

        async def on_closed() -> None:
            self._release(api, "协议端已断开")

        try:
            await api.serve_connection(on_ready, on_closed)
        except ConnectionClosed:
            pass
        finally:
            self._release(api, "协议端已断开")
            await api.close()
            await self._detach_core_if_idle()

    async def _register_server_api(self, api: OneBotApi, label: str) -> None:
        self_id, nickname = await self._fetch_login_info(api)
        key = self_id or f"conn-{id(api)}"
        previous = self._apis.get(key)
        if previous is not None and previous is not api:
            logger.info(f"[OneBotUID] {self.settings.label} 账号 {key} 重复接入，断开旧连接")
            await previous.close()
        self._apis[key] = api
        if not self._self_id and self_id:
            self._self_id, self._self_nickname = self_id, nickname
        self._mark_online()
        logger.success(
            f"[OneBotUID] {self.settings.label} 已连接 {label}"
            f"（bot_self_id={self_id or '未知'}，当前 {len(self._apis)} 个协议端）"
        )

    def _release(self, api: OneBotApi, message: str) -> None:
        dropped = False
        if self._api is api:
            self._api = None
            dropped = True
        for key, item in list(self._apis.items()):
            if item is api:
                self._apis.pop(key, None)
                dropped = True
        if dropped and self._connected_at is not None:
            logger.warning(f"[OneBotUID] {self.settings.label} {message}")
        if self._api is None and not self._apis:
            self._connected_at = None

    async def _attach_core(self) -> None:
        async with self._attach_lock:
            if self._core_socket is None:
                self._core_socket = await self._bridge.attach(self._sender)

    async def _detach_core_if_idle(self) -> None:
        async with self._attach_lock:
            if self._api is not None or self._apis or self._core_socket is None:
                return
            socket = self._core_socket
            self._core_socket = None
            await self._bridge.detach(socket)

    def find_api(self, self_id: str) -> Optional[OneBotApi]:
        if not self.settings.is_server:
            api = self._api
            if api is None:
                return None
            if self_id and self._self_id and self_id != self._self_id:
                return None
            return api
        if self_id:
            return self._apis.get(self_id)
        if len(self._apis) == 1:
            return next(iter(self._apis.values()))
        return None

    def _pick_api(self, self_id: str) -> Optional[OneBotApi]:
        if not self.settings.is_server:
            return self._api
        if self_id:
            api = self._apis.get(self_id)
            if api is not None:
                return api
        if len(self._apis) == 1:
            return next(iter(self._apis.values()))
        return None

    async def _on_connected(self, api: OneBotApi, label: str) -> None:
        self._self_id, self._self_nickname = await self._fetch_login_info(api)
        self._mark_online()
        logger.success(f"[OneBotUID] {self.settings.label} 已连接 {label}（bot_self_id={self._self_id or '未知'}）")

    def _mark_online(self) -> None:
        self._connected_at = time.time()
        self._attempts = 0
        self._last_error = ""

    async def _fetch_login_info(self, api: OneBotApi) -> tuple[str, str]:
        resp = await api.call("get_login_info", {})
        data = response_data(resp)
        if data is None:
            return "", ""
        nickname = data.get("nickname")
        return sid(data.get("user_id")), nickname if isinstance(nickname, str) else ""

    async def _submit(self, api: OneBotApi, event: ObEvent) -> None:
        try:
            self._queue.put_nowait((api, event))
        except asyncio.QueueFull:
            logger.warning(f"[OneBotUID] {self.settings.label} 事件队列已满，丢弃一条事件")

    async def _consume_events(self) -> None:
        while True:
            api, event = await self._queue.get()
            try:
                await self._handle_event(api, event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(f"[OneBotUID] {self.settings.label} 处理事件失败: {exc}")
            finally:
                self._queue.task_done()

    async def _handle_event(self, api: OneBotApi, event: ObEvent) -> None:
        if not api.connected:
            return

        self._adopt_event_self_id(api, event)

        settings = self.settings
        ctx = InboundContext(
            platform_bot_id=settings.platform_bot_id,
            self_id=self._self_id,
            ignore_self=settings.ignore_self,
            quote_fetch=settings.quote_fetch,
            api=api,
        )
        msg = await event_to_receive(event, ctx)
        if msg is not None:
            await self._bridge.push(msg)

    def _adopt_event_self_id(self, api: OneBotApi, event: ObEvent) -> None:
        if not self.settings.is_server:
            return
        self_id = sid(event.self_id)
        if not self_id or self_id == "0" or self._apis.get(self_id) is api:
            return
        for key, item in list(self._apis.items()):
            if item is api:
                self._apis.pop(key, None)
        self._apis[self_id] = api
        if not self._self_id:
            self._self_id = self_id

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=max(0.5, seconds))
        except TimeoutError:
            pass
        finally:
            self._wake.clear()


class AdapterManager:
    def __init__(self) -> None:
        self._runners: dict[str, LinkRunner] = {}
        self._watcher: Optional[asyncio.Task[None]] = None
        self._runtime_tag = RUNTIME_TAG

    async def start(self) -> None:
        if self._watcher is not None and not self._watcher.done():
            return
        await _stop_previous()
        await self._reconcile()
        self._watcher = asyncio.create_task(self._watch(), name="OneBotUID.manager")
        logger.info("[OneBotUID] 运行时已启动")

    async def stop(self) -> None:
        watcher = self._watcher
        self._watcher = None
        if watcher is not None and not watcher.done():
            watcher.cancel()
            try:
                await watcher
            except asyncio.CancelledError:
                pass

        runners = list(self._runners.values())
        self._runners.clear()
        for runner in runners:
            await runner.stop()
        logger.info("[OneBotUID] 运行时已停止")

    def runner_of(self, route_bot_id: Optional[str]) -> Optional[LinkRunner]:
        if route_bot_id:
            runner = self._runners.get(route_bot_id)
            if runner is not None:
                return runner
        if len(self._runners) == 1:
            return next(iter(self._runners.values()))
        return None

    def find_api(self, self_id: str = "") -> Optional[OneBotApi]:
        """在所有连接里按 self_id 找协议端连接。"""
        if self_id:
            for runner in self._runners.values():
                api = runner.find_api(self_id)
                if api is not None:
                    return api
            return None
        if len(self._runners) == 1:
            return next(iter(self._runners.values())).find_api("")
        return None

    async def force_reconnect(self, route_bot_id: Optional[str] = None) -> str:
        runner = self.runner_of(route_bot_id)
        if runner is None:
            return _no_runner_text(self._runners)
        return f"{runner.title}: {await runner.force_reconnect()}"

    def describe(self, route_bot_id: Optional[str] = None) -> str:
        runner = self.runner_of(route_bot_id)
        if runner is None:
            return _no_runner_text(self._runners)
        return f"—— {runner.title} ——\n{runner.describe()}"

    async def _watch(self) -> None:
        while not shutdown_event.is_set():
            await asyncio.sleep(_WATCH_INTERVAL)
            try:
                await self._reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(f"[OneBotUID] 读取连接列表失败: {exc}")

    async def _reconcile(self) -> None:
        wanted = {settings.route_bot_id: settings for settings in load_links()}

        for route, runner in list(self._runners.items()):
            target = wanted.get(route)
            if target is not None and target == runner.settings:
                continue
            self._runners.pop(route, None)
            await runner.stop()
            if target is not None:
                logger.info(f"[OneBotUID] 连接 {target.label} 配置已变更，重启该连接")

        for route, settings in wanted.items():
            if route in self._runners:
                continue
            runner = LinkRunner(settings)
            self._runners[route] = runner
            await runner.start()
            logger.info(f"[OneBotUID] 连接 {settings.label} 已启动（{settings.endpoint}）")


def _no_runner_text(runners: dict[str, LinkRunner]) -> str:
    if not runners:
        return "未启动任何连接。检查总开关，以及 连接列表 里是否有启用的连接。"
    return "定位不到当前连接，可能刚重载过，稍后再试。"


def _server_authorized(conn: ServerConnection, token: str) -> bool:
    if not token:
        return True
    request = conn.request
    if request is None:
        return False
    if request.headers.get("Authorization") == f"Bearer {token}":
        return True
    values = parse_qs(urlsplit(request.path).query).get("access_token")
    return bool(values) and values[0] == token


def _error_text(exc: BaseException, settings: Settings) -> str:
    if settings.is_server and isinstance(exc, OSError) and exc.errno == 10048:
        return f"监听端口 {settings.listen_port} 被占用，可能上一份实例还没释放，重启核心即可恢复"
    return f"连接失败: {exc}"


def _format_duration(seconds: float) -> str:
    total = int(max(0.0, seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours} 小时 {minutes} 分"
    if minutes:
        return f"{minutes} 分 {secs} 秒"
    return f"{secs} 秒"


adapter_runtime = AdapterManager()


async def _stop_previous() -> None:
    previous = getattr(gsuid_core, _RUNTIME_SLOT, None)
    setattr(gsuid_core, _RUNTIME_SLOT, adapter_runtime)
    if previous is None or previous is adapter_runtime:
        return
    if getattr(previous, "_runtime_tag", "") != RUNTIME_TAG:
        return
    logger.info("[OneBotUID] 检测到重载前遗留的运行时，先停掉再启动")
    await previous.stop()
