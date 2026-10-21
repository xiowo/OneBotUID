from copy import deepcopy
from dataclasses import dataclass

from gsuid_core.logger import logger
from gsuid_core.data_store import get_res_path
from gsuid_core.utils.plugins_config.models import (
    GSC,
    GsConfig,
    GsIntConfig,
    GsStrConfig,
    GsBoolConfig,
    GsRepeatGroupConfig,
)
from gsuid_core.utils.plugins_config.gs_config import StringConfig

LINK_TEMPLATE: dict[str, GSC] = {
    "name": GsStrConfig(
        title="名称",
        desc="连接列表里显示的名字，留空时用连接标识。",
        data="",
    ),
    "enable": GsBoolConfig(
        title="启用连接",
        desc="关闭后这条连接不再拨号 / 监听，其他连接不受影响。",
        data=True,
    ),
    "connection_mode": GsStrConfig(
        title="连接方式",
        desc="WS客户端：插件去连协议端的 WS 服务端；WS服务器：插件自己开 WS 服务端等协议端连进来。",
        data="WS客户端",
        options=["WS客户端", "WS服务器"],
    ),
    "ws_url": GsStrConfig(
        title="协议端 WS 地址",
        desc="WS客户端 模式下要连的地址，例如 ws://127.0.0.1:3001。",
        data="ws://127.0.0.1:3001",
    ),
    "listen_host": GsStrConfig(
        title="监听地址",
        desc="WS服务器 模式下监听哪个网卡，0.0.0.0 表示所有网卡。",
        data="0.0.0.0",
    ),
    "listen_port": GsIntConfig(
        title="监听端口",
        desc="WS服务器 模式下监听的端口，协议端那边填 ws://<本机IP>:<这个端口>。",
        data=3002,
        max_value=65535,
    ),
    "access_token": GsStrConfig(
        title="Access Token",
        desc="WS客户端 发 Bearer 头；WS服务器 校验协议端带来的 token。留空表示不鉴权。",
        data="",
        secret=True,
    ),
    "route_bot_id": GsStrConfig(
        title="连接标识",
        desc="这条连接在核心内的名字（WS_BOT_ID），多条连接必须互不相同。",
        data="OneBot",
    ),
    "platform_bot_id": GsStrConfig(
        title="平台标识",
        desc="上报消息时携带的 bot_id，插件与数据库按它区分平台。",
        data="onebot",
    ),
    "ignore_self": GsBoolConfig(
        title="忽略自身消息",
        desc="协议端会把机器人自己发的消息也上报，开启后不再转发给核心。",
        data=True,
    ),
}

CONFIG_DEFAULT: dict[str, GSC] = {
    "enable": GsBoolConfig(
        title="启用适配器",
        desc="总开关，关闭后所有连接都不启动。",
        data=True,
    ),
    "reconnect_interval": GsIntConfig(
        title="重连间隔",
        desc="断线后等待多少秒重连。",
        data=5,
    ),
    "max_reconnect": GsIntConfig(
        title="最大重连次数",
        desc="0 表示一直重连。",
        data=0,
    ),
    "quote_fetch": GsBoolConfig(
        title="回查引用正文",
        desc="收到引用回复时调用 get_msg 取被引用消息的内容，关闭可省一次接口调用。",
        data=True,
    ),
    "custom_image_summary": GsStrConfig(
        title="图片外显",
        desc="发送图片时的外显文案，多个用英文逗号分隔随机取一个，留空用默认 [图片]。",
        data="",
    ),
    "node_qq": GsStrConfig(
        title="合并转发 QQ",
        desc="合并转发节点上显示的发件人 QQ，留空用机器人自身。",
        data="",
    ),
    "node_name": GsStrConfig(
        title="合并转发昵称",
        desc="合并转发节点上显示的发件人昵称，留空用机器人自身。",
        data="",
    ),
    "links": GsRepeatGroupConfig(
        title="连接列表",
        desc="每条连接对应一个协议端。点击新增一项后填写方向、地址、连接标识，"
        "并可单独控制该连接是否启用。连接标识在多条连接之间不能重复。",
        data=[deepcopy(LINK_TEMPLATE)],
        template=LINK_TEMPLATE,
    ),
}

CONFIG_PATH = get_res_path("OneBotUID") / "config.json"

onebot_config = StringConfig("OneBotUID", CONFIG_PATH, CONFIG_DEFAULT)

MODE_CLIENT = "WS客户端"
MODE_SERVER = "WS服务器"


@dataclass(frozen=True)
class Settings:
    name: str
    enable: bool
    connection_mode: str
    ws_url: str
    listen_host: str
    listen_port: int
    access_token: str
    route_bot_id: str
    platform_bot_id: str
    ignore_self: bool
    reconnect_interval: int
    max_reconnect: int
    quote_fetch: bool
    custom_image_summary: str
    node_qq: str
    node_name: str

    @property
    def is_server(self) -> bool:
        return self.connection_mode == MODE_SERVER

    @property
    def endpoint(self) -> str:
        if self.is_server:
            return f"ws://{self.listen_host}:{self.listen_port}"
        return self.ws_url

    @property
    def label(self) -> str:
        return self.name or self.route_bot_id


def load_links() -> list[Settings]:
    if not bool(onebot_config.get_config("enable").data):
        return []

    raw = onebot_config.get_config("links").data or []
    result: list[Settings] = []
    seen: set[str] = set()
    for row in raw:
        settings = _build_settings(row)
        if not settings.enable:
            continue
        if settings.route_bot_id in seen:
            logger.warning(f"[OneBotUID] 连接标识 {settings.route_bot_id} 重复，已忽略后一条")
            continue
        seen.add(settings.route_bot_id)
        result.append(settings)
    return result


def _build_settings(row: dict[str, GSC]) -> Settings:
    route_bot_id = str(_pick(row, "route_bot_id")).strip() or "OneBot"
    return Settings(
        name=str(_pick(row, "name")).strip(),
        enable=bool(_pick(row, "enable")),
        connection_mode=str(_pick(row, "connection_mode")).strip() or MODE_CLIENT,
        ws_url=str(_pick(row, "ws_url")).strip(),
        listen_host=str(_pick(row, "listen_host")).strip() or "0.0.0.0",
        listen_port=_as_int(_pick(row, "listen_port"), 3002),
        access_token=str(_pick(row, "access_token")).strip(),
        route_bot_id=route_bot_id,
        platform_bot_id=str(_pick(row, "platform_bot_id")).strip() or "onebot",
        ignore_self=bool(_pick(row, "ignore_self")),
        reconnect_interval=max(1, _as_int(_global("reconnect_interval"), 5)),
        max_reconnect=max(0, _as_int(_global("max_reconnect"), 0)),
        quote_fetch=bool(_global("quote_fetch")),
        custom_image_summary=str(_global("custom_image_summary") or ""),
        node_qq=str(_global("node_qq") or "").strip(),
        node_name=str(_global("node_name") or "").strip(),
    )


def _pick(row: dict[str, GSC], key: str) -> object:
    item = row.get(key)
    if isinstance(item, GsConfig):
        return item.data
    return LINK_TEMPLATE[key].data


def _global(name: str) -> object:
    item = onebot_config.config.get(name)
    if isinstance(item, GsConfig):
        return item.data
    return None


def _as_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return default
