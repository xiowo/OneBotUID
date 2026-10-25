import os
import json
from copy import deepcopy
from typing import Optional
from dataclasses import dataclass

from gsuid_core.logger import logger
from gsuid_core.data_store import get_res_path
from gsuid_core.utils.plugins_config.models import (
    GSC,
    GsConfig,
    GsDivider,
    GsIntConfig,
    GsStrConfig,
    GsBoolConfig,
    GsRepeatGroupConfig,
)
from gsuid_core.utils.plugins_config.gs_config import StringConfig

MODE_CLIENT = "WS客户端"
MODE_SERVER = "WS服务器"


DEFAULT_ROUTE_BOT_ID = "OneBot"


def _head_fields() -> dict[str, GSC]:
    """两种连接共用的前置字段：前端拿 enable 渲染卡片上的开关，拿 name 渲染卡片标题。"""
    return {
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
    }


def _tail_fields() -> dict[str, GSC]:
    """两种连接共用的尾部字段。"""
    return {
        "access_token": GsStrConfig(
            title="Access Token",
            desc="WS客户端 发 Bearer 头；WS服务器 校验协议端带来的 token。留空表示不鉴权。",
            data="",
            secret=True,
        ),
        "route_bot_id": GsStrConfig(
            title="连接标识",
            desc="这条连接在核心内的名字（WS_BOT_ID）。默认 OneBot，一般不用改；",
            data=DEFAULT_ROUTE_BOT_ID,
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


CLIENT_TEMPLATE: dict[str, GSC] = {
    **_head_fields(),
    "ws_url": GsStrConfig(
        title="协议端地址",
        desc="插件主动去连的地址，例如 ws://127.0.0.1:3001。",
        data="ws://127.0.0.1:3001",
    ),
    **_tail_fields(),
}

SERVER_TEMPLATE: dict[str, GSC] = {
    **_head_fields(),
    "listen_host": GsStrConfig(
        title="监听地址",
        desc="监听哪个网卡，0.0.0.0 表示所有网卡；核心在 Docker 里时留 0.0.0.0 并映射端口。",
        data="0.0.0.0",
    ),
    "listen_port": GsIntConfig(
        title="监听端口",
        desc="协议端那边填 ws://<本机IP>:<这个端口>。一个端口可以同时接多个协议端，靠各自的 bot_self_id 区分。",
        data=3002,
        max_value=65535,
    ),
    **_tail_fields(),
}

CONFIG_DEFAULT: dict[str, GSC] = {
    "_DividerRuntime": GsDivider("运行控制", ""),
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
    "_DividerMessage": GsDivider("消息处理", ""),
    "quote_fetch": GsBoolConfig(
        title="回查引用正文",
        desc="收到引用回复时调用 get_msg 取被引用消息的内容，关闭可省一次接口调用。",
        data=True,
    ),
    "_DividerClient": GsDivider("WS客户端连接", ""),
    "client_links": GsRepeatGroupConfig(
        title="连接列表",
        desc="插件主动去连协议端的 WS 服务端。每条对应一个协议端，填对方的地址即可；"
        "懒得迁移数据可以直接改成你原来的WSID，否则不需要配置连接标识。",
        data=[deepcopy(CLIENT_TEMPLATE)],
        template=CLIENT_TEMPLATE,
    ),
    "_DividerServer": GsDivider("WS服务器连接", ""),
    "server_links": GsRepeatGroupConfig(
        title="连接列表",
        desc="插件自己开 WS 服务端，等协议端连进来。每条填本机监听地址与端口；"
        "一个端口可以同时接多个协议端。",
        data=[],
        template=SERVER_TEMPLATE,
    ),
}

CUSTOM_CONFIG_DEFAULT: dict[str, GSC] = {
    "_DividerImage": GsDivider("图片消息", ""),
    "custom_image_summary": GsStrConfig(
        title="图片外显",
        desc="发送图片时的外显文案，多个用英文逗号分隔随机取一个，留空用默认 [图片]。",
        data="",
    ),
    "_DividerForward": GsDivider("合并转发", ""),
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
}

CONFIG_PATH = get_res_path("OneBotUID") / "config.json"
CUSTOM_CONFIG_PATH = get_res_path("OneBotUID") / "custom_config.json"

_CUSTOM_KEYS = ("custom_image_summary", "node_qq", "node_name")
_LEGACY_LINKS_KEYS = ("links", "_DividerLinks")


def _write_raw_config(raw: dict) -> bool:
    payload = json.dumps(raw, ensure_ascii=False, indent=4).encode("utf-8")
    tmp = CONFIG_PATH.with_name(CONFIG_PATH.name + ".tmp-migrate")
    try:
        tmp.write_bytes(payload)
        os.replace(tmp, CONFIG_PATH)
    except OSError:
        return False
    return True


def _read_raw_config() -> Optional[dict]:
    if not CONFIG_PATH.is_file():
        return None
    try:
        raw = json.loads(CONFIG_PATH.read_bytes().decode("utf-8"))
    except (OSError, ValueError):
        return None
    return raw if isinstance(raw, dict) else None


def _migrate_custom_keys() -> dict[str, str]:
    raw = _read_raw_config()
    if raw is None:
        return {}

    take_values = not CUSTOM_CONFIG_PATH.exists()
    moved: dict[str, str] = {}
    found = False
    for key in _CUSTOM_KEYS:
        item = raw.pop(key, None)
        if item is None:
            continue
        found = True
        if take_values and isinstance(item, dict):
            moved[key] = str(item.get("data") or "")
    if not found:
        return {}

    if not _write_raw_config(raw):
        logger.warning("[OneBotUID] 自定义信息配置迁移失败，旧配置项暂时保留")
        return {}
    if moved:
        logger.info("[OneBotUID] 已将 图片外显 / 合并转发 配置迁移到「自定义信息」页面")
    else:
        logger.info("[OneBotUID] 已清理主配置中残留的 图片外显 / 合并转发 配置项")
    return moved


def _raw_value(item: object) -> object:
    return item.get("data") if isinstance(item, dict) else None


def _take_legacy_links() -> Optional[list[tuple[str, dict[str, object]]]]:
    raw = _read_raw_config()
    if raw is None:
        return None
    if not any(key in raw for key in _LEGACY_LINKS_KEYS):
        return None

    legacy = raw.pop("links", None)
    raw.pop("_DividerLinks", None)
    already_split = "client_links" in raw or "server_links" in raw
    rows = legacy.get("data") if isinstance(legacy, dict) else None

    moved: Optional[list[tuple[str, dict[str, object]]]] = None
    if not already_split and legacy is not None:
        moved = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            mode = str(_raw_value(row.get("connection_mode")) or "").strip()
            mode = MODE_SERVER if mode == MODE_SERVER else MODE_CLIENT
            moved.append((mode, {key: _raw_value(value) for key, value in row.items()}))

    if not _write_raw_config(raw):
        logger.warning("[OneBotUID] 连接列表拆分迁移失败，旧配置项暂时保留")
        return None
    if moved:
        logger.info("[OneBotUID] 已把连接列表按连接方式拆成 WS客户端 / WS服务器 两组")
    else:
        logger.info("[OneBotUID] 已清理主配置中残留的旧连接列表")
    return moved


def _row_from_values(values: dict[str, object], template: dict[str, GSC]) -> dict[str, GSC]:
    row: dict[str, GSC] = {}
    for key, field in template.items():
        proto = deepcopy(field)
        value = values.get(key)
        if value is not None and type(value) is type(proto.data):
            proto.data = value
        row[key] = proto
    return row


def _apply_split_links(moved: list[tuple[str, dict[str, object]]]) -> None:
    client_rows: list[dict[str, GSC]] = []
    server_rows: list[dict[str, GSC]] = []
    for mode, values in moved:
        if mode == MODE_SERVER:
            server_rows.append(_row_from_values(values, SERVER_TEMPLATE))
        else:
            client_rows.append(_row_from_values(values, CLIENT_TEMPLATE))

    onebot_config.config["client_links"].data = client_rows
    onebot_config.config["server_links"].data = server_rows
    onebot_config.write_config()
    logger.info(f"[OneBotUID] 连接列表已拆分：WS客户端 {len(client_rows)} 条，WS服务器 {len(server_rows)} 条")


_MOVED_CUSTOM = _migrate_custom_keys()
_MOVED_LINKS = _take_legacy_links()

onebot_config = StringConfig("OneBotUID", CONFIG_PATH, CONFIG_DEFAULT)
custom_config = StringConfig("自定义信息", CUSTOM_CONFIG_PATH, CUSTOM_CONFIG_DEFAULT)

if _MOVED_CUSTOM:
    for _key, _value in _MOVED_CUSTOM.items():
        _item = custom_config.config.get(_key)
        if isinstance(_item, GsConfig) and _value:
            _item.data = _value
    custom_config.write_config()

if _MOVED_LINKS is not None:
    _apply_split_links(_MOVED_LINKS)


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

    rows: list[tuple[str, dict[str, GSC]]] = [
        *((MODE_CLIENT, row) for row in _group_rows("client_links")),
        *((MODE_SERVER, row) for row in _group_rows("server_links")),
    ]
    result: list[Settings] = []
    seen: set[str] = set()
    for mode, row in rows:
        settings = _build_settings(row, mode)
        if not settings.enable:
            continue
        if settings.route_bot_id in seen:
            logger.warning(f"[OneBotUID] 连接标识 {settings.route_bot_id} 重复，已忽略后一条")
            continue
        seen.add(settings.route_bot_id)
        result.append(settings)
    return result


def _group_rows(name: str) -> list[dict[str, GSC]]:
    item = onebot_config.config.get(name)
    if isinstance(item, GsRepeatGroupConfig) and isinstance(item.data, list):
        return item.data
    return []


def _build_settings(row: dict[str, GSC], mode: str) -> Settings:
    is_server = mode == MODE_SERVER
    template = SERVER_TEMPLATE if is_server else CLIENT_TEMPLATE
    return Settings(
        name=str(_pick(row, "name", template)).strip(),
        enable=bool(_pick(row, "enable", template)),
        connection_mode=mode,
        ws_url="" if is_server else str(_pick(row, "ws_url", template)).strip(),
        listen_host=(str(_pick(row, "listen_host", template)).strip() or "0.0.0.0") if is_server else "",
        listen_port=_as_int(_pick(row, "listen_port", template), 3002) if is_server else 0,
        access_token=str(_pick(row, "access_token", template)).strip(),
        route_bot_id=str(_pick(row, "route_bot_id", template)).strip() or DEFAULT_ROUTE_BOT_ID,
        platform_bot_id=str(_pick(row, "platform_bot_id", template)).strip() or "onebot",
        ignore_self=bool(_pick(row, "ignore_self", template)),
        reconnect_interval=max(1, _as_int(_global("reconnect_interval"), 5)),
        max_reconnect=max(0, _as_int(_global("max_reconnect"), 0)),
        quote_fetch=bool(_global("quote_fetch")),
        custom_image_summary=_custom("custom_image_summary"),
        node_qq=_custom("node_qq").strip(),
        node_name=_custom("node_name").strip(),
    )


def _pick(row: dict[str, GSC], key: str, template: dict[str, GSC]) -> object:
    item = row.get(key)
    if isinstance(item, GsConfig):
        return item.data
    return template[key].data


def _global(name: str) -> object:
    item = onebot_config.config.get(name)
    if isinstance(item, GsConfig):
        return item.data
    return None


def _custom(name: str) -> str:
    item = custom_config.config.get(name)
    if isinstance(item, GsConfig):
        return str(item.data or "")
    return ""


def _as_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return default
