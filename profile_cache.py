import time
import asyncio
from typing import Dict, Optional
from dataclasses import dataclass

from gsuid_core.logger import logger

from .onebot_api import OneBotApi, response_data, response_failed

# QQ 群头像没有对应接口，只有这条公开路径
_GROUP_AVATAR = "https://p.qlogo.cn/gh/{gid}/{gid}/100"
# 查询失败后隔多久再试：协议端一直报错时别每条消息都打一遍
_RETRY_SECONDS = 300.0
# 同时最多几个查询在飞，避免涌进来的群消息把协议端打满
_CONCURRENCY = 4


@dataclass
class GroupProfile:
    name: str = ""
    icon: str = ""
    count: int = 0
    expire_at: float = 0.0

    @property
    def alive(self) -> bool:
        return self.expire_at > time.time()


_profiles: Dict[str, GroupProfile] = {}
_inflight: set[str] = set()
_tasks: set[asyncio.Task] = set()
_gate: Optional[asyncio.Semaphore] = None


def _semaphore() -> asyncio.Semaphore:
    global _gate
    if _gate is None:
        _gate = asyncio.Semaphore(_CONCURRENCY)
    return _gate


def cached(group_id: str) -> Optional[GroupProfile]:
    """还没过期就直接给，不发请求。"""
    item = _profiles.get(group_id)
    if item is None or not item.alive:
        return None
    return item


def forget(group_id: str = "") -> None:
    """清掉缓存；不传 group_id 就全清。"""
    if group_id:
        _profiles.pop(group_id, None)
    else:
        _profiles.clear()


def reset() -> None:
    """插件停止时收摊：清缓存，取消还没跑完的查询。"""
    global _gate

    forget()
    _inflight.clear()
    for task in list(_tasks):
        task.cancel()
    _tasks.clear()
    # 信号量跟着旧的事件循环一起丢掉，下次调度时重建
    _gate = None


def schedule(api: OneBotApi, bot_id: str, group_id: str, hours: int) -> None:
    """后台补一次群资料。

    不阻塞消息处理；同一个群正在查 / 已缓存 / 配置关掉，都直接返回。
    """
    if hours <= 0 or not group_id.isdigit():
        return
    if cached(group_id) is not None or group_id in _inflight:
        return

    _inflight.add(group_id)
    task = asyncio.create_task(
        _refresh(api, bot_id, group_id, hours),
        name=f"OneBotUID.groupinfo.{group_id}",
    )
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def _refresh(api: OneBotApi, bot_id: str, group_id: str, hours: int) -> None:
    try:
        async with _semaphore():
            if not api.connected:
                return
            resp = await api.call("get_group_info", {"group_id": int(group_id), "no_cache": False})

        if response_failed(resp):
            _profiles[group_id] = GroupProfile(expire_at=time.time() + _RETRY_SECONDS)
            return

        data = response_data(resp) or {}
        name = str(data.get("group_name") or "").strip()
        profile = GroupProfile(
            name=name,
            icon=_GROUP_AVATAR.format(gid=group_id),
            count=_as_int(data.get("member_count")),
            expire_at=time.time() + (hours * 3600 if name else _RETRY_SECONDS),
        )
        _profiles[group_id] = profile
        await _write_back(bot_id, group_id, profile)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.debug(f"[OneBotUID] 查询群资料失败 {group_id}: {exc}")
    finally:
        _inflight.discard(group_id)


async def _write_back(bot_id: str, group_id: str, profile: GroupProfile) -> None:
    """写回 CoreGroup"""
    from gsuid_core.utils.database.models import CoreGroup

    payload: Dict[str, object] = {}
    if profile.name:
        payload["group_name"] = profile.name
    if profile.icon:
        payload["group_icon"] = profile.icon
    if profile.count:
        payload["group_count"] = profile.count
    if not payload:
        return

    try:
        row = await CoreGroup.base_select_data(group_id=group_id)
        if row is None:
            await CoreGroup.full_insert_data(bot_id=bot_id, group_id=group_id, **payload)
        else:
            await CoreGroup.update_data_by_xx({"group_id": group_id}, **payload)
    except Exception as exc:
        logger.debug(f"[OneBotUID] 写回群资料失败 {group_id}: {exc}")


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return 0
