"""适配器管理指令，仅主人可用。"""

import json

from gsuid_core.sv import SV
from gsuid_core.bot import Bot
from gsuid_core.models import Event

from .runtime import adapter_runtime

sv = SV("OneBotUID", pm=0, area="ALL")


@sv.on_command("状态", block=True)
async def onebot_status(bot: Bot, ev: Event) -> None:
    await bot.send(f"[OneBotUID]\n{adapter_runtime.describe(ev.WS_BOT_ID)}")


@sv.on_command("重连", block=True)
async def onebot_reconnect(bot: Bot, ev: Event) -> None:
    await bot.send(await adapter_runtime.force_reconnect(ev.WS_BOT_ID))


_dbg_sv = SV("OneBotUID调试", pm=0, area="ALL")

_USAGE = '用法：onebot调接口 <接口名><JSON参数>\n例：onebot调接口 get_group_member_list{"group_id": 7777}'


@_dbg_sv.on_command("调接口", block=True)
async def onebot_debug_call_api(bot: Bot, ev: Event) -> None:
    from .api import OneBotApiError, call_bot_api

    try:
        action, params = _parse_call(ev.text)
    except ValueError as exc:
        await bot.send(f"[调试] {exc}\n{_USAGE}")
        return

    try:
        data = await call_bot_api(action, params, self_id=ev.bot_self_id)
    except OneBotApiError as exc:
        await bot.send(f"[调试] self_id={ev.bot_self_id} {action} {params}\n{exc}")
        return

    await bot.send(f"[调试] self_id={ev.bot_self_id} {action} {params}\n{_brief(data)}")


def _parse_call(text: str) -> tuple[str, dict[str, object]]:
    """把 ``<接口名><JSON>`` 拆成接口名和参数，用触发这条指令的机器人自己调。"""
    raw = text.strip()
    if not raw:
        raise ValueError("缺少接口名")

    cut = min((i for i in (raw.find("{"), raw.find("[")) if i >= 0), default=-1)
    action, payload = (raw, "") if cut < 0 else (raw[:cut].strip(), raw[cut:].strip())
    if not action:
        raise ValueError("缺少接口名")
    if not payload:
        return action, {}

    try:
        loaded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"参数不是合法 JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ValueError('参数必须是 JSON 对象，例如 {"group_id": 7777}')
    return action, loaded


def _brief(data: object) -> str:
    if isinstance(data, list):
        return f"共 {len(data)} 条，首条：{_dump(data[0]) if data else '空'}"
    return _dump(data)


def _dump(value: object) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=repr)
    return text if len(text) <= 400 else f"{text[:400]}…"
