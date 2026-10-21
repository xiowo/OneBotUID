import re
from typing import Optional
from dataclasses import dataclass

import msgspec

from gsuid_core.logger import logger
from gsuid_core.models import Message, MessageReceive

from .models import ObEvent, ObSegment, ObForwardNode, sid, pick, id_param
from .onebot_api import OneBotApi, response_data, response_failed

_AVATAR = "https://q1.qlogo.cn/g?b=qq&nk={}&s=640"
_NODE_MARK = "[合并转发]"
_NODE_MAX_DEPTH = 3
_FILE_BASE64_MAX = 2 * 1024 * 1024
_FORWARD_TYPES = ("forward", "forward_msg", "node")
_CQ_PATTERN = re.compile(r"\[CQ:(\w+)(?:,[^\]]*)?\]")
_CQ_PLACEHOLDER = {
    "image": "[图片]",
    "face": "[表情]",
    "mface": "[表情]",
    "record": "[语音]",
    "video": "[视频]",
    "at": "@",
    "reply": "",
}


@dataclass
class InboundContext:
    platform_bot_id: str
    self_id: str
    ignore_self: bool
    quote_fetch: bool
    api: OneBotApi


@dataclass(frozen=True)
class ChatScope:
    user_id: str = ""
    group_id: str = ""


async def event_to_receive(event: ObEvent, ctx: InboundContext) -> Optional[MessageReceive]:
    if event.post_type == "message":
        return await _from_message(event, ctx)
    if event.post_type == "notice":
        return _from_notice(event, ctx)
    return None


async def _from_message(event: ObEvent, ctx: InboundContext) -> Optional[MessageReceive]:
    message_type = event.message_type or ("group" if event.group_id else "private")
    if message_type not in ("group", "private"):
        return None

    user_id = sid(event.user_id) or sid(event.sender.user_id)
    if not user_id:
        return None

    self_id = ctx.self_id or sid(event.self_id)
    if ctx.ignore_self and self_id and user_id == self_id:
        return None

    group_id = sid(event.group_id) if message_type == "group" else ""
    scope = ChatScope(user_id=user_id, group_id=group_id)

    segments = event.message if isinstance(event.message, list) else []
    if segments:
        content = await _segments_to_content(segments, ctx, scope=scope)
    else:
        content = [Message("text", _cq_to_text(event.raw_message or str(event.message or "")))]

    reply_id = ""
    for seg in segments:
        if seg.type == "reply":
            reply_id = pick(seg, "id")
            break
    if reply_id:
        content.append(Message("reply_id", reply_id))
        content.extend(await _quote_context(reply_id, ctx, scope))

    content = [item for item in content if item.type != "text" or str(item.data or "").strip()]
    if not content:
        return None

    sender = {
        "user_id": user_id,
        "nickname": event.sender.card or event.sender.nickname or "",
        "card": event.sender.card,
        "role": event.sender.role,
        "avatar": _AVATAR.format(user_id),
    }

    return MessageReceive(
        bot_id=ctx.platform_bot_id,
        bot_self_id=self_id,
        msg_id=sid(event.message_id),
        user_type="group" if group_id else "direct",
        group_id=group_id or None,
        user_id=user_id,
        sender=sender,
        user_pm=6,
        content=content,
    )


async def _segments_to_content(
    segments: list[ObSegment],
    ctx: InboundContext,
    depth: int = 0,
    seen: Optional[set[str]] = None,
    scope: Optional[ChatScope] = None,
) -> list[Message]:
    content: list[Message] = []
    for seg in segments:
        kind = seg.type
        if kind == "text":
            content.append(Message("text", pick(seg, "text")))
        elif kind == "image":
            url = pick(seg, "url", "file")
            if url:
                content.append(Message("image", url))
        elif kind == "at":
            content.append(Message("at", pick(seg, "qq")))
        elif kind == "face":
            content.append(Message("text", f"[表情:{pick(seg, 'id')}]"))
        elif kind in ("record", "video"):
            media = pick(seg, "url", "file")
            if media:
                content.append(Message(kind, media))
        elif kind == "file":
            content.append(await _file_message(seg, ctx, scope))
        elif kind in _FORWARD_TYPES:
            nodes = await _forward_items(pick(seg, "id", "message_id", "resid"), ctx, depth, seen, scope)
            content.append(Message("node", nodes or [Message("text", _NODE_MARK)]))
        elif kind == "mface":
            content.append(Message("text", pick(seg, "summary") or "[表情]"))
    return content


async def _file_message(seg: ObSegment, ctx: InboundContext, scope: Optional[ChatScope]) -> Message:
    name = pick(seg, "file", "name") or "file"
    payload = pick(seg, "url") or await _fetch_file_url(seg, ctx, scope)
    if not payload:
        logger.debug(f"[OneBotUID] 文件段取不到链接，降级为文本: {name}")
        return Message("text", f"[文件:{name}]")
    return Message("file", f"{name}|{payload}")


async def _fetch_file_url(seg: ObSegment, ctx: InboundContext, scope: Optional[ChatScope]) -> str:
    file_id = pick(seg, "file_id", "fid", "file")
    if not file_id or scope is None:
        return ""

    for action, params in _file_actions(file_id, scope):
        resp = await ctx.api.call(action, params)
        if response_failed(resp):
            continue
        data = response_data(resp)
        if data is None:
            continue
        url = data.get("url")
        if isinstance(url, str) and url:
            return url
        raw = data.get("base64")
        if isinstance(raw, str) and raw and len(raw) <= _FILE_BASE64_MAX:
            return raw[len("base64://") :] if raw.startswith("base64://") else raw
    return ""


def _file_actions(file_id: str, scope: ChatScope) -> list[tuple[str, dict[str, object]]]:
    if scope.group_id:
        head = ("get_group_file_url", {"group_id": id_param(scope.group_id), "file_id": file_id})
    elif scope.user_id:
        head = ("get_private_file_url", {"user_id": id_param(scope.user_id), "file_id": file_id})
    else:
        return []
    return [head, ("get_file", {"file_id": file_id})]


async def _quote_context(reply_id: str, ctx: InboundContext, scope: Optional[ChatScope] = None) -> list[Message]:
    placeholder = [Message("reply", "")]
    if not ctx.quote_fetch:
        return placeholder

    resp = await ctx.api.call("get_msg", {"message_id": id_param(reply_id)})
    if response_failed(resp):
        return placeholder
    data = response_data(resp)
    if data is None or not _quote_in_scope(data, scope, ctx.self_id):
        return placeholder

    segments = _segments_of(data.get("message"))
    if not segments:
        return placeholder

    text = "".join(pick(seg, "text") for seg in segments if seg.type == "text").strip()
    images = [pick(seg, "url", "file") for seg in segments if seg.type == "image"]
    forward = [seg for seg in segments if seg.type in _FORWARD_TYPES]

    items: list[Message] = []
    for seg in forward:
        items.extend(await _forward_items(pick(seg, "id", "message_id", "resid"), ctx, scope=scope))
    if forward and not items:
        text = "\n".join([text, *(_forward_title(seg) for seg in forward)]).strip()

    content: list[Message] = [Message("reply", text)]
    content.extend(Message("image", url) for url in images if url)
    if items:
        content.append(Message("node", items))
    return content


def _quote_in_scope(data: dict[str, object], scope: Optional[ChatScope], self_id: str) -> bool:
    if scope is None:
        return True
    if scope.group_id:
        group_id = sid(data.get("group_id"))
        return not group_id or group_id == scope.group_id
    user_id = sid(data.get("user_id"))
    return not user_id or user_id in (scope.user_id, self_id)


def _forward_title(seg: ObSegment) -> str:
    return pick(seg, "prompt", "summary", "title", "name") or _NODE_MARK


async def _forward_items(
    forward_id: str,
    ctx: InboundContext,
    depth: int = 0,
    seen: Optional[set[str]] = None,
    scope: Optional[ChatScope] = None,
) -> list[Message]:
    if seen is None:
        seen = set()
    if not forward_id or depth >= _NODE_MAX_DEPTH or forward_id in seen:
        return []

    seen.add(forward_id)
    resp = await ctx.api.call("get_forward_msg", {"id": forward_id})
    if response_failed(resp):
        return []
    data = response_data(resp)
    entries = _forward_entries(data if data is not None else resp)
    if not entries:
        return []

    items: list[Message] = []
    for entry in entries:
        node = _to_forward_node(entry)
        if node is None:
            continue
        nickname = (node.sender.card or node.sender.nickname or node.nickname or node.name).strip()
        if nickname:
            items.append(Message("text", f"{nickname}:"))
        raw = node.message if node.message is not None else node.content
        if isinstance(raw, str):
            items.append(Message("text", _cq_to_text(raw)))
        elif isinstance(raw, list):
            items.extend(await _segments_to_content(raw, ctx, depth + 1, seen, scope))

    return items


def _forward_entries(payload: dict[str, object]) -> list[object]:
    messages = payload.get("messages")
    if isinstance(messages, list):
        return messages
    return []


def _to_forward_node(entry: object) -> Optional[ObForwardNode]:
    if not isinstance(entry, dict):
        return None
    inner = entry.get("data") if entry.get("type") == "node" else None
    source = inner if isinstance(inner, dict) else entry
    try:
        return msgspec.convert(source, ObForwardNode)
    except msgspec.ValidationError:
        logger.debug("[OneBotUID] 合并转发节点格式异常，已跳过")
        return None


def _segments_of(raw: object) -> list[ObSegment]:
    if not isinstance(raw, list):
        return []
    segments: list[ObSegment] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            segments.append(msgspec.convert(item, ObSegment))
        except msgspec.ValidationError:
            continue
    return segments


def _cq_to_text(raw: str) -> str:
    if not raw:
        return ""

    def _replace(match: re.Match[str]) -> str:
        return _CQ_PLACEHOLDER.get(match.group(1), "")

    return _CQ_PATTERN.sub(_replace, raw).strip()


def _from_notice(event: ObEvent, ctx: InboundContext) -> Optional[MessageReceive]:
    if event.notice_type == "group_increase":
        name = "user_join_group"
    elif event.notice_type == "group_decrease":
        name = "user_exit_group"
    elif event.notice_type == "notify" and event.sub_type == "poke":
        name = "poke"
    else:
        return None

    user_id = sid(event.user_id)
    if not user_id:
        return None

    group_id = sid(event.group_id)
    data: dict[str, str] = {"user_id": user_id}
    if group_id:
        data["group_id"] = group_id
    operator_id = sid(event.operator_id)
    if operator_id:
        data["operator_id"] = operator_id
    if event.sub_type:
        data["sub_type"] = event.sub_type
    if name == "poke":
        data["target_id"] = sid(event.target_id) or ctx.self_id

    return MessageReceive(
        bot_id=ctx.platform_bot_id,
        bot_self_id=ctx.self_id or sid(event.self_id),
        msg_id="",
        user_type="group" if group_id else "direct",
        group_id=group_id or None,
        user_id=user_id,
        sender={},
        user_pm=6,
        content=[Message(f"meta-{name}", data)],
    )
