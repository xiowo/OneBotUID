import re
import json
import base64
from typing import Optional
from pathlib import Path
from dataclasses import dataclass

import msgspec

from gsuid_core.logger import logger
from gsuid_core.models import Message, MessageReceive

from .models import ObEvent, ObSegment, ObForwardNode, sid, pick, id_param
from .onebot_api import OneBotApi, response_data, response_failed

_AVATAR = "https://q1.qlogo.cn/g?b=qq&nk={}&s=640"
_NODE_MARK = "[合并转发]"
_IMAGE_MARK = "[图片]"
_CARD_MARK = "[卡片]"
_CARD_DEPTH = 4
_NODE_MAX_DEPTH = 3
_FILE_BASE64_MAX = 2 * 1024 * 1024
_FILE_LOCAL_MAX = 1536 * 1024
_HTTP_PREFIX = ("http://", "https://")
_FORWARD_TYPES = ("forward", "forward_msg", "node")
_CQ_PATTERN = re.compile(r"\[CQ:(\w+)(?:,[^\]]*)?\]")
_CQ_PLACEHOLDER = {
    "image": "[图片]",
    "face": "[表情]",
    "mface": "[表情]",
    "record": "[语音]",
    "video": "[视频]",
    "json": _CARD_MARK,
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


def _sid_or(value: object, fallback: str) -> str:
    text = sid(value)
    return text if text and text != "0" else fallback


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

    self_id = _sid_or(event.self_id, ctx.self_id)
    if ctx.ignore_self and self_id and user_id == self_id:
        return None

    group_id = sid(event.group_id) if message_type == "group" else ""
    scope = ChatScope(user_id=user_id, group_id=group_id)

    segments = event.message if isinstance(event.message, list) else []
    if segments:
        content = await _segments_to_content(segments, ctx, scope=scope)
    else:
        content = _cq_to_content(event.raw_message or str(event.message or ""))

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
        elif kind == "json":
            content.extend(_json_card(_card_raw(seg)))
    return content


def _card_raw(seg: ObSegment) -> object:
    return seg.data.get("data")


def _json_card(raw: object) -> list[Message]:
    card = _load_card(raw)
    if card is None:
        return [Message("text", _CARD_MARK)]
    return [Message("card", {"type": "json", "content": card}), Message("text", _card_text(card))]


def _card_text(card: Optional[dict[str, object]]) -> str:
    if card is None:
        return _CARD_MARK

    prompt = _str_of(card.get("prompt"))
    title = _nested_str(card, "title")
    desc = _nested_str(card, "desc")
    url = _nested_str(card, "jumpUrl", "qqdocurl")

    lines: list[str] = []
    for item in (prompt or title, desc):
        if item and not _is_placeholder(item) and item not in lines:
            lines.append(item)
    if url and not any(url in line for line in lines):
        lines.append(url)
    return "\n".join(lines) or _CARD_MARK


def _is_placeholder(text: str) -> bool:
    return text.isdigit()


def _load_card(raw: object) -> Optional[dict[str, object]]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return None
    try:
        card = json.loads(raw)
    except ValueError:
        logger.debug("[OneBotUID] 卡片段不是合法 JSON，降级为占位文本")
        return None
    return card if isinstance(card, dict) else None


def _str_of(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _nested_str(payload: object, *keys: str, depth: int = _CARD_DEPTH) -> str:
    if depth <= 0:
        return ""
    if isinstance(payload, dict):
        for key in keys:
            found = _str_of(payload.get(key))
            if found:
                return found
        children: object = payload.values()
    elif isinstance(payload, list):
        children = payload
    else:
        return ""
    for child in children:
        found = _nested_str(child, *keys, depth=depth - 1)
        if found:
            return found
    return ""


async def _file_message(seg: ObSegment, ctx: InboundContext, scope: Optional[ChatScope]) -> Message:
    name, payload = await _file_payload(seg, ctx, scope)
    if not payload:
        logger.debug(f"[OneBotUID] 文件段取不到内容，降级为文本: {name}")
        return Message("text", f"[文件:{name}]")
    return Message("file", f"{name}|{payload}")


async def _file_payload(seg: ObSegment, ctx: InboundContext, scope: Optional[ChatScope]) -> tuple[str, str]:
    name = _file_name(seg)
    url = pick(seg, "url")
    if url.startswith(_HTTP_PREFIX):
        return name, url
    return name, await _fetch_file_payload(seg, ctx, scope)


def _file_name(seg: ObSegment) -> str:
    raw = pick(seg, "file", "name", "file_name").replace("\\", "/")
    return raw.rsplit("/", 1)[-1].strip() or "file"


async def _fetch_file_payload(seg: ObSegment, ctx: InboundContext, scope: Optional[ChatScope]) -> str:
    if scope is None:
        return ""

    file_id = pick(seg, "file_id", "fid")
    for action, params in _file_actions(file_id, _file_name(seg), scope):
        resp = await ctx.api.call(action, params)
        if _response_bad(resp):
            continue
        data = response_data(resp)
        if not isinstance(data, dict):
            continue
        payload = _payload_of(data)
        if payload:
            return payload
    return ""


def _response_bad(resp: Optional[dict[str, object]]) -> bool:
    if resp is None:
        return True
    retcode = resp.get("retcode")
    return isinstance(retcode, int) and retcode not in (0, 1)


def _payload_of(data: dict[str, object]) -> str:
    url = data.get("url")
    if isinstance(url, str) and url.startswith(_HTTP_PREFIX):
        return url

    raw = data.get("base64")
    if isinstance(raw, str) and raw:
        payload = raw[len("base64://") :] if raw.startswith("base64://") else raw
        if len(payload) <= _FILE_BASE64_MAX:
            return payload

    for key in ("file", "path"):
        local = data.get(key)
        if isinstance(local, str) and local:
            payload = _read_local_as_base64(local)
            if payload:
                return payload
    return ""


def _read_local_as_base64(path: str) -> str:
    local = Path(path)
    try:
        if not local.is_file() or local.stat().st_size > _FILE_LOCAL_MAX:
            return ""
        return base64.b64encode(local.read_bytes()).decode()
    except OSError:
        return ""


def _file_actions(file_id: str, name: str, scope: ChatScope) -> list[tuple[str, dict[str, object]]]:
    actions: list[tuple[str, dict[str, object]]] = []
    if file_id and scope.group_id:
        actions.append(("get_group_file_url", {"group_id": id_param(scope.group_id), "file_id": file_id}))
    elif file_id and scope.user_id:
        actions.append(("get_private_file_url", {"file_id": file_id}))
        actions.append(("get_private_file_url", {"user_id": id_param(scope.user_id), "file_id": file_id}))
    if file_id:
        actions.append(("get_file", {"file_id": file_id}))
    if name:
        actions.append(("get_file", {"file": name}))
    return actions


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

    text = "".join(_inline_text(seg) for seg in segments if seg.type in ("text", "json")).strip()
    forward = [seg for seg in segments if seg.type in _FORWARD_TYPES]
    files = [seg for seg in segments if seg.type == "file"]

    items: list[Message] = []
    for seg in forward:
        items.extend(await _forward_items(pick(seg, "id", "message_id", "resid"), ctx, scope=scope))
    if forward and not items:
        text = "\n".join([text, *(_forward_title(seg) for seg in forward)]).strip()

    image_msgs: list[Message] = []
    for seg in segments:
        if seg.type != "image":
            continue
        text = "\n".join(item for item in (text, _image_label(seg)) if item)
        media = pick(seg, "url")
        if not media:
            raw = pick(seg, "file")
            if raw.startswith((*_HTTP_PREFIX, "base64://")):
                media = raw
        if media:
            image_msgs.append(Message("image", media))

    quoted_scope = _quote_scope(data, scope)
    file_msgs: list[Message] = []
    for seg in files:
        name, payload = await _file_payload(seg, ctx, quoted_scope)
        text = "\n".join(item for item in (text, f"[文件:{name}]") if item)
        if payload:
            file_msgs.append(Message("file", f"{name}|{payload}"))

    content: list[Message] = [Message("reply", text)]
    content.extend(image_msgs)
    content.extend(file_msgs)
    if items:
        content.append(Message("node", items))
    return content


def _inline_text(seg: ObSegment) -> str:
    if seg.type == "json":
        return _card_text(_load_card(_card_raw(seg)))
    return pick(seg, "text")


def _image_label(seg: ObSegment) -> str:
    raw = pick(seg, "summary", "file", "name").strip().strip("[]").strip()
    return f"[图片:{raw}]" if raw and raw != "图片" else _IMAGE_MARK


def _quote_scope(data: dict[str, object], scope: Optional[ChatScope]) -> Optional[ChatScope]:
    group_id = sid(data.get("group_id"))
    if group_id:
        return ChatScope(user_id=sid(data.get("user_id")), group_id=group_id)
    if scope is not None and not scope.group_id:
        return ChatScope(user_id=scope.user_id)
    return scope


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
    if resp is None or response_failed(resp):
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
            items.extend(_cq_to_content(raw))
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


def _cq_to_content(raw: str) -> list[Message]:
    if not raw:
        return []

    content: list[Message] = []
    last = 0
    for match in _CQ_PATTERN.finditer(raw):
        chunk = raw[last : match.start()]
        if chunk.strip():
            content.append(Message("text", chunk))

        kind = match.group(1)
        qq = _cq_param(match.group(0), "qq")
        if kind == "at" and qq:
            content.append(Message("at", qq))
        else:
            placeholder = _CQ_PLACEHOLDER.get(kind, "")
            if placeholder:
                content.append(Message("text", placeholder))
        last = match.end()

    tail = raw[last:]
    if tail.strip():
        content.append(Message("text", tail))
    return content


def _cq_param(code: str, key: str) -> str:
    matched = re.search(rf"(?:^|,){key}=([^,\]]*)", code)
    return matched.group(1).strip() if matched else ""


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
    self_id = _sid_or(event.self_id, ctx.self_id)
    if name == "poke":
        data["target_id"] = _sid_or(event.target_id, self_id)

    return MessageReceive(
        bot_id=ctx.platform_bot_id,
        bot_self_id=self_id,
        msg_id="",
        user_type="group" if group_id else "direct",
        group_id=group_id or None,
        user_id=user_id,
        sender={},
        user_pm=6,
        content=[Message(f"meta-{name}", data)],
    )
