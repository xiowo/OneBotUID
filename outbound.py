import re
import random
from typing import Callable, Optional, Awaitable

import msgspec

from gsuid_core.logger import logger
from gsuid_core.models import Message, MessageSend, MessageReceive

from .config import Settings
from .models import sid, id_param
from .onebot_api import OneBotApi, response_data, response_failed

msgjson = msgspec.json

_NODE_MAX = 99
_LOG_PREFIX = "log_"
_SIGNATURE_MAX = 32
_SIGNATURE_PUNCT = "，,。.！!？?、；;：:"
_CQ_AT_RE = re.compile(r"\[CQ:at,\s*qq=([0-9]+|all)\]", re.IGNORECASE)


class OutboundSender:
    def __init__(
        self,
        pick_api: Callable[[str], Optional[OneBotApi]],
        push: Callable[[MessageReceive], Awaitable[None]],
        settings: Settings,
        self_id: Callable[[], str],
        nickname: Callable[[], str],
    ) -> None:
        self._pick_api = pick_api
        self._push = push
        self._settings = settings
        self._self_id = self_id
        self._nickname = nickname
        self._carry_self_id = True

    async def __call__(self, payload: bytes) -> None:
        await self.dispatch(payload)

    async def dispatch(self, payload: bytes) -> None:
        try:
            send = msgjson.decode(payload, type=MessageSend)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            logger.warning(f"[OneBotUID] 核心下发的帧无法解析，已跳过: {exc}")
            return

        content = send.content or []
        if not content:
            return

        head = content[0]
        if len(content) == 1 and head.type == "excute_delete_message":
            await self._delete(head.data, send.bot_self_id)
            return
        if len(content) == 1 and head.type == "excute_ban_user":
            await self._ban(head.data, send.bot_self_id)
            return
        if head.type is not None and head.type.startswith(_LOG_PREFIX):
            self._log(head.type, head.data)
            return

        if not send.target_id:
            logger.warning("[OneBotUID] 核心下发的消息缺少 target_id，已丢弃")
            if send.echo:
                await self._receipt(send, None)
            return

        segments = self._to_segments(content)
        if not segments:
            if send.echo:
                await self._receipt(send, None)
            return

        message_id = await self._send(send, segments)
        if send.echo:
            await self._receipt(send, message_id)

    async def _send(self, send: MessageSend, segments: list[dict[str, object]]) -> Optional[str]:
        target = sid(send.target_id)
        direct = send.target_type == "direct"
        if any(seg.get("type") == "node" for seg in segments):
            resp = await self._send_forward(target, direct, segments, send.bot_self_id)
        elif direct:
            resp = await self._request(
                "send_private_msg",
                {"user_id": id_param(target), "message": segments},
                send.bot_self_id,
            )
        else:
            resp = await self._request(
                "send_group_msg",
                {"group_id": id_param(target), "message": segments},
                send.bot_self_id,
            )
        if response_failed(resp):
            return None
        data = response_data(resp)
        if data is None:
            return None
        return sid(data.get("message_id")) or None

    async def _request(
        self,
        action: str,
        params: dict[str, object],
        self_id: str,
    ) -> Optional[dict[str, object]]:
        api = self._pick_api(self_id)
        if api is None:
            logger.warning(f"[OneBotUID] 找不到 self_id={self_id or '未知'} 对应的连接，接口 {action} 已跳过")
            return None

        if not (self._carry_self_id and self_id):
            return await api.call(action, params)

        resp = await api.call(action, {**params, "self_id": id_param(self_id)})
        if resp is None or not response_failed(resp):
            return resp

        self._carry_self_id = False
        logger.debug(f"[OneBotUID] 协议端不接受 self_id 参数，后续发送不再携带（接口 {action}）")
        return await api.call(action, params)

    async def _send_forward(
        self,
        target: str,
        direct: bool,
        segments: list[dict[str, object]],
        self_id: str,
    ) -> Optional[dict[str, object]]:
        nodes = [seg for seg in segments if seg.get("type") == "node"]
        plain = [seg for seg in segments if seg.get("type") != "node"]
        if plain:
            nodes.insert(0, self._wrap_node(plain))
        action = "send_private_forward_msg" if direct else "send_group_forward_msg"
        key = "user_id" if direct else "group_id"
        return await self._request(action, {key: id_param(target), "messages": nodes}, self_id)

    def _wrap_node(self, segments: list[dict[str, object]]) -> dict[str, object]:
        return self._make_node(segments, *self._node_identity())

    def _make_node(self, segments: list[dict[str, object]], qq: str, nickname: str) -> dict[str, object]:
        return {"type": "node", "data": {"user_id": qq, "nickname": nickname, "content": segments}}

    def _node_identity(self) -> tuple[str, str]:
        qq = self._settings.node_qq or self._self_id() or "10000"
        nickname = self._settings.node_name or self._nickname() or "机器人"
        return qq, nickname

    async def _receipt(self, send: MessageSend, message_id: Optional[str]) -> None:
        direct = send.target_type == "direct"
        target = sid(send.target_id)
        msg = MessageReceive(
            bot_id=send.bot_id,
            bot_self_id=send.bot_self_id,
            msg_id="",
            user_type="direct" if direct else "group",
            group_id=None if direct else target,
            user_id=target if direct else "",
            sender={},
            user_pm=6,
            content=[Message("recall_message_id", {"echo": send.echo, "id": message_id})],
        )
        await self._push(msg)

    async def _delete(self, data: object, self_id: str) -> None:
        if not isinstance(data, dict):
            return
        message_id = sid(data.get("message_id"))
        if not message_id:
            return
        await self._request("delete_msg", {"message_id": id_param(message_id)}, self_id)

    async def _ban(self, data: object, self_id: str) -> None:
        if not isinstance(data, dict):
            return
        user_id = sid(data.get("user_id"))
        group_id = sid(data.get("group_id"))
        duration = data.get("duration")
        if not user_id or not group_id:
            return
        if isinstance(duration, str) and duration.isdigit():
            seconds = int(duration)
        elif isinstance(duration, int) and not isinstance(duration, bool):
            seconds = duration
        else:
            logger.warning("[OneBotUID] 禁言控制包 duration 非法，已忽略")
            return
        await self._request(
            "set_group_ban",
            {
                "group_id": id_param(group_id),
                "user_id": id_param(user_id),
                "duration": seconds,
            },
            self_id,
        )

    def _log(self, kind: str, data: object) -> None:
        level = kind[len(_LOG_PREFIX) :].lower()
        text = str(data or "")
        if level == "error":
            logger.error(f"[早柚] {text}")
        elif level == "warning":
            logger.warning(f"[早柚] {text}")
        elif level == "success":
            logger.success(f"[早柚] {text}")
        else:
            logger.info(f"[早柚] {text}")

    def _to_segments(self, content: list[Message]) -> list[dict[str, object]]:
        segments: list[dict[str, object]] = []
        for msg in content:
            kind = msg.type
            data = msg.data
            if kind is None or data is None:
                continue
            if kind == "text":
                segments.extend(_text_segments(str(data)))
            elif kind == "image":
                segments.append(
                    {
                        "type": "image",
                        "data": {"file": _strip_scheme(str(data)), "summary": self._summary()},
                    }
                )
            elif kind == "at":
                segments.append({"type": "at", "data": {"qq": str(data)}})
            elif kind in ("reply", "reply_id"):
                segments.append({"type": "reply", "data": {"id": str(data)}})
            elif kind in ("record", "video"):
                segments.append({"type": kind, "data": {"file": _strip_scheme(str(data))}})
            elif kind == "file":
                segment = _file_segment(str(data))
                if segment is not None:
                    segments.append(segment)
            elif kind == "node":
                segments.extend(self._node_segments(data))
            elif kind == "markdown":
                segments.extend(_text_segments(str(data)))
        return segments

    def _summary(self) -> str:
        options = [item.strip() for item in self._settings.custom_image_summary.split(",") if item.strip()]
        if not options:
            return "[图片]"
        return random.choice(options)

    def _node_segments(self, data: object) -> list[dict[str, object]]:
        if not isinstance(data, list):
            return []
        items: list[Message] = []
        for item in data:
            if isinstance(item, Message):
                items.append(item)
            elif isinstance(item, dict):
                kind = item.get("type")
                items.append(Message(str(kind) if kind else None, item.get("data")))
        return self._build_nodes(items)[:_NODE_MAX]

    def _build_nodes(self, items: list[Message]) -> list[dict[str, object]]:
        default_qq, default_name = self._node_identity()
        if self._plain_lines(items):
            return [self._make_node(self._to_segments([item]), default_qq, default_name) for item in items]

        nodes: list[dict[str, object]] = []
        current: list[dict[str, object]] = []
        nickname = default_name

        def flush() -> None:
            nonlocal current
            if current:
                nodes.append(self._make_node(current, default_qq, nickname))
                current = []

        for index, item in enumerate(items):
            if item.type == "text":
                text = str(item.data or "").strip()
                if _is_signature(text) and index + 1 < len(items):
                    flush()
                    nickname = text.rstrip(":：").strip() or default_name
                    continue
            current.extend(self._to_segments([item]))

        flush()
        return nodes

    def _plain_lines(self, items: list[Message]) -> bool:
        if len(items) < 2 or not all(item.type == "text" for item in items):
            return False
        signatures = sum(1 for item in items if _is_signature(str(item.data or "").strip()))
        return signatures < 2


def _is_signature(text: str) -> bool:
    if not text.endswith((":", "：")):
        return False
    name = text.rstrip(":：").strip()
    if not name or len(name) > _SIGNATURE_MAX:
        return False
    return not any(ch.isspace() or ch in _SIGNATURE_PUNCT for ch in name)


def _text_segments(raw: str) -> list[dict[str, object]]:
    if "[CQ:" not in raw:
        return [{"type": "text", "data": {"text": raw}}]

    segments: list[dict[str, object]] = []
    last = 0
    for matched in _CQ_AT_RE.finditer(raw):
        chunk = raw[last : matched.start()]
        if chunk:
            segments.append({"type": "text", "data": {"text": chunk}})
        segments.append({"type": "at", "data": {"qq": matched.group(1)}})
        last = matched.end()
    tail = raw[last:]
    if tail:
        segments.append({"type": "text", "data": {"text": tail}})
    return segments


def _strip_scheme(raw: str) -> str:
    for scheme in ("link://", "file://"):
        if raw.startswith(scheme):
            return raw[len(scheme) :]
    return raw


def _file_segment(raw: str) -> Optional[dict[str, object]]:
    name, sep, content = raw.partition("|")
    if not sep:
        return None
    content = content.strip()
    if content.startswith(("base64://", "http://", "https://")):
        file_value = content
    elif content.startswith("link://"):
        file_value = content[len("link://") :]
    elif content:
        file_value = f"base64://{content}"
    else:
        return None
    return {"type": "file", "data": {"file": file_value, "name": name.strip() or "file"}}
