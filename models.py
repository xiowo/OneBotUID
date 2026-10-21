import msgspec


class ObSender(msgspec.Struct):
    user_id: int | str = 0
    nickname: str = ""
    card: str = ""
    role: str = "member"


class ObSegment(msgspec.Struct):
    type: str = ""
    data: dict[str, object] = msgspec.field(default_factory=dict)


class ObEvent(msgspec.Struct):
    post_type: str = ""
    message_type: str = ""
    sub_type: str = ""
    notice_type: str = ""
    self_id: int | str = 0
    user_id: int | str = 0
    group_id: int | str | None = None
    operator_id: int | str | None = None
    target_id: int | str | None = None
    message_id: int | str = 0
    message: list[ObSegment] | str | None = None
    raw_message: str = ""
    sender: ObSender = msgspec.field(default_factory=ObSender)


class ObForwardNode(msgspec.Struct):
    sender: ObSender = msgspec.field(default_factory=ObSender)
    nickname: str = ""
    name: str = ""
    message: list[ObSegment] | str | None = None
    content: list[ObSegment] | str | None = None


def sid(value: object) -> str:
    if value is None or value == "":
        return ""
    return str(value)


def pick(seg: ObSegment, *keys: str) -> str:
    for key in keys:
        value = seg.data.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, str) and value:
            return value
        if isinstance(value, int):
            return str(value)
    return ""


def id_param(value: str) -> int | str:
    return int(value) if value.isdigit() else value
