from typing import Optional

from .runtime import adapter_runtime
from .onebot_api import response_failed

DEFAULT_TIMEOUT = 30.0


class OneBotApiError(RuntimeError):
    """接口没调通：找不到连接、协议端返回失败或等响应超时。"""

    def __init__(
        self,
        action: str,
        message: str,
        response: Optional[dict[str, object]] = None,
    ) -> None:
        super().__init__(f"接口 {action} 调用失败: {message}")
        self.action = action
        self.response = response


async def call_bot_api(
    action: str,
    params: Optional[dict[str, object]] = None,
    self_id: str = "",
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, object]:
    """调 OneBot 接口，返回响应里的 data；失败抛 ``OneBotApiError``。

    :param action: OneBot v11 接口名，如 ``get_group_member_list``。
    :param params: 接口参数，原样交给协议端。
    :param self_id: 目标机器人 QQ，一般传 ``ev.bot_self_id``；只有一条连接时可留空。
    :param timeout: 等协议端响应的秒数。
    """
    api = adapter_runtime.find_api(self_id)
    if api is None:
        raise OneBotApiError(action, f"找不到 self_id={self_id or '未知'} 对应的连接")

    resp = await api.call(action, params, timeout)
    if resp is None:
        raise OneBotApiError(action, "连接不可用或等待响应超时")
    if response_failed(resp):
        message = resp.get("wording") or resp.get("message") or "协议端返回失败"
        raise OneBotApiError(action, str(message), resp)

    data = resp.get("data")
    if not isinstance(data, dict):
        raise OneBotApiError(action, "响应里没有 data", resp)
    return data
