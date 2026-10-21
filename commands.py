"""适配器管理指令，仅主人可用。"""

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
