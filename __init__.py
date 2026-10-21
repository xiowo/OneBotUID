from gsuid_core.sv import Plugins
from gsuid_core.logger import logger
from gsuid_core.server import on_core_start, on_core_shutdown

Plugins(
    name="OneBotUID",
    force_prefix=["onebot"],
    allow_empty_prefix=False,
)

from . import commands  # noqa: E402,F401
from .runtime import adapter_runtime  # noqa: E402


@on_core_start
async def _start_onebot_adapter() -> None:
    await adapter_runtime.start()


@on_core_shutdown
async def _stop_onebot_adapter() -> None:
    await adapter_runtime.stop()
    logger.info("[OneBotUID] 插件已退出")
