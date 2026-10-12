"""兑换码发放插件。

在指定的群聊中，允许指定的管理员通过自然语言索要兑换码：每次调用发放一个
未使用的兑换码，并把该码转移到已使用记录、记下使用时间。一个码只发一次。

实现要点：

- 发放通过 ``@Tool`` 暴露给麦麦，由大模型自行判断何时调用，而不是拦截消息。
  这样兑换码的发放与结果都会留在麦麦的对话上下文里，它自然知道自己发过什么码；
  ``@Command``（正则一次性编译）与 blocking Hook（在消息登记前 return）都做不到这点。
- 插件 Tool 默认是 ``deferred``，只能靠 ``tool_search`` 才能被发现，因此显式声明
  ``visibility="visible"`` 让它直接进入工具列表。
- 群白名单由工具在**调用时**实时读取配置校验，因此在配置页改完立即生效。
  这里刻意**不用**组件的 ``allowed_session``：宿主只在组件注册时读取该字段，
  而配置热更新不会重新注册组件，用它做白名单会导致"改了配置但工具还挂在旧群上"，
  且插件仍会打印"配置已更新"，极难排查。
- 群聊里工具拿不到发起人 QQ（``MaiChatSession`` 对群会话显式把 ``user_id`` 置空，
  见 ``chat_session_data_model``；宿主注入工具上下文的 ``user_id`` 同样来自该字段），
  因此身份靠两条路还原：先看本插件观察型 Hook 记下的最近发言人，查不到再回退到
  数据库里该会话最近一条消息的发送者。
- 数据库兜底**必须**再核对一次「这条消息是不是本次请求」，否则会认错人：多人群里
  最后一条未必是当前请求者，若刚好是管理员说过话，旁边的非管理员说一句「发个兑换码」
  也能领到码。核对方式是把这条消息的时间戳与「工具被调用的时刻」比对，超出窗口就
  保守拒绝（见 ``_resolve_caller``）。消息里没有可用时间戳时同样拒绝，绝不放行。
- 兑换码池与已使用记录都存放在插件自己的 ``config.toml`` 里，因此可以直接在插件
  配置页里填写和查看。宿主对插件目录的文件监听只认 ``.py`` 和 ``_manifest.json``
  （见 ``integration._is_watchable_plugin_source``），改写 ``config.toml`` 只会定向
  触发本插件的 ``on_config_update``，不会引发全局重载；因此这里采用「内存改好 →
  直接整体覆盖写」的方式，不在插件目录里创建任何临时文件。
"""

from __future__ import annotations

import json
import logging
import random
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tomlkit

from maibot_sdk import CONFIG_RELOAD_SCOPE_SELF, Field, HookHandler, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder

logger = logging.getLogger(__name__)

# 插件目录与配置文件：plugin.py 就位于插件根目录下。
PLUGIN_DIR = Path(__file__).resolve().parent
CONFIG_FILENAME = "config.toml"

# 已使用记录在配置页展示时保留的最大条数，超出后丢弃最旧的记录。
MAX_USED_RECORDS = 1000

# 已使用记录里用于打码的兑换码：保留前若干字符，其余以星号代替。
# 余量查询工具据此避免回显完整兑换码明文。
MASK_KEEP_PREFIX = 4
MASK_KEEP_SUFFIX = 2

# 数据库兜底时允许的「消息时间戳与本次工具调用时刻」最大偏差（秒）。
# 超过该偏差说明这条消息不是本次请求，不能据此认定请求者身份。
# 取值需覆盖「用户发言 → 麦麦思考 → 调用工具」的整段耗时。
IDENTITY_FALLBACK_WINDOW_SECONDS = 120

# 同进程内串行化「读改写 config.toml」，避免并发触发时互相覆盖。
_CONFIG_WRITE_LOCK = threading.Lock()

# 已使用记录里的兑换码字段。优先匹配 JSON 字符串（1.2.1 起的格式），
# 它能容纳兑换码里的 ``|`` 与 ``:``；匹配不到再退回旧的无引号格式。
_USED_CODE_JSON_RE = re.compile(r'码:("(?:[^"\\]|\\.)*")')
_USED_CODE_LEGACY_RE = re.compile(r"码:([^|]*)")


class PluginSectionConfig(PluginConfigBase):
    """插件开关。"""

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(
        default=True,
        description="是否启用兑换码功能；关闭后麦麦完全不会读取兑换码。",
    )
    config_version: str = Field(default="1.0.0", description="配置版本")


class AccessConfig(PluginConfigBase):
    """群与管理员白名单。"""

    __ui_label__ = "权限"
    __ui_icon__ = "shield-check"
    __ui_order__ = 1

    group_whitelist: list[str] = Field(
        default_factory=list,
        description="允许使用兑换码功能的群号；列表为空时任何群都不生效。",
        json_schema_extra={"hint": "只填群号纯数字，例如 123456789；改完需要重载插件才生效"},
    )
    admin_whitelist: list[str] = Field(
        default_factory=list,
        description="允许发放兑换码的管理员 QQ 号。",
        json_schema_extra={"hint": "只有这些 QQ 号能触发兑换码发放；其他人一律回绝"},
    )


class TriggerConfig(PluginConfigBase):
    """激发信号。"""

    __ui_label__ = "激发信号"
    __ui_icon__ = "zap"
    __ui_order__ = 2

    trigger_signals: list[str] = Field(
        default_factory=list,
        description="额外提示词，会附在工具说明里，帮助麦麦理解什么样的请求该发放兑换码。",
        json_schema_extra={
            "hint": "由大模型判断是否发放，因此这是「提示语」而不是精确匹配的开关；改完需重载插件生效"
        },
    )


class RedeemPoolConfig(PluginConfigBase):
    """兑换码池。"""

    __ui_label__ = "兑换码池"
    __ui_icon__ = "ticket"
    __ui_order__ = 3

    available_codes: str = Field(
        default="",
        description="待发放的兑换码，一行一个（中间的多个空行会被忽略）。",
        json_schema_extra={
            "x-widget": "textarea",
            "rows": 10,
            "hint": "在这里粘贴兑换码后点页面底部的保存即写入；每发放一个，该码会自动从未使用移到下面的已使用记录。",
        },
    )
    used_codes: list[str] = Field(
        default_factory=list,
        description=f"已使用的兑换码记录（插件自动维护，最多保留 {MAX_USED_RECORDS} 条，最新在前）。",
        json_schema_extra={"hint": "由插件在发放时自动写入，包含使用时间、群号和领取人；手动删除条目不影响已发出去的兑换码。"},
    )


class ReplyConfig(PluginConfigBase):
    """回复文案。"""

    __ui_label__ = "回复文案"
    __ui_icon__ = "message-square"
    __ui_order__ = 4

    code_prefix: str = Field(
        default="你的兑换码：",
        description="兑换码前面加的前缀文案。",
    )
    custom_message: str = Field(
        default="",
        description="发放兑换码时附带的固定话术，留空则只发兑换码本身。",
        json_schema_extra={
            "x-widget": "textarea",
            "rows": 3,
            "hint": "例如：这是你的一份小心意，请收好～（会显示在兑换码上方）",
        },
    )
    no_permission_reply: str = Field(
        default="没有权限哦",
        description="非白名单成员在开放群里索要时回复的文案。",
    )
    empty_reply: str = Field(
        default="我的钱包都已经空了",
        description="兑换码已发完时回复的文案。",
    )


class ToolConfig(PluginConfigBase):
    """工具行为。"""

    __ui_label__ = "工具"
    __ui_icon__ = "wrench"
    __ui_order__ = 5

    expose_status_tool: bool = Field(
        default=True,
        description=(
            "是否额外提供一个查询兑换码余量的工具，方便麦麦回答「还有多少码」。"
            "该工具与发码工具受同样的群白名单与管理员白名单限制。"
        ),
    )
    used_record_code: str = Field(
        default="mask",
        description="余量查询工具展示最近发放记录时，兑换码字段的呈现方式。",
        json_schema_extra={
            "hint": "hide=完全不显示；mask=只显示首尾各几个字符（推荐）；full=显示完整兑换码，"
            "任何白名单管理员都能看到别人领走的码，通常不需要"
        },
    )
    recent_message_limit: int = Field(
        default=10,
        description="回退到数据库确认请求者身份时，读取的最近消息条数。",
        json_schema_extra={"hint": "越大越稳，但查询稍慢；一般 10 足够"},
    )
    speaker_ttl_seconds: int = Field(
        default=300,
        description="内存里记住的「最近发言人」有效期（秒），超时后改用数据库确认身份。",
    )


class RedeemCodeConfig(PluginConfigBase):
    """兑换码发放插件配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    access: AccessConfig = Field(default_factory=AccessConfig)
    trigger: TriggerConfig = Field(default_factory=TriggerConfig)
    pool: RedeemPoolConfig = Field(default_factory=RedeemPoolConfig)
    reply: ReplyConfig = Field(default_factory=ReplyConfig)
    tool: ToolConfig = Field(default_factory=ToolConfig)


def parse_code_lines(raw_text: str) -> List[str]:
    """把多行文本解析为兑换码列表。

    Args:
        raw_text: 用户在配置里粘贴的原始文本。

    Returns:
        List[str]: 去掉空行与首尾空白后的兑换码，按出现顺序去重。
    """

    codes: List[str] = []
    seen: set[str] = set()
    for line in str(raw_text or "").splitlines():
        code = line.strip()
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def dump_code_lines(codes: List[str]) -> str:
    """把兑换码列表序列化回多行文本，保持与配置页文本框一致。

    Args:
        codes: 待发放的兑换码列表。

    Returns:
        str: 一行一个兑换码的文本；列表为空时返回空字符串。
    """

    return "\n".join(codes)


class RedeemCodeDistributorPlugin(MaiBotPlugin):
    """兑换码发放插件。"""

    config_model = RedeemCodeConfig

    def __init__(self) -> None:
        super().__init__()
        # 兑换码池的内存权威副本，与 config.toml 保持同步。
        self._available_codes: List[str] = []
        self._used_records: List[str] = []
        # 记录本插件最后一次写回 config.toml 的兑换码池文本，
        # 用于区分「自己写回触发的回调」与「用户在配置页改的」。
        self._last_written_codes_text: Optional[str] = None
        # 会话 -> (群号, QQ, 记录时刻)，由观察型 Hook 维护，供工具确认是谁在索要。
        self._recent_speakers: Dict[str, Tuple[str, str, float]] = {}

    # ===== 组件声明 =====

    def get_components(self) -> List[Dict[str, Any]]:
        """在 SDK 收集结果的基础上把工具限定为群聊，并注入激发信号提示。

        这里**故意不用** ``allowed_session`` 去按群过滤。宿主仅在组件注册时读取
        ``allowed_session``，而配置热更新（``on_config_update``）不会重新注册组件，
        因此用它做群白名单会导致「配置页改了但工具还挂在旧群上」——新群看不到工具、
        旧群却能继续用，且插件仍会打印"配置已更新"，非常难排查。
        群白名单改由发码工具在调用时实时校验（见 ``handle_issue_code``），
        这样在配置页改完立即生效。

        把配置里的「激发信号」拼进发码工具的描述：它们是给大模型看的提示语，
        让麦麦知道用户惯常用哪些说法索要兑换码。该注入同样只在注册时发生，
        改完需重载插件。

        Returns:
            List[Dict[str, Any]]: 补齐过滤条件后的组件声明列表。
        """

        components = super().get_components()
        hint_line = self._build_trigger_hint()

        for component in components:
            if component.get("type") != "TOOL":
                continue
            component["chat_scope"] = "group"
            if hint_line and component.get("name") == "redeem_code_issue":
                metadata = component.get("metadata")
                if isinstance(metadata, dict):
                    base = str(metadata.get("description") or "")
                    metadata["description"] = f"{base}\n{hint_line}" if base else hint_line
        return components

    def _build_trigger_hint(self) -> str:
        """把配置里的激发信号拼成给大模型的提示语。

        Returns:
            str: 形如「用户也可能说：……，这些同样应当调用本工具。」的提示；
            未配置激发信号时返回空字符串。
        """

        try:
            signals = [str(item).strip() for item in self.config.trigger.trigger_signals if str(item).strip()]
        except Exception:
            signals = []
        if not signals:
            return ""
        listed = "、".join(f"「{item}」" for item in signals)
        return f"本群用户还可能用这些说法索要兑换码：{listed}。遇到这些说法同样调用本工具。"

    # ===== 生命周期 =====

    async def on_load(self) -> None:
        """加载配置里的兑换码池到内存。"""

        self._reload_pool_from_config()
        logger.info(
            "兑换码发放插件已加载：待发放 %d 个，已使用记录 %d 条",
            len(self._available_codes),
            len(self._used_records),
        )

    async def on_unload(self) -> None:
        """卸载时无需额外清理，兑换码池已实时落盘。"""

        return None

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """处理配置热更新。

        自己在发放后写回 config.toml 也会走到这里；因为写回内容与内存状态
        一致，重新按配置装载得到的结果相同，是幂等的。用户在配置页保存时，
        这里会把新的兑换码池装载进来。

        Args:
            scope: 配置变更范围。
            config_data: 最新的配置数据。
            version: 配置版本号。
        """

        if scope != CONFIG_RELOAD_SCOPE_SELF:
            return

        pool = config_data.get("pool")
        incoming_text = ""
        if isinstance(pool, dict):
            raw_available = pool.get("available_codes")
            incoming_text = "" if raw_available is None else str(raw_available)

        # 自己写回引发的回声：内存状态已经是最新的，跳过重新装载，
        # 避免把刚发放的兑换码重新算回未使用。
        if self._last_written_codes_text is not None and incoming_text == self._last_written_codes_text:
            return

        self._reload_pool_from_config()
        logger.info(
            "兑换码池已按配置更新：待发放 %d 个，已使用记录 %d 条",
            len(self._available_codes),
            len(self._used_records),
        )

    # ===== 发言人记录 =====

    @HookHandler(
        "chat.receive.after_process",
        name="redeem_code_speaker_tracker",
        description="记录群里最近发言的人，供兑换码工具确认请求者身份",
        mode=HookMode.OBSERVE,
        order=HookOrder.LATE,
        error_policy=ErrorPolicy.SKIP,
    )
    async def track_recent_speaker(self, message: Any = None, **kwargs: Any) -> Dict[str, Any]:
        """把入站消息的发言人记进内存，覆盖之前记录。

        群聊会话对象不保留发言人身份，而工具又拿不到发起人 QQ，因此这里先留一份
        轻量记录；工具优先查它，查不到再回退数据库。这是观察型处理器，不参与
        主链路控制流，失败也不影响消息处理。

        Args:
            message: 宿主传入的序列化会话消息。
            **kwargs: 其余 Hook 参数（忽略）。

        Returns:
            Dict[str, Any]: 恒为继续执行。
        """

        del kwargs
        try:
            if not isinstance(message, dict):
                return {"action": "continue"}
            session_id = str(message.get("session_id") or "").strip()
            message_info = message.get("message_info")
            if not session_id or not isinstance(message_info, dict):
                return {"action": "continue"}
            user_info = message_info.get("user_info")
            group_info = message_info.get("group_info")
            if not isinstance(user_info, dict) or not isinstance(group_info, dict):
                # 私聊不参与兑换码发放。
                return {"action": "continue"}
            user_id = str(user_info.get("user_id") or "").strip()
            group_id = str(group_info.get("group_id") or "").strip()
            if not user_id or not group_id:
                return {"action": "continue"}
            self._recent_speakers[session_id] = (group_id, user_id, time.monotonic())
        except Exception:
            logger.debug("记录最近发言人失败", exc_info=True)
        return {"action": "continue"}

    def _load_recent_speaker(self, session_id: str) -> Optional[Tuple[str, str]]:
        """从内存记录里取出最近发言人，过期则忽略。

        Args:
            session_id: 会话 ID。

        Returns:
            Optional[Tuple[str, str]]: ``(群号, QQ)``；没有有效记录时返回 ``None``。
        """

        record = self._recent_speakers.get(session_id)
        if record is None:
            return None
        group_id, user_id, recorded_at = record
        ttl = max(int(self.config.tool.speaker_ttl_seconds), 1)
        if time.monotonic() - recorded_at > ttl:
            self._recent_speakers.pop(session_id, None)
            return None
        return group_id, user_id

    async def _resolve_caller(self, session_id: str, group_id_hint: str) -> Optional[Tuple[str, str]]:
        """确认这次请求是谁在哪个群发起的。

        群聊里工具拿不到发起人 QQ，所以先用观察型 Hook 留下的内存记录，再回退到
        数据库。回退时**必须**核对这条消息确实是本次请求：多人群里最后一条未必是
        当前请求者，若刚好是管理员说过话，旁边的非管理员说一句「发个兑换码」也能
        领到码。核对方式是把消息时间戳与本次工具调用时刻比对，超出
        ``IDENTITY_FALLBACK_WINDOW_SECONDS`` 就保守拒绝。

        群号则以宿主注入的 ``group_id_hint`` 为准——它来自当前会话，是最权威的；
        内存记录与数据库里的群号只是兜底。

        Args:
            session_id: 会话 ID。
            group_id_hint: 宿主注入的群号。

        Returns:
            Optional[Tuple[str, str]]: ``(群号, QQ)``；无法确认时返回 ``None``。
        """

        hint = str(group_id_hint or "").strip()
        speaker = self._load_recent_speaker(session_id)
        if speaker is not None:
            recorded_group, user_id = speaker
            return (hint or recorded_group, user_id)

        try:
            limit = max(int(self.config.tool.recent_message_limit), 1)
            recent = await self.ctx.message.get_recent(session_id, limit=limit)
        except Exception:
            logger.error("查询最近消息失败，无法确认请求者: session_id=%s", session_id, exc_info=True)
            return None

        if not isinstance(recent, list):
            return None

        now = time.time()
        # find_messages 在 limit_mode="latest" 下仍按时间正序返回，所以从后往前找，
        # 取最近一条「与本次请求时间吻合」的消息。
        for item in reversed(recent):
            if not isinstance(item, dict):
                continue
            info = item.get("message_info")
            if not isinstance(info, dict):
                continue
            user_info = info.get("user_info")
            if not isinstance(user_info, dict):
                continue
            user_id = str(user_info.get("user_id") or "").strip()
            if not user_id:
                continue

            sent_at = self._message_timestamp(item)
            if sent_at is None or abs(now - sent_at) > IDENTITY_FALLBACK_WINDOW_SECONDS:
                # 这条消息与本次请求对不上（或无法判断时间），不能据此认定身份。
                # 继续往前找一条更早的，只会更对不上，因此直接拒绝。
                logger.warning(
                    "数据库兜底身份校验未通过：消息时间与本次请求偏差过大或缺失，已保守拒绝"
                    " session_id=%s message_id=%s",
                    session_id,
                    item.get("message_id"),
                )
                return None

            group_info = info.get("group_info")
            group_id = ""
            if isinstance(group_info, dict):
                group_id = str(group_info.get("group_id") or "").strip()
            return (group_id or group_id_hint, user_id)
        return None

    # ===== 兑换码池维护 =====

    def _reload_pool_from_config(self) -> None:
        """按当前配置重建内存中的兑换码池与已使用记录。"""

        self._available_codes = parse_code_lines(self.config.pool.available_codes)
        self._used_records = [str(item).strip() for item in self.config.pool.used_codes if str(item).strip()]
        self._last_written_codes_text = None

    def _issue_one_code(self, group_id: str, user_id: str) -> Tuple[Optional[str], int, bool]:
        """随机取出一个未使用的兑换码并记录使用情况。

        取码与写盘在同一段同步逻辑里完成，期间没有 await，因此并发触发时
        不会把同一个兑换码发两次。

        写盘失败时会**把内存改动整体回滚**并返回 ``False``，调用方据此拒绝
        发放。若在这里"照发不误"，磁盘上的池子仍含该码，插件重载后会再发一次。

        Args:
            group_id: 领取人所在的群号。
            user_id: 领取人的 QQ 号。

        Returns:
            Tuple[Optional[str], int, bool]: ``(本次发出的兑换码, 发放后剩余数量, 是否成功)``。
            兑换码发完时返回 ``(None, 0, True)``；写盘失败时返回 ``(None, 原数量, False)``。
        """

        if not self._available_codes:
            return None, 0, True

        index = random.randrange(len(self._available_codes))
        code = self._available_codes.pop(index)
        record = self._format_used_record(code, group_id, user_id)
        self._used_records.insert(0, record)
        # 只保留最近 MAX_USED_RECORDS 条，避免配置文件无限膨胀。
        # 被挤掉的尾部先留一份，落盘失败时才能原样放回。
        dropped: List[str] = []
        if len(self._used_records) > MAX_USED_RECORDS:
            dropped = self._used_records[MAX_USED_RECORDS:]
            del self._used_records[MAX_USED_RECORDS:]

        if not self._persist_pool():
            # 回滚：把码放回原位置，撤掉刚插入的记录，并放回被挤掉的尾部。
            self._available_codes.insert(index, code)
            del self._used_records[0]
            if dropped:
                self._used_records.extend(dropped)
            logger.error(
                "写回兑换码池失败，已取消本次发放并回滚内存状态（群:%s QQ:%s）",
                group_id,
                user_id,
            )
            return None, len(self._available_codes), False

        return code, len(self._available_codes), True

    @staticmethod
    def _format_used_record(code: str, group_id: str, user_id: str) -> str:
        """生成一条已使用记录的展示文本。

        兑换码本身用 JSON 字符串包起来，这样即使它含有 ``|`` 或 ``:``，
        余量查询工具也能精确定位并打码，而不会误伤时间、群号或 QQ。

        Args:
            code: 已发放的兑换码。
            group_id: 领用的群号。
            user_id: 领用的 QQ 号。

        Returns:
            str: 形如 ``2026-10-09 21:30:00 | 码:"XXX" | 群:123 | QQ:456`` 的记录。
        """

        used_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"{used_at} | 码:{json.dumps(code, ensure_ascii=False)} | 群:{group_id or '-'} | QQ:{user_id or '-'}"

    @staticmethod
    def _message_timestamp(item: Dict[str, Any]) -> Optional[float]:
        """取出消息载荷里的时间戳，统一换算为秒。

        宿主把 ``timestamp`` 序列化成「秒」的字符串（见
        ``PluginMessageUtils._session_message_to_dict``），但为兼容其它来源，
        这里同时接受秒与毫秒。

        Args:
            item: ``get_recent`` 返回的单条消息字典。

        Returns:
            Optional[float]: Unix 时间戳（秒）；缺失或无法解析时返回 ``None``。
        """

        raw = item.get("timestamp")
        if raw is None:
            return None
        try:
            value = float(str(raw).strip())
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        # 13 位及以上按毫秒处理。
        return value / 1000.0 if value > 1e11 else value

    def _persist_pool(self) -> bool:
        """把内存中的兑换码池写回插件自己的 config.toml。

        以内存中的完整配置为骨架生成文档，再覆盖兑换码相关的两个字段，
        因此无论配置文件是否已存在、是否缺少某个配置节，写回结果都是完整
        且可再次加载的——尤其能保证 ``[plugin]`` 节不会丢失（缺少该节会
        触发宿主的 ``PluginConfigVersionError`` 而使插件无法加载）。

        写盘失败会**返回 False**，由调用方回滚内存改动。这一点很关键：若写盘
        失败却照常发码，磁盘上的 ``available_codes`` 仍含该码，插件重载后会
        再发一次，同一个码就被发出两次。

        按宿主约定不在插件目录创建任何临时文件。

        Returns:
            bool: 写盘成功返回 ``True``，失败返回 ``False``。
        """

        codes_text = dump_code_lines(self._available_codes)
        config_path = PLUGIN_DIR / CONFIG_FILENAME

        with _CONFIG_WRITE_LOCK:
            try:
                document = tomlkit.parse(tomlkit.dumps(self.get_plugin_config_data()))
                pool_table = document.get("pool")
                if pool_table is None:
                    pool_table = tomlkit.table()
                    document["pool"] = pool_table
                pool_table["available_codes"] = codes_text
                pool_table["used_codes"] = list(self._used_records)

                with open(config_path, "w", encoding="utf-8") as handle:
                    handle.write(tomlkit.dumps(document))
            except Exception:
                logger.error("写回兑换码池到 %s 失败", config_path, exc_info=True)
                return False

        self._last_written_codes_text = codes_text
        return True

    # ===== 工具 =====

    @Tool(
        "redeem_code_issue",
        description=(
            "当有人索要兑换码、兑换券、福利码、激活码、礼品码，或说「发个兑换码」「给个码」"
            "「有没有兑换码」「要个码」时，立即调用本工具发放。"
            "\n"
            "本工具已经直接可用，不需要、也不要先调用 tool_search 去搜索它。"
            "\n"
            "本工具会自行校验发放权限，并保证一个兑换码只会被发出一次。"
            "\n"
            "拿到结果后必须遵守："
            "1) 若返回文本里包含兑换码，把它原样逐字发给用户，不要改写、不要省略任何字符；"
            "2) 若返回的是拒绝或已发完的提示文案，把该文案原样转述给用户，不要自行编造别的说法；"
            "3) 不要重复调用本工具，一次请求只发一个码。"
        ),
        parameters=[],
        visibility="visible",
    )
    async def handle_issue_code(
        self,
        stream_id: str = "",
        chat_id: str = "",
        group_id: str = "",
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """校验权限并发放一个兑换码。

        Args:
            stream_id: 宿主注入的会话 ID。
            chat_id: 宿主注入的会话 ID（与 ``stream_id`` 同义，兼容不同调用路径）。
            group_id: 宿主注入的群号。
            **kwargs: 其余参数（忽略）。

        Returns:
            Dict[str, Any]: 供宿主解析的工具结果，``content`` 为要让麦麦转述的文本。
        """

        del kwargs

        if not self.config.plugin.enabled:
            return {"success": False, "error": "兑换码功能当前未启用，请自然回绝对方。"}

        session_id = str(stream_id or chat_id or "").strip()
        if not session_id:
            logger.warning("发放兑换码失败：本次调用没有 session_id")
            return {"success": False, "error": "无法确定当前会话，未发放兑换码。"}

        caller = await self._resolve_caller(session_id, str(group_id or "").strip())
        if caller is None:
            logger.warning("发放兑换码失败：无法确认请求者身份 session_id=%s", session_id)
            return {"success": False, "error": "暂时无法确认你的身份，请稍后再试。"}

        resolved_group_id, user_id = caller

        group_whitelist = {str(item).strip() for item in self.config.access.group_whitelist if str(item).strip()}
        if resolved_group_id not in group_whitelist:
            # 群白名单的主校验点。这里用配置文件的最新值，因此在配置页改完立即生效，
            # 不像组件注册期属性那样要重载插件。措辞要求麦麦当作普通消息处理、不要提兑换码。
            return {"success": False, "error": "本群未开通该功能，请当作普通聊天回应，不要提及兑换码。"}

        admin_whitelist = {str(item).strip() for item in self.config.access.admin_whitelist if str(item).strip()}
        if user_id not in admin_whitelist:
            return {"success": False, "error": str(self.config.reply.no_permission_reply or "没有权限哦")}

        code, remaining, ok = self._issue_one_code(resolved_group_id, user_id)
        if not ok:
            return {
                "success": False,
                "error": "兑换码池暂时无法写入，本次没有发放，请稍后再试。",
            }
        if code is None:
            return {"success": False, "error": str(self.config.reply.empty_reply or "我的钱包都已经空了")}

        logger.info(
            "已向 QQ:%s（群:%s）发放兑换码，剩余 %d 个",
            user_id,
            resolved_group_id,
            remaining,
        )
        return {"success": True, "content": self._build_issue_content(code, remaining)}

    @Tool(
        "redeem_code_status",
        description=(
            "查询当前还有多少兑换码可发、以及最近发出去的记录。"
            "当有人询问「还剩多少兑换码」「谁领过」时使用。"
            "本工具不消耗兑换码，但同样只对白名单群里的白名单管理员开放。"
        ),
        parameters=[],
        visibility="visible",
    )
    async def handle_code_status(
        self,
        stream_id: str = "",
        chat_id: str = "",
        group_id: str = "",
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """报告兑换码余量与最近使用记录。

        本工具与发码工具受**完全相同**的群白名单 + 管理员白名单约束。这不是
        多余的：已使用记录里含兑换码、群号与领取人 QQ，而兑换码本身就是凭据，
        若任何人都能查询，等于把已经发出去的码再送人一次。

        Args:
            stream_id: 宿主注入的会话 ID。
            chat_id: 宿主注入的会话 ID（与 ``stream_id`` 同义）。
            group_id: 宿主注入的群号。
            **kwargs: 其余参数（忽略）。

        Returns:
            Dict[str, Any]: 供宿主解析的工具结果。
        """

        del kwargs

        if not self.config.tool.expose_status_tool:
            return {"success": False, "error": "当前未开放兑换码余量查询，请自然回绝对方。"}

        if not self.config.plugin.enabled:
            return {"success": False, "error": "兑换码功能当前未启用，请自然回绝对方。"}

        session_id = str(stream_id or chat_id or "").strip()
        if not session_id:
            logger.warning("查询兑换码余量失败：本次调用没有 session_id")
            return {"success": False, "error": "无法确定当前会话，未查询兑换码。"}

        caller = await self._resolve_caller(session_id, str(group_id or "").strip())
        if caller is None:
            logger.warning("查询兑换码余量失败：无法确认请求者身份 session_id=%s", session_id)
            return {"success": False, "error": "暂时无法确认你的身份，请稍后再试。"}

        resolved_group_id, user_id = caller

        group_whitelist = {str(item).strip() for item in self.config.access.group_whitelist if str(item).strip()}
        if resolved_group_id not in group_whitelist:
            return {"success": False, "error": "本群未开通该功能，请当作普通聊天回应，不要提及兑换码。"}

        admin_whitelist = {str(item).strip() for item in self.config.access.admin_whitelist if str(item).strip()}
        if user_id not in admin_whitelist:
            return {"success": False, "error": str(self.config.reply.no_permission_reply or "没有权限哦")}

        remaining = len(self._available_codes)
        lines = [f"当前还有 {remaining} 个兑换码可发。"]
        records = self._render_used_records(limit=5)
        if records:
            lines.append("最近发放记录：")
            lines.extend(f"- {item}" for item in records)
        return {"success": True, "content": "\n".join(lines)}

    def _mask_code(self, code: str) -> str:
        """按配置把已使用记录里的兑换码打码。

        首尾保留的字符数会随码长收缩（各不超过码长的四分之一），因此
        **至少有一半字符被隐藏**——否则短码会被首尾拼接完全还原。

        Args:
            code: 原始兑换码。

        Returns:
            str: 打码后的兑换码；配置为 ``full`` 时原样返回。
        """

        mode = str(self.config.tool.used_record_code or "mask").strip().lower()
        if mode == "full":
            return code
        total = len(code)
        if total == 0:
            return code
        keep_prefix = min(MASK_KEEP_PREFIX, total // 4)
        keep_suffix = min(MASK_KEEP_SUFFIX, total // 4)
        hidden = total - keep_prefix - keep_suffix
        if hidden <= 0:
            return "*" * total
        tail = code[total - keep_suffix :] if keep_suffix else ""
        return f"{code[:keep_prefix]}{'*' * hidden}{tail}"

    def _render_used_records(self, limit: int) -> List[str]:
        """生成用于展示的已使用记录，兑换码按配置打码。

        记录形如 ``2026-10-09 21:30:00 | 码:"XXXX**YZ" | 群:123 | QQ:456``。
        兑换码字段是 JSON 字符串，这里用正则按「带引号的完整字符串」匹配，
        因此即使兑换码本身含 ``|`` 或 ``:`` 也能被正确取出并打码——**不会**
        因为按 ``|`` 切分而把整个码原样漏出去。

        Args:
            limit: 最多返回多少条。

        Returns:
            List[str]: 可直接展示的记录文本。
        """

        mode = str(self.config.tool.used_record_code or "mask").strip().lower()
        if mode == "full":
            return [str(item) for item in self._used_records[: max(int(limit), 0)]]
        return [
            self._mask_record(str(item), mode)
            for item in self._used_records[: max(int(limit), 0)]
        ]

    def _mask_record(self, text: str, mode: str) -> str:
        """把一条记录里的兑换码字段替换为打码后的文本。

        Args:
            text: 一条已使用记录。
            mode: ``hide`` 或 ``mask``。

        Returns:
            str: 替换后的记录；无法定位兑换码字段时原样返回。
        """

        trailing = ""
        match = _USED_CODE_JSON_RE.search(text)
        if match is not None:
            try:
                code = str(json.loads(match.group(1)))
            except Exception:
                return text
        else:
            # 旧格式（1.2.0 及更早）没有引号，回退到按 | 截断。
            legacy = _USED_CODE_LEGACY_RE.search(text)
            if legacy is None:
                return text
            raw = legacy.group(1)
            code = raw.strip()
            if not code:
                return text
            # 旧格式的码后面原本带一个空格，替换后补回来，保持排版一致。
            trailing = raw[len(raw.rstrip()) :]
            match = legacy

        if mode == "hide":
            replacement = "码:已隐藏"
        else:
            replacement = f"码:{json.dumps(self._mask_code(code), ensure_ascii=False)}"
        return f"{text[: match.start()]}{replacement}{trailing}{text[match.end() :]}"

    def _build_issue_content(self, code: str, remaining: int) -> str:
        """组合交给麦麦转述的发放文本。

        Args:
            code: 本次发放的兑换码。
            remaining: 发放后剩余的兑换码数量。

        Returns:
            str: 包含话术、前缀与兑换码的完整文本。
        """

        parts: List[str] = ["发放成功。请自然地告诉对方，并把下面这串兑换码原样逐字发出（不要改写、不要省略任何字符）："]

        custom_message = str(self.config.reply.custom_message or "").strip()
        if custom_message:
            parts.append(custom_message)
        parts.append(f"{self.config.reply.code_prefix}{code}")
        if remaining <= 0:
            parts.append("（这已经是最后一个兑换码。）")
        return "\n".join(parts)


def create_plugin() -> RedeemCodeDistributorPlugin:
    """创建兑换码发放插件实例。

    Returns:
        RedeemCodeDistributorPlugin: 插件实例。
    """

    return RedeemCodeDistributorPlugin()
