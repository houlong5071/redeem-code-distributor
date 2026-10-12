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
- 群聊里工具拿不到发起人 QQ（``MaiChatSession`` 对群会话显式把 ``user_id`` 置空），
  因此身份靠两条路还原：先看本插件观察型 Hook 记下的最近发言人（准确、含重启前的
  窗口），查不到再回退到数据库里该会话最近一条消息的发送者。
- 兑换码池与已使用记录都存放在插件自己的 ``config.toml`` 里，因此可以直接在插件
  配置页里填写和查看。宿主对插件目录的文件监听只认 ``.py`` 和 ``_manifest.json``
  （见 ``integration._is_watchable_plugin_source``），改写 ``config.toml`` 只会定向
  触发本插件的 ``on_config_update``，不会引发全局重载；因此这里采用「内存改好 →
  直接整体覆盖写」的方式，不在插件目录里创建任何临时文件。
"""

from __future__ import annotations

import copy
import hashlib
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

# 「最近发言人」内存记录的最大会话数，超出后淘汰最旧的，避免长期运行下无限增长。
MAX_RECENT_SPEAKERS = 512

# 允许的「来自未来」的时间戳偏差（秒）。超出即视为时间不可信、保守拒绝。
FUTURE_TIMESTAMP_TOLERANCE_SECONDS = 30

# 同进程内串行化「读改写 config.toml」，避免并发触发时互相覆盖。
_CONFIG_WRITE_LOCK = threading.Lock()

# 从已使用记录里抠出兑换码字段。记录形如
# ``2026-10-09 21:30:00 | 码:"XXXX" | 群:123 | QQ:456``；
# 兑换码用 JSON 字符串包裹（可含转义），因此即使码本身带 ``|`` 或 ``:`` 也能精确匹配。
# 同时兼容未加引号的旧格式记录。
_USED_CODE_RE = re.compile(r'\| 码:(?P<code>"(?:[^"\\]|\\.)*"|[^|]*?)\s*(?=\||$)')


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
        json_schema_extra={"hint": "由插件在发放时自动写入，包含使用时间、群号和领取人；手动删除条目不会让该兑换码回到待发放池。"},
    )
    issued_code_hashes: list[str] = Field(
        default_factory=list,
        description=(
            "已发放过的兑换码哈希（插件自动维护，不随上面的展示记录裁剪）。"
            "用于在配置页陈旧保存、把已发出的码写回待发放池时把它挡住。"
        ),
        json_schema_extra={
            "hint": "自动维护，请勿手改。每条为 sha256 前 16 位十六进制，不含兑换码明文。"
        },
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
        description="是否额外提供一个查询兑换码余量的工具，方便麦麦回答「还有多少码」。",
    )
    recent_message_limit: int = Field(
        default=10,
        description="回退到数据库确认请求者身份时，读取的最近消息条数。",
        json_schema_extra={"hint": "越大越稳，但查询稍慢；一般 10 足够"},
    )
    speaker_ttl_seconds: int = Field(
        default=90,
        description="内存里记住的「最近发言人」有效期（秒），超时后改用数据库确认身份。"
        "内存记录是身份判定的第一路径，且不经过时间窗校验——它的可信度完全依赖于"
        "「记录发生在本次请求之前」这一时序。把它收得和 identity_fallback_window_seconds"
        "同量级，可以限制时序万一变化时的影响范围：超时后就会落到带时间窗的数据库兜底。",
        json_schema_extra={"hint": "建议与下面的兜底时间窗保持同量级；默认 90 秒"},
    )
    identity_fallback_window_seconds: int = Field(
        default=90,
        description="身份兜底时允许的「最近消息」时间窗（秒）。内存记录失效后，只有当该会话最后一条消息"
        "足够新（说明多半就是本次请求）才认它的发送者，避免把群里上一个说话的人误认成请求者。"
        "设为 0 表示禁用兜底，身份只认内存记录，认不出就保守拒绝。",
        json_schema_extra={"hint": "0 = 禁用兜底，最安全；默认 90 秒"},
    )


class RedeemCodeConfig(PluginConfigBase):
    """兑换码发放插件配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    access: AccessConfig = Field(default_factory=AccessConfig)
    trigger: TriggerConfig = Field(default_factory=TriggerConfig)
    pool: RedeemPoolConfig = Field(default_factory=RedeemPoolConfig)
    reply: ReplyConfig = Field(default_factory=ReplyConfig)
    tool: ToolConfig = Field(default_factory=ToolConfig)


def _code_hash(code: str) -> str:
    """计算兑换码的短哈希，用于「已发放过」判定。

    标记集合会随发放次数长期增长并落盘，因此存哈希而非明文：既避免在配置文件里
    额外堆积一份兑换码明文，也不受兑换码自身字符（``|``、``:``、换行等）影响。
    取 sha256 前 16 位十六进制，够用且短。

    Args:
        code: 兑换码原文。

    Returns:
        str: 16 位十六进制哈希。
    """

    return hashlib.sha256(code.encode("utf-8")).hexdigest()[:16]


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
        # 已发放过的兑换码哈希集合（**不随展示记录裁剪**），单调保护靠它判定。
        # 展示用的 _used_records 只保留最近 MAX_USED_RECORDS 条，被裁掉的那些码
        # 若只从记录里推断就认不出来了，因此单独维护一份不裁剪的集合，并落盘保存。
        # 存哈希而非明文：这份集合会长期增长，且不需要还原出原码。
        self._issued_hashes: set[str] = set()
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
        expose_status = bool(self.config.tool.expose_status_tool)

        # 这一路依赖 SDK 组件字典的键名（type / name / metadata / chat_scope），
        # 宿主若改键名，群聊限定与激发信号注入会静默失效。真正兜底的是「群白名单
        # 在调用时校验 + 拿不到 group_id 就拒绝」，因此失效也不会误发码，最坏表现
        # 是「私聊里也能看见这个工具」。这里统计一下实际改写到的组件数，数量为 0
        # 时给出提示，免得那种退化悄无声息。
        touched = 0
        synced_tool_count = 0

        # 深拷贝后再改：super().get_components() 返回的结构可能被 SDK 缓存复用，
        # 就地改写会污染 SDK 侧的状态（也可能影响其他插件看到的同一份对象）。
        result: List[Dict[str, Any]] = []
        for component in components:
            if not isinstance(component, dict):
                result.append(component)
                continue
            item = copy.deepcopy(component)
            if item.get("type") == "TOOL":
                synced_tool_count += 1
                # 关掉余量查询时直接把组件摘掉，而不是留着它「调用即报错」：
                # 挂着一个永远失败的工具会白占大模型的工具列表，也可能诱导它去调。
                # 注意这属于注册期行为，改动后需重载插件才生效。
                if item.get("name") == "redeem_code_status" and not expose_status:
                    del item
                    continue
                item["chat_scope"] = "group"
                touched += 1
                if hint_line and item.get("name") == "redeem_code_issue":
                    metadata = item.get("metadata")
                    if isinstance(metadata, dict):
                        base = str(metadata.get("description") or "")
                        metadata["description"] = f"{base}\n{hint_line}" if base else hint_line
            result.append(item)

        if hint_line and synced_tool_count == 0:
            logger.warning(
                "没有匹配到任何 TOOL 组件：激发信号未能注入到工具描述里。"
                "可能是宿主的组件结构发生了变化，请检查 get_components 里依赖的键名。"
            )
        elif synced_tool_count and touched == 0:
            logger.warning(
                "匹配到 %d 个 TOOL 组件但没有一个被加上 chat_scope："
                "宿主的组件结构可能已变化，群聊限定可能失效。",
                synced_tool_count,
            )
        return result

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

        # 内存身份路径不经过时间窗校验，其正确性依赖「chat.receive.after_process
        # 在本次工具调用之前触发」这一时序。该时序在当前宿主版本成立（bot.py 中
        # 钩子在 register_message 之前），但它不是宿主承诺的契约。内存窗口一旦
        # 明显宽于兜底时间窗，时序万一变化就会静默退化成「窗口期内最后说过话的人」。
        # 这里在装载时做一次低成本提示，避免那种退化悄无声息。
        ttl = max(int(self.config.tool.speaker_ttl_seconds), 1)
        window = int(self.config.tool.identity_fallback_window_seconds)
        if window > 0 and ttl > window * 3:
            logger.warning(
                "内存身份窗口（%d 秒）远宽于兜底时间窗（%d 秒）：内存路径不经过时间窗校验，"
                "若宿主调整了 chat.receive.after_process 的触发时序，身份判定会比带校验的"
                "兜底路径更松。建议把 speaker_ttl_seconds 收敛到与兜底同量级。",
                ttl,
                window,
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
        incoming_records: List[str] = []
        if isinstance(pool, dict):
            raw_available = pool.get("available_codes")
            incoming_text = "" if raw_available is None else str(raw_available)
            raw_records = pool.get("used_codes")
            if isinstance(raw_records, (list, tuple)):
                incoming_records = [str(item).strip() for item in raw_records if str(item).strip()]

        # 自己写回引发的回声：内存状态已经是最新的，跳过重新装载，
        # 避免把刚发放的兑换码重新算回未使用。
        # 注意这里同时比对 used_codes，而不只是 available_codes：只比对后者的话，
        # 当池子恰好没变化（例如只剩 0 个码时又发了一次），一次「只改了 used_codes」
        # 的真实配置更新会被误判成回声而跳过。
        if self._last_written_codes_text is not None and incoming_text == self._last_written_codes_text:
            if incoming_records == self._used_records:
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
            self._prune_recent_speakers()
        except Exception:
            logger.debug("记录最近发言人失败", exc_info=True)
        return {"action": "continue"}

    def _prune_recent_speakers(self) -> None:
        """给「最近发言人」记录做上限保护，避免这个字典随会话数无限增长。

        一个长期运行的实例会见过任意多个会话，如果只在读取时惰性删除，没被再次
        查询的会话就会一直留在内存里。这里在写入路径上顺手清理：先删掉已过期的，
        若仍然超限，再按记录时刻淘汰最旧的那些。
        """

        if len(self._recent_speakers) <= MAX_RECENT_SPEAKERS:
            return
        ttl = max(int(self.config.tool.speaker_ttl_seconds), 1)
        now = time.monotonic()
        for key in [key for key, record in self._recent_speakers.items() if now - record[2] > ttl]:
            self._recent_speakers.pop(key, None)

        overflow = len(self._recent_speakers) - MAX_RECENT_SPEAKERS
        if overflow > 0:
            oldest = sorted(self._recent_speakers.items(), key=lambda kv: kv[1][2])[:overflow]
            for key, _ in oldest:
                self._recent_speakers.pop(key, None)

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

        群聊里工具拿不到发起人 QQ，所以先用观察型 Hook 留下的内存记录；内存里没有
        有效记录时（例如插件刚重启），再回退到数据库，但这时的依据是「该会话最后一
        条消息的发送者」——它未必就是本次请求者。所以兜底必须加时间窗：只有当这条
        消息足够新、与本次调用在时间上对得上时才采信，否则保守拒绝。

        不加时间窗的后果是：群里管理员刚说过话（内存记录又恰好失效），紧接着某个
        非管理员说一句「发个兑换码」，就会被认成管理员而领到码。

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

        window = int(self.config.tool.identity_fallback_window_seconds)
        if window <= 0:
            # 兜底被显式关闭：身份只认内存记录。
            logger.info("身份兜底已禁用且内存无有效记录，保守拒绝: session_id=%s", session_id)
            return None

        try:
            limit = max(int(self.config.tool.recent_message_limit), 1)
            # filter_mai=True：把麦麦自己发的消息排除掉。宿主该参数的默认值是把
            # 麦麦自己的消息也算进来的，而兜底逻辑取的是「最后一条消息的发送者」——
            # 麦麦刚回复过的话，取到的就是麦麦自己（其 QQ 不在管理员白名单里），
            # 于是兜底会稳定地失败、形同虚设。显式过滤掉才是「谁在跟麦麦说话」。
            recent = await self.ctx.message.get_recent(session_id, limit=limit, filter_mai=True)
        except TypeError:
            # 宿主/ SDK 版本不支持该参数时退回不传，行为与旧版一致。
            try:
                recent = await self.ctx.message.get_recent(session_id, limit=limit)
            except Exception:
                logger.error("查询最近消息失败，无法确认请求者: session_id=%s", session_id, exc_info=True)
                return None
        except Exception:
            logger.error("查询最近消息失败，无法确认请求者: session_id=%s", session_id, exc_info=True)
            return None

        if not isinstance(recent, list):
            return None

        # find_messages 在 limit_mode="latest" 下仍按时间正序返回，所以从后往前找。
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

            # 时间窗校验：序列化后的 timestamp 是「秒级浮点时间戳的字符串」。
            age = self._message_age_seconds(item.get("timestamp"))
            if age is None or age > window:
                logger.info(
                    "身份兜底失败：会话最后一条消息已过期或时间不可读（age=%s，上限 %s 秒），保守拒绝 session_id=%s",
                    "未知" if age is None else f"{age:.1f}",
                    window,
                    session_id,
                )
                return None

            group_info = info.get("group_info")
            group_id = ""
            if isinstance(group_info, dict):
                group_id = str(group_info.get("group_id") or "").strip()
            return (group_id or group_id_hint, user_id)
        return None

    @staticmethod
    def _message_age_seconds(timestamp: Any) -> Optional[float]:
        """把消息的 ``timestamp`` 换算成「距今多少秒」。

        宿主序列化消息时把时间写成了秒级浮点时间戳的字符串（见
        ``PluginMessageUtils._session_message_to_dict``），但字段类型在不同版本间
        未必稳定，因此这里同时接受数字、数字字符串与可解析的文本时间。

        Args:
            timestamp: 消息里的 timestamp 字段。

        Returns:
            Optional[float]: 距今秒数；无法解析时返回 ``None``（调用方据此保守拒绝）。
        """

        if timestamp is None:
            return None
        try:
            if isinstance(timestamp, (int, float)):
                value = float(timestamp)
            else:
                text = str(timestamp).strip()
                if not text:
                    return None
                try:
                    value = float(text)
                except ValueError:
                    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                    value = parsed.timestamp()
        except (TypeError, ValueError, OSError, OverflowError):
            return None

        if value <= 0:
            return None
        # 毫秒级时间戳兜底：换算成秒，避免被误判成「未来」。
        if value > 1e11:
            value /= 1000.0

        age = time.time() - value
        if age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            # 明显来自未来的时间戳：可能是格式误判或时钟有问题。直接当作不可读，
            # 让调用方保守拒绝——否则「未来」会被 clamp 成 0 秒，恰好落进时间窗而
            # 被当成「刚发的消息」放行。
            return None
        # 轻微的时钟漂移（几秒内）不算问题，一并归零。
        return max(age, 0.0)

    # ===== 兑换码池维护 =====

    def _reload_pool_from_config(self) -> None:
        """按当前配置重建内存中的兑换码池与已使用记录。

        这里加了一道**单调保护**，用来兜住「配置页陈旧保存」：管理员先打开配置页、
        插件在这期间发放并写盘、管理员随后保存页面上的任意字段，如果宿主是按页面
        里的旧模型整份写回，那么 ``available_codes`` 会连已发出的码一起回来、
        ``used_codes`` 也会被旧值覆盖。照单全收的话「一个码只发一次」和发放记录
        会同时报废，重载后同一个码还能再发一次。

        保护规则有两条：
        1. 已使用记录**只增不减**：新配置里的记录与内存里的求并集，绝不因为一次
           陈旧保存而丢掉发放历史；
        2. 待发放池**剔除**所有已发放过的码，因此即使旧模型把码写回了
           ``available_codes``，它也复活不了。

        第 2 条依据的是 ``_issued_codes`` 这个**不随展示记录裁剪**的集合：
        展示用的 ``_used_records`` 只保留最近 ``MAX_USED_RECORDS`` 条，若只从它
        推断已发码，被裁掉的那些码就脱离了保护，陈旧保存时仍能复活。因此集合在
        装载时把「配置里的记录」与「上一轮的集合」都并进来，只增不减。

        代价是：管理员在配置页里**手动删掉**某条已使用记录并不会让那个码回到
        待发放池。这是有意的——「一个码只发一次」比「能手工回收」更重要。
        """

        incoming_codes = parse_code_lines(self.config.pool.available_codes)
        incoming_records = [str(item).strip() for item in self.config.pool.used_codes if str(item).strip()]

        # 并集：内存里已有的记录排在前（它们在时间上更新），新来的补充在后。
        merged_records = list(self._used_records)
        for record in incoming_records:
            if record not in merged_records:
                merged_records.append(record)
        if len(merged_records) > MAX_USED_RECORDS:
            del merged_records[MAX_USED_RECORDS:]

        # 已发码集合只增不减：持久化的哈希 + 上一轮集合 + 仍在记录里的 + 被裁掉的历史。
        self._issued_hashes |= {str(item).strip() for item in self.config.pool.issued_code_hashes if str(item).strip()}
        self._issued_hashes |= {_code_hash(code) for code in self._collect_used_codes(incoming_records)}
        self._issued_hashes |= {_code_hash(code) for code in self._collect_used_codes(merged_records)}

        resurrected = [code for code in incoming_codes if _code_hash(code) in self._issued_hashes]
        available = [code for code in incoming_codes if _code_hash(code) not in self._issued_hashes]

        self._available_codes = available
        self._used_records = merged_records
        self._last_written_codes_text = None

        if resurrected:
            logger.warning(
                "配置里的兑换码池含有 %d 个已发放过的码，已剔除以免重复发放（可能是配置页陈旧保存）",
                len(resurrected),
            )

    @staticmethod
    def _collect_used_codes(records: List[str]) -> set[str]:
        """从已使用记录里取出所有已发放的兑换码。

        记录格式见 :meth:`_format_used_record`：``时间 | 码:"XXX" | 群:... | QQ:...``。
        兑换码用 JSON 字符串包裹，因此即使码本身含 ``|`` 或 ``:`` 也能精确定位。
        旧格式（未加引号）的记录也一并兼容，避免升级后老记录失去保护作用。

        Args:
            records: 已使用记录文本列表。

        Returns:
            set[str]: 已发放过的兑换码集合。
        """

        codes: set[str] = set()
        for record in records:
            match = _USED_CODE_RE.search(record)
            if match is None:
                continue
            raw = match.group(1)
            if raw.startswith('"'):
                try:
                    code = json.loads(raw)
                except (ValueError, TypeError):
                    continue
            else:
                code = raw
            code = str(code).strip()
            if code:
                codes.add(code)
        return codes

    def _issue_one_code(self, group_id: str, user_id: str) -> Tuple[Optional[str], int, bool]:
        """随机取出一个未使用的兑换码并记录使用情况。

        取码与写盘在同一段同步逻辑里完成，期间没有 await，因此并发触发时
        不会把同一个兑换码发两次。

        关于「在事件循环里做同步写盘」：这是有意为之。把这个方法丢进线程池可以让出
        事件循环，但那样「取码 → 写盘 → 失败回滚」就不再是不可分割的一步，两个并发
        请求可能交错地取到同一个码，而「一个码只发一次」正是本插件的核心不变量。
        配置文件很小（一次读 + 一次写，量级在几十 KB 以内），发码又是低频操作，
        阻塞可忽略；用异步换来的那点吞吐不值得拿正确性去赌。因此这里保持同步，
        由 :data:`_CONFIG_WRITE_LOCK` 串行化写回。

        写盘成功才算发放成功。写盘失败时会把内存状态整体回滚到发放前的样子，
        并让调用方拒绝发放——否则磁盘上的 ``available_codes`` 仍含这个码，
        插件重载后会再发一次。

        Args:
            group_id: 领取人所在的群号。
            user_id: 领取人的 QQ 号。

        Returns:
            Tuple[Optional[str], int, bool]: 本次发出的兑换码（发完或写盘失败时为
            ``None``）、发放后剩余的兑换码数量，以及是否发放成功。
        """

        if not self._available_codes:
            return None, 0, False

        # 记下发放前的状态，写盘失败时用于回滚。
        previous_codes = list(self._available_codes)
        previous_records = list(self._used_records)
        previous_hashes = set(self._issued_hashes)

        index = random.randrange(len(self._available_codes))
        code = self._available_codes.pop(index)
        self._issued_hashes.add(_code_hash(code))
        self._used_records.insert(0, self._format_used_record(code, group_id, user_id))
        # 只保留最近 MAX_USED_RECORDS 条，避免配置文件无限膨胀。
        if len(self._used_records) > MAX_USED_RECORDS:
            del self._used_records[MAX_USED_RECORDS:]

        if not self._persist_pool():
            self._available_codes = previous_codes
            self._used_records = previous_records
            self._issued_hashes = previous_hashes
            return None, len(self._available_codes), False

        return code, len(self._available_codes), True

    @staticmethod
    def _format_used_record(code: str, group_id: str, user_id: str) -> str:
        """生成一条已使用记录的展示文本。

        兑换码用 JSON 字符串包裹（``json.dumps``），这样即使码本身含 ``|`` 或 ``:``
        也仍能被 :data:`_USED_CODE_RE` 精确抠出来——否则按 ``|`` 切分会把码截断，
        单调保护就会漏掉这个码，陈旧保存时它还能复活。旧格式记录同样被兼容。

        Args:
            code: 已发放的兑换码。
            group_id: 领用的群号。
            user_id: 领用的 QQ 号。

        Returns:
            str: 形如 ``2026-10-09 21:30:00 | 码:"XXX" | 群:123 | QQ:456`` 的记录。
        """

        used_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"{used_at} | 码:{json.dumps(code, ensure_ascii=False)} | 群:{group_id or '-'} | QQ:{user_id or '-'}"

    def _persist_pool(self) -> bool:
        """把内存中的兑换码池写回插件自己的 config.toml。

        优先在磁盘上已有的文档上就地改 ``[pool]`` 两个字段，这样用户在配置页里
        写的注释与排版都能保住；只有在文件不存在或解析失败时，才退回「以内存中的
        完整配置为骨架」重建整份文档——那条路径能保证写回结果完整且可再次加载，
        尤其能保证 ``[plugin]`` 节不会丢失（缺少该节会触发宿主的
        ``PluginConfigVersionError`` 而使插件无法加载）。

        按宿主约定不在插件目录创建任何临时文件。

        Returns:
            bool: 是否成功写盘。调用方必须据此决定要不要把兑换码发出去——写盘失败
            却照样发码的话，磁盘上的 ``available_codes`` 还留着这个码，插件重载后
            会把它再发一次，同一个码就发了两次。
        """

        codes_text = dump_code_lines(self._available_codes)
        config_path = PLUGIN_DIR / CONFIG_FILENAME

        with _CONFIG_WRITE_LOCK:
            try:
                document = self._load_document_for_write(config_path)
                pool_table = document.get("pool")
                if pool_table is None:
                    pool_table = tomlkit.table()
                    document["pool"] = pool_table
                pool_table["available_codes"] = codes_text
                pool_table["used_codes"] = list(self._used_records)
                # 已发码哈希集合也落盘，否则插件重载后这份保护就丢了——
                # 而恰恰是「重载 + 配置页陈旧保存」的组合能复活已发出的码。
                pool_table["issued_code_hashes"] = sorted(self._issued_hashes)

                with open(config_path, "w", encoding="utf-8") as handle:
                    handle.write(tomlkit.dumps(document))
            except Exception:
                logger.error("写回兑换码池到 %s 失败", config_path, exc_info=True)
                return False

        self._last_written_codes_text = codes_text
        return True

    def _load_document_for_write(self, config_path: Path) -> Any:
        """取用于写回的 TOML 文档：能读到现有文件就沿用它的格式。

        宿主在某些时机可能会把配置里的键名规范化（例如下划线键），这类改写不影响
        我们写入的两个字段，所以直接沿用磁盘上的文档是安全的——它同样是完整可加载
        的。若文件不存在（插件首次发放、宿主尚未落盘）或内容已损坏到解析不了，再
        退回用内存配置重建。

        Args:
            config_path: 插件配置文件路径。

        Returns:
            Any: tomlkit 文档对象。
        """

        try:
            existing_text = config_path.read_text(encoding="utf-8")
        except OSError:
            existing_text = ""
        except Exception:
            logger.debug("读取 %s 失败，改用内存配置重建文档", config_path, exc_info=True)
            existing_text = ""

        if existing_text.strip():
            try:
                return tomlkit.parse(existing_text)
            except Exception:
                logger.warning("解析 %s 失败，改用内存配置重建文档", config_path, exc_info=True)

        return tomlkit.parse(tomlkit.dumps(self.get_plugin_config_data()))

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

        code, remaining, issued = self._issue_one_code(resolved_group_id, user_id)
        if not issued:
            if code is not None:  # pragma: no cover - 失败路径下 code 恒为 None
                logger.error("兑换码发放状态异常：写盘失败却带回了兑换码")
            if self._available_codes:
                # 池子里还有码，说明是写盘失败而不是发完了。
                logger.error(
                    "发放兑换码失败：兑换码池写盘未成功，已拒绝本次发放 session_id=%s",
                    session_id,
                )
                return {"success": False, "error": "兑换码暂时发不出去，请稍后再试。"}
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
            "查询当前还有多少兑换码可发。当有人询问「还剩多少兑换码」「兑换码还有吗」时使用。"
            "本工具不消耗兑换码，只会返回一个数量。"
            "本工具需要与领码相同的权限，无权限时会被回绝，届时把回绝文案原样转述即可。"
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
        """报告兑换码余量。

        权限校验与发码工具完全一致：先确认请求者身份，再校验群白名单与管理员
        白名单。只返回数量，**不回显任何已发放的兑换码**——兑换码本身就是凭据，
        回显等于把发出去的码再送一次，顺带泄露领取人的群号与 QQ。

        Args:
            stream_id: 宿主注入的会话 ID。
            chat_id: 宿主注入的会话 ID（与 ``stream_id`` 同义，兼容不同调用路径）。
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
            return {"success": False, "error": "无法确定当前会话。"}

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
        logger.info("已响应 QQ:%s（群:%s）的余量查询，剩余 %d 个", user_id, resolved_group_id, remaining)
        return {"success": True, "content": f"当前还有 {remaining} 个兑换码可发。"}

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
