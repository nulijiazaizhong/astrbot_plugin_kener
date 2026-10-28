"""Kener 维护事件推送插件。

轮询 Kener /api/v4/maintenances/events,对"新出现的事件"以及
事件状态进入 ONGOING / COMPLETED / CANCELLED 的迁移,
自动向配置的多个 QQ 群推送通知。

推送时机:
  1. 事件首次出现(任意非 CANCELLED 状态) → "新维护计划"
  2. 定时提醒: now >= start - lead 分钟      → "维护即将开始"(每个事件只推一次)
  3. 事件进入 ONGOING                       → "维护开始"
  4. 事件进入 COMPLETED                     → "维护结束"
  5. 事件进入 CANCELLED(且之前不是)        → "维护取消"

状态用插件 KV 持久化,重启 AstrBot 不会重复推送。
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Optional

import httpx

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star


# Kener 维护事件状态
ST_SUCCEEDED = ("COMPLETED", "CANCELLED")
ST_ACTIVE = ("ONGOING",)
ST_PENDING = ("SCHEDULED", "READY")
ST_ALL = ST_ACTIVE + ST_PENDING + ST_SUCCEEDED


def _fmt_ts(ts: int, tz_offset_hours: int = 8) -> str:
    """UTC 秒 → 'YYYY-MM-DD HH:MM'(默认北京时间)。"""
    try:
        ts = int(ts)
    except (TypeError, ValueError):
        return "未知时间"
    # 手动加时区偏移,避免依赖系统时区
    import datetime as _dt

    utc = _dt.datetime.fromtimestamp(ts, _dt.timezone.utc)
    local = utc + _dt.timedelta(hours=tz_offset_hours)
    return local.strftime("%Y-%m-%d %H:%M")


def _fmt_duration(seconds: int) -> str:
    if not seconds or seconds <= 0:
        return "未知时长"
    if seconds < 60:
        return f"{seconds} 秒"
    if seconds < 3600:
        return f"{seconds // 60} 分钟"
    h, r = divmod(seconds, 3600)
    m = r // 60
    if m:
        return f"{h} 小时 {m} 分钟"
    return f"{h} 小时"


# impact 英文 → 中文映射
IMPACT_ZH = {
    "UP": "恢复",
    "DOWN": "宕机",
    "DEGRADED": "降级",
    "MAINTENANCE": "维护中",
}


def _fmt_monitors(monitors: list[dict], details: Optional[dict] = None) -> str:
    """把 Kener 返回的 monitors 列表渲染成多行节点信息。

    details: {monitor_tag: {name, category_name, description, ...}} 可选,
    提供后每行显示"节点名 [分类] (影响)"。
    """
    if not monitors:
        return "(未指定节点)"
    lines = []
    for m in monitors:
        tag = m.get("monitor_tag", "?")
        impact = (m.get("impact") or "MAINTENANCE").upper()
        impact_zh = IMPACT_ZH.get(impact, impact)
        detail = (details or {}).get(tag) or {}
        name = detail.get("name") or tag
        category = detail.get("category_name") or ""
        desc = (detail.get("description") or "").strip()
        # 单行: 节点名 [分类] (影响)
        line = f"  • {name}"
        if category and category != name:
            line += f"  [{category}]"
        line += f"  ({impact_zh})"
        lines.append(line)
    return "\n".join(lines)


class KenerClient:
    """极简 Kener v4 API 客户端(只用 httpx 异步)。"""

    def __init__(self, base_url: str, api_key: str):
        # 去掉末尾斜杠,避免拼接出双斜杠
        self.base = base_url.rstrip("/")
        self.client = httpx.AsyncClient(
            base_url=self.base,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10.0,
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def get_events(
        self,
        event_status: Optional[str] = None,
        monitors: Optional[str] = None,
        start_ts: Optional[int] = None,
        limit: int = 100,
        page: int = 1,
    ) -> list[dict]:
        params: dict[str, Any] = {"limit": limit, "page": page}
        if event_status:
            params["event_status"] = event_status
        if monitors:
            params["monitors"] = monitors
        if start_ts is not None:
            params["event_start_date_time"] = start_ts
        r = await self.client.get("/api/v4/maintenances/events", params=params)
        r.raise_for_status()
        return r.json().get("events", []) or []

    async def get_maintenance(self, maintenance_id: int) -> Optional[dict]:
        r = await self.client.get(f"/api/v4/maintenances/{maintenance_id}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        # Kener 这个端点返回 {"maintenance": {...}}
        return data.get("maintenance") or data

    async def get_monitor(self, tag: str) -> Optional[dict]:
        """拉单个 monitor 的完整信息(name / category_name / description 等)。"""
        r = await self.client.get(f"/api/v4/monitors/{tag}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        return data.get("monitor") or data


class KenerMaintenanceBot(Star):
    """Kener 维护事件推送。"""

    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config
        self.name = "kener_maintenance_bot"

        # 运行状态
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._http: Optional[KenerClient] = None

        # 事件状态缓存: event_id -> {"status": str, "maintenance_id": int, "title": str, ...}
        # 用 KV 持久化,重启不丢
        self._cache: dict[int, dict] = {}

        # monitor 详情缓存: tag -> {name, category_name, description, ...}
        # monitor 元数据基本不变,进程内缓存即可,不用持久化
        self._monitor_details: dict[str, dict] = {}

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """AstrBot 加载插件后调用。"""
        if not self._enabled():
            logger.info("[kener-maintenance] 插件已禁用(enabled=false),不启动轮询")
            return
        await self._load_cache()
        if self.config.get("backfill_on_start"):
            await self._backfill_ongoing()
        self._task = asyncio.create_task(self._loop(), name="kener-maintenance-loop")
        logger.info(
            "[kener-maintenance] 轮询已启动, 间隔={}s, 目标数={}".format(
                self.config.get("poll_interval", 60),
                len(self._push_targets()),
            )
        )

    async def terminate(self) -> None:
        """插件卸载/停用时调用。"""
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._http:
            await self._http.close()
            self._http = None
        logger.info("[kener-maintenance] 轮询已停止")

    # ------------------------------------------------------------------ #
    # 配置读取
    # ------------------------------------------------------------------ #

    def _enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    def _push_targets(self) -> list[dict]:
        """返回 [{"target_id": "123", "platform": "aiocqhttp"}, ...]"""
        raw = self.config.get("push_targets") or []
        out = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            tid = str(item.get("target_id", "")).strip()
            if not tid:
                continue
            out.append(
                {
                    "target_id": tid,
                    "platform": item.get("platform", "aiocqhttp"),
                }
            )
        return out

    def _watch_monitors(self) -> Optional[str]:
        s = (self.config.get("watch_monitors") or "").strip()
        return s or None

    def _reminder_enabled(self) -> bool:
        """维护开始前的定时提醒开关。"""
        return bool(self.config.get("enable_start_reminder", True))

    def _reminder_lead_seconds(self) -> int:
        """提前多少秒提醒,负数视为关闭。"""
        try:
            m = int(self.config.get("reminder_lead_minutes", 30))
        except (TypeError, ValueError):
            m = 30
        return max(0, m) * 60

    # ------------------------------------------------------------------ #
    # KV 缓存(重启不丢)
    # ------------------------------------------------------------------ #

    async def _load_cache(self) -> None:
        try:
            raw = await self.get_kv_data("event_cache", "{}")
            data = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(data, dict):
                # KV 里 key 会被转成 str,这里统一转回 int
                self._cache = {int(k): v for k, v in data.items() if isinstance(v, dict)}
        except Exception as e:
            logger.warning(f"[kener-maintenance] 读取事件缓存失败: {e}")
            self._cache = {}

    async def _save_cache(self) -> None:
        try:
            await self.put_kv_data(
                "event_cache",
                json.dumps(self._cache, ensure_ascii=False),
            )
        except Exception as e:
            logger.warning(f"[kener-maintenance] 保存事件缓存失败: {e}")

    # ------------------------------------------------------------------ #
    # 主循环
    # ------------------------------------------------------------------ #

    async def _loop(self) -> None:
        interval = max(10, int(self.config.get("poll_interval", 60)))
        consecutive_errors = 0
        while not self._stop.is_set():
            try:
                if not self._enabled():
                    await self._sleep(interval)
                    continue
                await self._poll_once()
                consecutive_errors = 0
            except httpx.HTTPStatusError as e:
                consecutive_errors += 1
                logger.warning(
                    f"[kener-maintenance] HTTP {e.response.status_code} "
                    f"(连续 {consecutive_errors} 次)"
                )
                if e.response.status_code == 401:
                    logger.error(
                        "[kener-maintenance] API Key 无效或被禁用,请在 Kener 后台检查"
                    )
            except Exception as e:
                consecutive_errors += 1
                logger.warning(
                    f"[kener-maintenance] 轮询异常(连续 {consecutive_errors} 次): {e}"
                )
            # 连续失败时指数退避,最多 10 分钟
            wait = interval if consecutive_errors == 0 else min(
                interval * (2 ** min(consecutive_errors, 5)), 600
            )
            await self._sleep(wait)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------ #
    # 轮询逻辑
    # ------------------------------------------------------------------ #

    async def _ensure_http(self) -> KenerClient:
        if self._http is None:
            base = (self.config.get("kener_base_url") or "").strip()
            key = (self.config.get("kener_api_key") or "").strip()
            if not base or not key:
                raise RuntimeError("kener_base_url / kener_api_key 未配置")
            self._http = KenerClient(base, key)
        return self._http

    async def _fetch_all_events(self) -> list[dict]:
        """拉取所有相关状态的事件,合并去重。"""
        http = await self._ensure_http()
        watches = self._watch_monitors()
        now = int(time.time())
        # 回溯 7 天,足够覆盖大多数维护窗口
        start_ts = now - 7 * 86400

        merged: dict[int, dict] = {}
        for status in ST_ALL:
            try:
                events = await http.get_events(
                    event_status=status,
                    monitors=watches,
                    start_ts=start_ts,
                    limit=100,
                )
            except Exception as e:
                logger.warning(f"[kener-maintenance] 拉取 {status} 事件失败: {e}")
                continue
            for ev in events:
                eid = ev.get("event_id")
                if eid is None:
                    continue
                ev["_fetched_status"] = status
                merged[int(eid)] = ev
        return list(merged.values())

    async def _poll_once(self) -> None:
        events = await self._fetch_all_events()
        new_cache: dict[int, dict] = {}
        notifications: list[tuple[str, dict]] = []  # (kind, event)
        now = int(time.time())
        lead = self._reminder_lead_seconds()

        for ev in events:
            eid = int(ev["event_id"])
            status = ev.get("event_status") or ev.get("_fetched_status") or ""
            prev = self._cache.get(eid)
            prev_status = prev.get("status") if prev else None

            # 记录本次状态(reminded 从上次缓存继承,默认 False)
            new_cache[eid] = {
                "status": status,
                "maintenance_id": ev.get("maintenance_id"),
                "maintenance_title": ev.get("maintenance_title"),
                "title": ev.get("maintenance_title"),
                "start": ev.get("event_start_date_time"),
                "end": ev.get("event_end_date_time"),
                "duration": ev.get("maintenance_duration_seconds"),
                "monitors": ev.get("monitors") or [],
                "url": ev.get("url"),
                "reminded": bool(prev.get("reminded")) if prev else False,
            }

            if prev is None:
                # 首次出现
                if status in ST_PENDING or status in ST_ACTIVE:
                    notifications.append(("new", ev))
            else:
                # 状态迁移
                if prev_status != status:
                    if status in ST_ACTIVE and prev_status in ST_PENDING:
                        notifications.append(("start", ev))
                    elif status == "COMPLETED" and prev_status not in ST_SUCCEEDED:
                        notifications.append(("end", ev))
                    elif status == "CANCELLED" and prev_status not in ST_SUCCEEDED:
                        notifications.append(("cancel", ev))

            # 定时提醒: 事件仍在 PENDING、已开启提醒、未提醒过、且进入提醒窗口
            if (
                self._reminder_enabled()
                and lead > 0
                and status in ST_PENDING
                and not new_cache[eid]["reminded"]
            ):
                start_ts = new_cache[eid].get("start") or 0
                if start_ts and now >= start_ts - lead and now < start_ts:
                    notifications.append(("start_reminder", ev))
                    new_cache[eid]["reminded"] = True

        # 清理太久远的缓存(30 天前结束/取消的),防止无限增长
        cutoff = int(time.time()) - 30 * 86400
        for eid, info in list(self._cache.items()):
            end_ts = info.get("end") or 0
            if info.get("status") in ST_SUCCEEDED and end_ts and end_ts < cutoff:
                self._cache.pop(eid, None)

        self._cache = new_cache
        await self._save_cache()

        for kind, ev in notifications:
            try:
                await self._push(kind, ev)
            except Exception as e:
                logger.error(f"[kener-maintenance] 推送失败 kind={kind}: {e}")

    # ------------------------------------------------------------------ #
    # monitor 详情(节点名 / 分类 / 描述)
    # ------------------------------------------------------------------ #

    async def _resolve_monitors(self, monitors: list[dict]) -> dict:
        """给 monitors 列表里的每个 tag 拉一次详情,返回 {tag: detail}。

        已缓存的 tag 直接命中,不重复请求。
        """
        if not monitors:
            return {}
        out: dict[str, dict] = {}
        http = await self._ensure_http()
        for m in monitors:
            tag = m.get("monitor_tag")
            if not tag:
                continue
            if tag in self._monitor_details:
                out[tag] = self._monitor_details[tag]
                continue
            try:
                detail = await http.get_monitor(tag)
            except Exception as e:
                logger.warning(f"[kener-maintenance] 拉取 monitor {tag} 失败: {e}")
                detail = None
            if detail:
                self._monitor_details[tag] = detail
                out[tag] = detail
            else:
                # 404 或拉取失败,放一个占位,避免反复重试
                self._monitor_details[tag] = {"name": tag, "category_name": "", "description": ""}
                out[tag] = self._monitor_details[tag]
        return out

    # ------------------------------------------------------------------ #
    # 启动补推
    # ------------------------------------------------------------------ #

    async def _backfill_ongoing(self) -> None:
        """启动时把当前 ONGOING 的事件推一次(配置开启时)。"""
        try:
            events = await self._fetch_all_events()
        except Exception as e:
            logger.warning(f"[kener-maintenance] 补推拉取失败: {e}")
            return
        pushed_any = False
        for ev in events:
            status = ev.get("event_status") or ""
            if status not in ST_ACTIVE:
                continue
            eid = int(ev.get("event_id", 0))
            prev = self._cache.get(eid)
            if prev and prev.get("status") == status:
                # 已经推过,不重复
                continue
            logger.info(f"[kener-maintenance] 补推 ONGOING 事件 #{eid}")
            await self._push("start", ev)
            pushed_any = True
        if pushed_any:
            await self._save_cache()

    # ------------------------------------------------------------------ #
    # 推送
    # ------------------------------------------------------------------ #

    async def _push(self, kind: str, ev: dict) -> None:
        targets = self._push_targets()
        if not targets:
            logger.info("[kener-maintenance] 未配置 push_targets,跳过推送")
            return
        # 拉受影响节点的详情(名称 / 分类 / 描述),渲染时用
        details = await self._resolve_monitors(ev.get("monitors") or [])
        text = self._render(kind, ev, details)
        logger.info(f"[kener-maintenance] 推送 kind={kind} 到 {len(targets)} 个群")
        for t in targets:
            try:
                await self._send_to_target(t, text)
            except Exception as e:
                logger.error(
                    f"[kener-maintenance] 发送到 {t['platform']}:{t['target_id']} 失败: {e}"
                )

    async def _send_to_target(self, target: dict, text: str) -> None:
        """通过 AstrBot 平台适配器主动发群消息。"""
        platform_name = target.get("platform", "aiocqhttp")
        target_id = target["target_id"]
        try:
            platform = self.context.get_platform_inst(platform_name)
        except Exception as e:
            logger.error(f"[kener-maintenance] 获取平台 {platform_name} 失败: {e}")
            return
        if platform is None:
            logger.error(f"[kener-maintenance] 平台 {platform_name} 未加载")
            return

        # 不同平台 API 略有差异,统一走 send_group_message(OneBot 系)
        # 或 send_channel_message(Discord/频道系)。这里只处理 QQ 系。
        sender = getattr(platform, "send_group_message", None)
        if sender is None:
            logger.error(
                f"[kener-maintenance] 平台 {platform_name} 不支持 send_group_message"
            )
            return
        await sender(group_id=int(target_id), message=text)

    # ------------------------------------------------------------------ #
    # 消息渲染
    # ------------------------------------------------------------------ #

    def _render_event_block(self, idx: int, ev: dict, details: Optional[dict] = None) -> str:
        """渲染单条事件的完整详情(调试指令用)。"""
        title = ev.get("maintenance_title") or "(无标题)"
        desc = ev.get("maintenance_description") or ""
        status = ev.get("event_status") or "UNKNOWN"
        start = ev.get("event_start_date_time")
        end = ev.get("event_end_date_time")
        duration = ev.get("maintenance_duration_seconds")
        monitors = ev.get("monitors") or []
        url = ev.get("url") or ""
        mid = ev.get("maintenance_id")
        eid = ev.get("event_id")
        plan_status = ev.get("maintenance_status") or ""

        lines = [f"--- [{idx}] 事件 #{eid} [{status}] ---"]
        lines.append(f"标题: {title}")
        if desc:
            lines.append(f"说明: {desc}")
        lines.append(f"开始: {_fmt_ts(start)} (北京时间)")
        if end:
            lines.append(f"结束: {_fmt_ts(end)} (北京时间)")
        if duration:
            lines.append(f"时长: {_fmt_duration(duration)}")
        if plan_status:
            lines.append(f"计划状态: {plan_status}")
        lines.append("影响节点:")
        lines.append(_fmt_monitors(monitors, details))
        if url:
            lines.append(f"详情: {url}")
        if mid is not None:
            lines.append(f"计划 ID: {mid}")
        return "\n".join(lines)

    def _render(self, kind: str, ev: dict, details: Optional[dict] = None) -> str:
        title = ev.get("maintenance_title") or "(无标题)"
        desc = ev.get("maintenance_description") or ""
        start = ev.get("event_start_date_time")
        end = ev.get("event_end_date_time")
        duration = ev.get("maintenance_duration_seconds")
        monitors = ev.get("monitors") or []
        url = ev.get("url") or ""
        mid = ev.get("maintenance_id")

        # 受影响节点(多行)
        monitors_block = _fmt_monitors(monitors, details)

        # 计算"还剩多久"提示(用于 start_reminder)
        now = int(time.time())
        remaining = ""
        if start:
            remain_s = int(start) - now
            if remain_s > 0:
                remaining = f" 距今约 {_fmt_duration(remain_s)}"

        lines: list[str] = []
        if kind == "new":
            lines.append("📅 新维护计划")
            lines.append(f"标题: {title}")
            if desc:
                lines.append(f"说明: {desc}")
            lines.append(f"开始: {_fmt_ts(start)} (北京时间)")
            if duration:
                lines.append(f"时长: {_fmt_duration(duration)}")
            lines.append("影响节点:")
            lines.append(monitors_block)
        elif kind == "start_reminder":
            lines.append("⏰ 维护即将开始")
            lines.append(f"标题: {title}")
            lines.append(f"开始: {_fmt_ts(start)} (北京时间){remaining}")
            if duration:
                lines.append(f"预计时长: {_fmt_duration(duration)}")
            if desc:
                lines.append(f"说明: {desc}")
            lines.append("影响节点:")
            lines.append(monitors_block)
        elif kind == "start":
            lines.append("🚧 维护开始")
            lines.append(f"标题: {title}")
            lines.append(f"开始: {_fmt_ts(start)} (北京时间)")
            if end:
                lines.append(f"预计结束: {_fmt_ts(end)} (北京时间)")
            lines.append("影响节点:")
            lines.append(monitors_block)
        elif kind == "end":
            lines.append("✅ 维护结束")
            lines.append(f"标题: {title}")
            if end:
                lines.append(f"结束: {_fmt_ts(end)} (北京时间)")
            lines.append("影响节点:")
            lines.append(monitors_block)
        elif kind == "cancel":
            lines.append("❌ 维护取消")
            lines.append(f"标题: {title}")
            lines.append(f"原计划: {_fmt_ts(start)} (北京时间)")
            lines.append("影响节点:")
            lines.append(monitors_block)
        else:
            lines.append(f"[{kind}] {title}")
            lines.append("影响节点:")
            lines.append(monitors_block)

        if url:
            lines.append(f"详情: {url}")
        if mid is not None:
            lines.append(f"计划 ID: {mid}")
        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    # 调试指令(可选)
    # ------------------------------------------------------------------ #

    @filter.command("kener维护", priority=10)
    async def cmd_kener_status(self, event: AstrMessageEvent):
        """手动触发一次 Kener 拉取,完整展示每条维护事件(调试用)。"""
        if not self._enabled():
            yield event.plain_result("插件已禁用(enabled=false)")
            return
        try:
            events = await self._fetch_all_events()
        except Exception as e:
            yield event.plain_result(f"拉取失败: {e}")
            return
        if not events:
            yield event.plain_result("当前没有任何维护事件")
            return

        # 按开始时间倒序,最近的在前
        events.sort(
            key=lambda e: e.get("event_start_date_time") or 0,
            reverse=True,
        )
        # 每条单独发一条消息,避免 QQ 单条消息长度限制(4000 字符)
        yield event.plain_result(f"共 {len(events)} 条维护事件,按时间倒序展示:")
        for i, ev in enumerate(events, 1):
            # 拉受影响节点的详情(名称/分类/描述)
            details = await self._resolve_monitors(ev.get("monitors") or [])
            block = self._render_event_block(i, ev, details)
            yield event.plain_result(block)

    @filter.command("kener缓存", priority=10)
    async def cmd_kener_cache(self, event: AstrMessageEvent):
        """查看本地事件状态缓存。"""
        if not self._cache:
            yield event.plain_result("缓存为空")
            return
        lines = [f"缓存 {len(self._cache)} 条:"]
        for eid, info in list(self._cache.items())[:10]:
            lines.append(
                "  #{} [{}] {} 结束={}".format(
                    eid,
                    info.get("status"),
                    (info.get("title") or "")[:20],
                    _fmt_ts(info.get("end") or 0),
                )
            )
        yield event.plain_result("\n".join(lines))
