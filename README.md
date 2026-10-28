# Kener 维护推送插件 (kener_maintenance_bot)

把 [Kener](https://github.com/rajnandan1/kener) 状态页的**维护事件**自动推送到多个 QQ 群。

## 功能

| 事件 | 推送文案 |
|---|---|
| 维护事件首次出现(`SCHEDULED` / `READY`) | 📅 新维护计划 |
| 维护开始前 N 分钟(`SCHEDULED` / `READY`) | ⏰ 维护即将开始 |
| 事件进入 `ONGOING` | 🚧 维护开始 |
| 事件进入 `COMPLETED` | ✅ 维护结束 |
| 事件进入 `CANCELLED` | ❌ 维护取消 |

- 轮询 Kener `GET /api/v4/maintenances/events`,4 种状态各拉一次,合并去重。
- 用 AstrBot 插件 KV 持久化事件状态(含"是否已提前提醒"),重启不重复推送。
- 支持 `watch_monitors` 过滤只关心特定 monitor。
- 支持 `backfill_on_start` 启动时补推当前进行中的维护。
- 支持 `enable_start_reminder` + `reminder_lead_minutes` 在维护开始前 N 分钟推送一次提醒。
- 连续失败指数退避(最长 10 分钟),避免 Kener 抖动时刷屏日志。

## 安装

1. 把整个目录克隆/复制到 `AstrBot/data/plugins/kener_maintenance_bot/`。
2. AstrBot WebUI → 插件 → 重载(或重启 AstrBot)。
3. 在插件配置页填:
   - `kener_base_url`: 例如 `https://status.ets2la.cn`
   - `kener_api_key`: `kener_` 开头的 API Key(在 Kener 管理后台 → API Keys 生成)
   - `push_targets`: 添加一个或多个 QQ 群号
4. 启用插件。

## 配置项

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `enabled` | bool | `true` | 总开关 |
| `kener_base_url` | string | — | Kener 站点地址 |
| `kener_api_key` | string(secret) | — | Bearer Token |
| `push_targets` | template_list | `[]` | 推送目标,每项 `{target_id, platform}` |
| `poll_interval` | int | `60` | 轮询间隔(秒) |
| `watch_monitors` | string | `""` | 逗号分隔的 monitor tag 白名单,留空 = 全部 |
| `backfill_on_start` | bool | `false` | 启动时补推 ONGOING |
| `enable_start_reminder` | bool | `true` | 维护开始前推送提前提醒 |
| `reminder_lead_minutes` | int | `30` | 提前多少分钟提醒,`0` = 关闭 |

### 提前提醒机制

- 触发条件:事件状态为 `SCHEDULED` 或 `READY`,且 `now >= 开始时间 - reminder_lead_minutes`,且 `now < 开始时间`。
- 每个 `event_id` 只提醒一次,标志随缓存写入 KV,AstrBot 重启后不会重复。
- 若事件在提醒前就进入 `ONGOING`/`CANCELLED`/`COMPLETED`,则不再补发提醒(避免刷屏)。

## 调试指令

群里发(需要 `only_command` 默认前缀,通常是 `/` 或 `!`,看你 AstrBot 配置):

- `/kener维护` — 手动拉一次 Kener,显示前 5 条事件
- `/kener缓存` — 查看本地事件状态缓存

## 目录结构

```
kener_maintenance_bot/
├── metadata.yaml        # 插件元数据
├── _conf_schema.json    # 可视化配置 schema
├── requirements.txt     # 依赖(httpx)
├── main.py              # 插件主逻辑
└── README.md
```

## 注意事项

- **时间**:Kener 所有时间戳是 UTC 秒,插件渲染时手动 +8 小时显示北京时间。
- **API Key 失效**:Kener 后台禁用 Key 后,插件会打 `401` 日志并指数退避,不会崩溃。
- **一次性维护**:Kener 里 `rrule: FREQ=MINUTELY;COUNT=1` 是"一次性维护"的惯用写法,本插件对它和周期性维护一视同仁。
- **平台**:目前只适配 OneBot 系(`aiocqhttp` / `qq_official` / `qq_official_webhook`),走 `send_group_message`。如果要支持 Telegram / Discord 频道,需要在 `_send_to_target` 里加分支。

## 灵感 / 参考

- Kener v4 API 文档:见 Kener 仓库 `src/routes/(api)/api/v4/`
- AstrBot 插件开发:https://docs.astrbot.app/dev/star/plugin-new.html
