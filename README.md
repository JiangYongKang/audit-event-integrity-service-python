# 审计事件摄取与合规评估服务

本地可运行的审计事件服务：对多来源审计事件进行**批量接收与规范化**、**哈希链完整性校验**、
**时间/容量双重保留**与**可配置合规规则评估**。纯 Python 标准库 + FastAPI，持久层为进程内存储
（接口按可替换设计），无需外部依赖即可运行与验证。

## 1. 快速开始

```bash
uv sync                       # 安装依赖（含 pytest/httpx 开发依赖）
uv run pytest                 # 运行全部 31 项测试（-s 打开，日志打印事件标识与判定依据）
uv run python examples/demo.py  # 端到端演示：幂等/拒绝原因/篡改/保留/规则隔离/背压
uv run python main.py         # 启动 HTTP 服务 http://127.0.0.1:8000 （/docs 为 Swagger）
```

## 2. 事件模型

入站事件（`EventInput` / HTTP 请求体 `events[]`）：

| 字段 | 含义 |
| --- | --- |
| `event_id` | 事件唯一标识；**幂等键**，同一标识重复投递不产生新记录 |
| `source`（事件体内嵌） | 事件数据自己声明的来源 |
| 顶层 `source`（投递信封 `declared_source`） | 通道声明的来源；与内嵌来源必须一致 |
| `event_type` | 事件类型 |
| `timestamp` | ISO-8601 时间戳；naive 时间按 UTC 解释，`Z` 后缀支持 |
| `payload` | 任意 JSON 对象，参与哈希 |
| `sequence`（可选） | 客户端序号；不给则由服务端分配 |

规范化后落库为 `StoredEvent`：解析后的 `timestamp`、单调 `sequence`、
`prev_hash` 与 `record_hash`。

### 摄取结果与可区分的拒绝原因

每条入站事件都有一条显式结果（**绝不静默丢弃**），拒绝原因互不混淆（`RejectReason`）：

| 原因 | 触发条件 |
| --- | --- |
| `MISSING_FIELD` | 必填字段缺失/空白，或 payload 不是对象（空时间戳也归此，先于格式校验） |
| `INVALID_TIMESTAMP` | 时间戳非 ISO-8601 或日期不存在（如 `2026-13-45T..`、`2026-02-30T..`） |
| `SOURCE_MISMATCH` | 信封声明来源与事件体内嵌来源不一致，或路由到错误来源的存储 |
| `UNKNOWN_SOURCE` | 配置了来源白名单时，事件来源未注册 |
| `DUPLICATE_EVENT` | 同 `event_id` 重放但**内容与首次不同**（冲突，非幂等成功） |
| `SEQUENCE_CONFLICT` | 客户端序号不等于期望值（重复/回退/跳号） |
| `CAPACITY_EXCEEDED` | 时间淘汰后仍达容量硬上限（背压，详见第 4 节） |

幂等语义：`event_id` 已存在且内容完全一致时返回 `accepted=true, duplicate=true`，
复用首次的序号与摘要，不新增记录、不推进序号。并发重放同一事件也只落一条记录。

### 并发与序号

每个来源一把独立锁：同来源并发写入串行化，跨来源互不阻塞。
序号在来源内**严格单调递增且永不复用**（即使事件被保留策略淘汰，
`_next_sequence` 仍只增）。客户端可携带序号做强校验，也可由服务端分配。

## 3. 完整性校验（哈希链）

每个来源维护一条哈希链（`audit_service/chain.py`）：

```
record_hash = SHA256( canonical_json({
    event_id, source, event_type, timestamp, sequence, payload, prev_hash
}) )
canonical_json = 键排序、无空白、ensure_ascii=False 的 JSON
链首 prev_hash = 64 个 0（GENESIS_HASH）
```

`GET /verify`（或 `AuditService.verify_all()`）从头重算每条记录并检查：

1. **字段摘要**：重算 `record_hash` 必须与存储一致 —— 任何字段的插入/删除/篡改
   （含 payload 内部改动、事件类型被改）都会在此暴露，返回 `ok=false`、
   `problem_event_id` 与具体 `problem`（`record_hash mismatch: data tampered or corrupted`）；
2. **前驱链接**：`prev_hash` 必须指向前一条（或锚点），重排/插入伪造记录导致
   `prev_hash linkage broken`；
3. **序号单调**：序号必须严格递增，否则报 `sequence not strictly increasing`。

### 淘汰后的确定结论（锚点机制）

时间保留淘汰链首记录时，链会记录**最后一条被淘汰记录的摘要与序号作为锚点**：

- 剩余记录仍按锚点继续校验，结论确定；
- 结果中 `evicted_count` 与锚点信息明确可解释（如
  `OK; 3 historical record(s) evicted before sequence 4, anchored at b140e1535913`）；
- 淘汰后篡改唯一剩余记录仍会被检出。

> 说明：本实现为进程内内存存储，直接改写对象即可模拟磁盘/外部篡改（测试与演示即如此做）。
> 哈希链设计与存储介质无关，迁移到持久存储时校验语义不变。

## 4. 保留策略（时间 + 容量双重上限）

`RetentionPolicy(max_age_seconds=..., max_count=...)`，语义：

- **时间上限（主动淘汰）**：摄取时（及 `POST /retention/run`）淘汰
  `timestamp < now - max_age_seconds` 的记录。淘汰集合始终是**链首连续前缀**，
  与哈希链锚点推进严格一致；遇到第一条未过期记录即停止。
- **容量上限（硬上限 / 背压）**：先做时间淘汰，再判定容量；若记录数已达 `max_count`，
  新事件以 `CAPACITY_EXCEEDED` **显式拒绝**（detail 提示背压），**绝不静默丢弃**。
  容量满时对历史事件的幂等重放仍然成功。时间淘汰空出槽位后新事件可正常进入。

选择“容量满即拒绝（背压）而非淘汰最新/最旧”是刻意的语义决定：审计场景下
淘汰只应由明确的保留时间策略驱动，容量压力以可区分的失败上抛给调用方处理
（重试/扩容/等待时间淘汰）。

## 5. 合规规则语义

规则通过 `PUT /rules/{rule_id}` 配置，内置类型（`audit_service/rules.py`）：

| `rule_type` | params | 命中条件 |
| --- | --- | --- |
| `event_type_equals` | `{"event_type": "..."}` | 事件类型相等 |
| `source_equals` | `{"source": "..."}` | 来源相等 |
| `payload_equals` | `{"path": "a.b", "value": X}` | 点号路径取值等于 X |
| `payload_threshold` | `{"path": "amount", "min": 1000}` | 路径数值 `>= min`（非数值报规则错误） |

- **可解释结论**：每条结论包含 `matched/status/rationale`，rationale 写明实际值、
  比较过程与 MATCH/NO_MATCH，日志同时打印 `event_id`、规则 id 与版本。
- **异常隔离**：单条规则的任何异常（路径不存在、类型错误、未知规则类型等）被隔离为
  `status=RULE_ERROR` 的结果（带 `error` 与 `event_id`），**不中断整批评估**，
  其余规则与事件照常得出确定结论。
- **增删改与历史可复现**：每次 `upsert` 生成单调递增的新版本，历史版本永久保留；
  `delete` 为逻辑删除（追加 `enabled=false` 墓碑版本）。评估默认用当前启用版本，
  也可通过 `rule_versions={"rule_id": version}` 或 HTTP `rule_versions` 字段
  **按历史版本重算**，保证历史结论可复现。评估结果中带 `rule_version`。

## 6. HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查 |
| `POST /ingest` | 批量摄取；返回 received/accepted/rejected 及每条的原因、序号、摘要 |
| `GET /verify` | 全来源完整性校验结论 |
| `POST /retention/run` | 主动执行时间保留巡检，返回各来源淘汰条数 |
| `GET /rules` | 当前启用规则 |
| `PUT /rules/{rule_id}` | 新建/更新规则（版本递增） |
| `DELETE /rules/{rule_id}` | 逻辑删除（墓碑版本保留，可按旧版本重算） |
| `POST /evaluate` | 合规评估；支持 `rule_versions` 历史重算 |

摄取示例：

```bash
curl -s -X POST http://127.0.0.1:8000/ingest -H 'Content-Type: application/json' -d '{
  "source": "src-billing",
  "events": [
    {"event_id":"evt-001","source":"src-billing","event_type":"payment",
     "timestamp":"2026-09-25T10:00:00+00:00","payload":{"amount":5000}},
    {"event_id":"evt-001","source":"src-billing","event_type":"payment",
     "timestamp":"2026-09-25T10:00:00+00:00","payload":{"amount":5000}}
  ]
}'
```

## 7. 代码结构

```
audit_service/
  models.py      # 事件/结果/规则/保留策略等类型与可区分拒绝原因枚举
  errors.py      # 业务异常
  hashing.py     # 规范化 JSON + SHA-256
  chain.py       # 单来源哈希链：追加、重算校验、淘汰锚点
  retention.py   # 时间保留前缀选择
  store.py       # 单来源存储（幂等索引/序号/锁）+ 多来源聚合（白名单/路由）
  rules.py       # 规则评估器（异常隔离）+ 版本化注册表
  service.py     # 门面：摄取/校验/保留/规则/评估
  api.py         # FastAPI HTTP 层
examples/demo.py # 端到端演示
tests/           # 31 项测试（摄取/完整性/容量/规则/API）
```

## 8. 测试覆盖

- `tests/test_ingestion.py`：规范化、幂等重放（批内+跨批）、同 ID 内容冲突、
  五类拒绝原因可区分；
- `tests/test_integrity.py`：序号单调/冲突、payload 篡改、链断裂、伪造插入、
  淘汰后锚点校验、4×50 并发写入序号连续不重复、8 路并发同 ID 重放只落一条；
- `tests/test_capacity.py`：容量背压原因、满员重放仍幂等、时间淘汰释放容量、
  8×10 并发下接受数恰为容量；
- `tests/test_rules.py`：命中依据、单规则异常隔离（KeyError/TypeError/未知类型）、
  版本更新与历史重算、墓碑删除；
- `tests/test_api.py`：HTTP 端到端。

所有测试日志均打印**事件标识 + 结论/拒绝原因 + 判定依据**（见 `tests/conftest.py`）。
