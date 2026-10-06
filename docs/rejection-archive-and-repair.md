# 被拒投递留档与修复重投

本文档说明在既有「批量摄取 / 幂等 / 序号约束 / 容量背压 / 完整性校验」
之上新增的两块能力：**被拒投递留档** 与 **修复后重投**，以及它们与既有
语义的兼容边界。

## 1. 被拒投递留档（RejectionArchive）

- 每一条被拒绝的投递都会留档（`app/archive.py`），记录：来源、事件标识、
  本轮拒绝原因（`RejectReason`）、可解释细节、当时的原始投递内容、
  首次/最近一次出现时间与累计投递次数。
- **去重**：以 `(source, event_id)` 为键。同一份内容被反复投递时只更新
  `attempts` / `last_seen_ms` 与最近一次的原因、原始内容，**不产生新条目**。
- **筛选查询**：`AuditService.list_rejections(source=, reason=, event_id=)`
  支持按来源、按原因、按事件标识组合筛选；
  `GET /rejections`、`GET /rejections/{source}/{event_id}` 提供 HTTP 访问。
- **容量上限**：留档有容量上限（`AuditService(archive_capacity=...)`，
  默认 10000）。写满后：
  - 新键的拒绝结果以可区分原因 `ARCHIVE_CAPACITY_EXCEEDED` 返回（背压），
    `detail` 中保留本轮原始拒绝原因；
  - **不静默丢弃、不挤占旧留档**；已有键的重复投递不受容量限制，
    仍合并到原留档。

## 2. 修复后重投（resubmit）

- 入口：`AuditService.resubmit(source, event_id, raw)` 或
  `POST /sources/{source}/events/{event_id}:resubmit`。
- 仅面向「**之前被拒、且从未成功落库**」的事件标识；既无留档也未接收过
  的标识走修复通道会被拒绝（`RepairNotAllowed` / HTTP 409）。
- 修正后的内容走与正常摄取**完全相同**的校验与接收路径（字段校验 →
  幂等/冲突 → 序号 → 容量 → 落库入链），通过后结果状态为
  `REPAIRED`（修复后这次终于通过了），此后与其它正常事件一样参与
  完整性校验。
- 修复成功后，当初的留档**保留可查**并标记 `resolved=True`（合规留痕），
  拒绝原因与当时的原始内容不丢。

## 3. 每次投递结果的可区分状态

| 状态 | 含义 |
|---|---|
| `ACCEPTED` | 全新接收 |
| `DUPLICATE` | 幂等重放：内容与原接收记录一致，不重复记录 |
| `REPAIRED` | 修复重投：此前被拒（有未解决留档），本次通过全部校验 |
| `REJECTED` | 仍然没通过，`reason` 给出本轮原因（含 `ARCHIVE_CAPACITY_EXCEEDED` 背压） |

## 4. 与既有语义的兼容边界

**保持不变的旧行为：**

- 幂等：`event_id` 已接收且内容一致 → `DUPLICATE`，不重复落库；
- 冲突：`event_id` 已接收但内容不同 → 按 `DUPLICATE_EVENT_ID` 拒绝，
  **修复通道同样不得绕过**（已接收标识再走修复通道，相同内容按幂等重放、
  不同内容按冲突拒绝）；
- 序号：来源内序号从 0 起严格递增（`next = last + 1`），判定基准
  （`last_sequence`）不受保留策略淘汰影响；修复重投也必须满足该约束；
- 容量：摄取路径不做静默淘汰，在窗事件数达上限以 `CAPACITY_EXCEEDED`
  背压拒绝，由调用方执行保留策略后重试；
- 完整性：链式哈希 + 只增锚点 + 检查点的校验结论可复现。

**新明确的边界：**

- 留档是**链外数据**：留档的写入、合并、标记已解决都不进入完整性链，
  不改写已入链数据，也不改变校验结论（拒绝前后链头不变；
  修复只是正常追加新链接）；
- 所有被拒绝的投递（含冲突、序号、容量、留档背压）都会尝试留档，
  留档本身写满时以 `ARCHIVE_CAPACITY_EXCEEDED` 可区分地背压；
- 并发下同一标识的修复重投只成功一次（每来源一把锁，判定与追加原子化），
  其余并发请求按幂等重放或冲突拒绝处理，来源内序号保持单调不重复。

## 5. 本地验证

```bash
# 安装依赖（uv）
uv sync

# 运行完整测试（日志可见每条的 event_id 与命中的判定/拒绝原因）
uv run pytest tests/ -v

# 启动服务
uv run python main.py
# POST /events:ingest                 批量摄取
# GET  /rejections?source=&reason=&event_id=   留档筛选
# POST /sources/{s}/events/{id}:resubmit       修复重投
# GET  /verify/{source}               完整性校验
```

关键用例与文件对应：

- `tests/test_rejection_archive.py`：留档去重、筛选、容量背压；
- `tests/test_repair_resubmit.py`：修复成功、修复后仍不通过、已接收标识
  不得走修复通道、留档/修复不改写链；
- `tests/test_concurrent_repair.py`：并发修复只成功一次、序号单调不重复；
- `tests/test_ingest_baseline.py`：既有幂等/序号/容量/完整性语义回归。
