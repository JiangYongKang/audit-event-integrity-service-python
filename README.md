# 审计事件完整性服务

按来源批量接收审计事件，提供幂等去重、来源内序号约束、容量背压、
SHA-256 哈希链完整性校验、可配置合规规则评估，以及 **被拒投递留档 +
修复后重投** 能力。

## 运行与本地验证

```bash
uv sync                  # 安装依赖
uv run pytest            # 跑全部测试（日志含事件标识与命中原因）
uv run python main.py    # 启动 HTTP 服务（127.0.0.1:8000）
```

HTTP 接口：`POST /ingest`、`POST /repair`、`GET /rejections`、
`POST /retention`、`GET /verify[/{source}]`、`GET /events/{id}`、
`PUT/DELETE /rules`、`POST /evaluate`。

## 新增能力：被拒投递留档

- 每条被拒绝的投递都会留档：来源、事件标识、拒绝原因与细节、当时的
  原始内容、产生通道（`ingest` / `repair`）、首次/最近时间与累计次数。
- **去重**：以 `(event_id, source, 内容哈希)` 为键，同一份内容反复投递
  只累计 `attempts`，不产生新条目。
- **筛选**：`GET /rejections?source=&reason=&event_id=`（与关系）。
- **容量上限**：`AuditService(archive_capacity=N)`。写满后新留档以
  `archive_capacity_exceeded` 可区分原因背压（响应中附带本轮原始拒绝
  原因），不静默丢弃、不挤掉老留档。

## 新增能力：修复后重投

`POST /repair`（或服务端 `repair_ingest`）面向「曾被拒、且从未成功落库」
的事件标识：

- 修正后的内容通过全部校验 → 状态 `repaired`，按正常路径落库并参与
  完整性校验；当初留档保留可查并标记 `resolved=true`（合规留痕）。
- 仍不通过 → 状态 `rejected` + 本轮原因，写入 `repair` 通道留档。
- 无在先拒档的标识 → `repair_not_allowed`，不允许走修复通道。

## 结果可区分

每次投递结果为四选一：`accepted`（全新接收）、`duplicate`（幂等重放）、
`repaired`（修复后本次通过）、`rejected`（仍不通过，附本轮原因）。

## 与既有语义的兼容边界

**保持不变的旧行为**：

- 幂等：同标识同内容重放 → `duplicate`，不重复落库；
- 冲突：同标识不同内容 → `rejected / duplicate_event_id`，
  **修复通道同样遵守，不能绕过**；已成功接收的标识不能再走修复通道；
- 来源内序号单调（重复 → `duplicate_sequence`，倒退 →
  `sequence_out_of_order`）与容量背压（`capacity_exceeded`）在修复
  重投路径上继续生效；
- 完整性校验结论不受留档/修复影响：留档不写链，修复只追加新链接，
  不改写已入链数据。

**新明确的边界**：

- 修复通道仅限「有在先拒档且从未落库」的标识（否则
  `repair_not_allowed`）；
- 留档容量独立于事件窗口容量，写满即背压
  （`archive_capacity_exceeded`），不做静默淘汰；
- 修复成功后留档不删除，仅置 `resolved` 标记。

## 并发约定

摄取与修复重投在一把摄取锁内串行化：标识判重、序号检查、链追加原子
完成。并发对同一标识修复重投时，只会有一个 `repaired`，其余得到
`duplicate` 或带明确原因的 `rejected`，来源内序号保持单调不重复。

## 测试

```bash
uv run pytest -v   # 20 个用例；日志可见事件标识与命中的判定/拒绝原因
```

覆盖：留档去重、筛选、容量背压、修复成功/仍不通过/序号约束、已接收
标识冲突不被绕过、无在先拒档拒绝、并发修复只成功一次，以及基线摄取
/幂等/序号/容量/保留/完整性回归。
