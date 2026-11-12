# Delta Lake 回退核查与订单日汇总补传网页

对象存储误删文件 / 订单同步中断后，湖仓值班员在同一网页上完成两件事：

1. **回退核查**：查清一张 Delta Lake 表还有哪些版本可以完整回退；
2. **日汇总补传**：逐版补传 JSON 提交与 Parquet ZIP，把订单净变化累加进
   SQLite 日汇总，供下游对账。

## 一、回退核查

值班员在网页上填写版本区间，上传三类材料：

1. `_delta_log` 中留存的 **JSON 提交**（`NNN.json`，20 位补零版本号）；
2. 留存的**经典单文件 Parquet 检查点**（`NNN.checkpoint.parquet`，用 PyArrow 读取）；
3. **现存数据文件的表内相对路径清单**（每行一个路径，如 `data/part-00000-….parquet`）。

工具逐版从 **版本 0 的提交**或**不晚于该版的最新检查点**出发，连续重放
`add` / `remove` 动作到目标版本，再把重建出的快照引用文件与现存清单核对：

- 🟢 **可重建 · 文件齐全**：重放连续无断档，且快照引用的数据文件全部现存；
- 🔴 **缺失提交，重放中断**：明确给出缺失的提交号（版本号列表）；
- 🟠 **重放完整但缺数据文件**：明确给出每个缺失的数据文件表内相对路径。

**仅处理无删除向量（Deletion Vector）的表**：协议特性或任何 add/remove
动作中出现 `deletionVector` 时直接拒绝分析，并指出来源文件。

## 二、订单日汇总补传

回退核查页下半部的“补传入口”，每次补传**一版**两份材料：

- 一个 `_delta_log` **JSON 提交**（`NNN.json`，20 位补零版本号）；
- 一个 **ZIP**，内部按**表内相对路径**打包该版引用到的 Parquet
  （如 `data/part-00000-….parquet`、`_change_data/cdc-….parquet`）。

### 表约束（限同一张表）

- 从**版本 0 启用 CDF**：v0 的 metadata 必须含
  `delta.enableChangeDataFeed = true`，且 v0 必须带 protocol + metadata；
- **无分区**（`partitionColumns` 为空）、**无列映射**
  （`delta.columnMapping.mode` 未配置或为 `none`）、**无删除向量**
  （协议特性与 add/remove/cdc 动作都不得带 `deletionVector`）。

每行一单，非空列为：

- `order_day`：`YYYY-MM-DD` 字符串（必须是合法日历日期）；
- `amount_cents`：int64。

### 计数规则

- 提交里出现 `cdc` 动作时**只读 CDC 文件**，add/remove 一律忽略：
  `insert`、`update_postimage` 计入，`delete`、`update_preimage` 扣减；
- 否则只读 `dataChange=true` 的 `add`（加）/ `remove`（减）文件，
  `dataChange=false` 的动作（如 OPTIMIZE 压缩）跳过；
- **空库从版本 0 开始收，只收紧邻的下一版**；没有任何数据变化的提交
  （如仅含 commitInfo）也推进版号。

### 事务与并发

- 版号与日汇总在**单个 SQLite 事务**（`BEGIN IMMEDIATE`）中一起提交，
  WAL 模式下并发补传同一版只有一份入账，重复提交明确拒绝、不重复计数；
- 全部文件解析 / 校验在事务外完成：**缺件或坏数据会指出具体文件名，
  数据库与已提交汇总均不改变**。

页面从同一已提交状态展示最新版号、已提交版号序列，以及各日单量与分币合计。

## 目录结构

```
app/
  analyzer.py            # 回退核查：日志解析、逐版重放、文件核对、DV 拦截
  backfill.py            # 补传核心：提交/ZIP 解析、CDC 与 add/remove 计数、SQLite 事务
  web.py                 # Flask 路由：核查分析 + 补传上传 + 本地样例 API
  templates/index.html   # 单页界面（核查表单、补传入口、已提交状态）
scripts/generate_samples.py  # 用 PyArrow 构造三套样例 Delta 表与补传 ZIP
samples/                 # 本地样例（健康表 / 误删事故表 / 补传订单表）
tests/                   # pytest：核心逻辑与路由测试
Dockerfile / docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- 回退核查样例按钮：健康表（v0–v4 全可重建）与误删事故表
  （v3 提交丢失、`part-00004` / `part-00006` 被删）；
- 补传库 SQLite 文件落在宿主 `./data/backfill.db`（compose 挂载
  `/srv/data`），容器重建后已提交版号与汇总不丢；可用页面上的
  “重置补传库”按钮清空演示数据。

### 补传样例：跨日更正（v0–v4）

页面点“用本地样例补传下一版”，或逐版 POST
`/api/backfill/sample/<version>`：

| 版本 | 模式 | 内容 |
|---|---|---|
| v0 | add | 建表（CDF=true）+ 两单：11-01 1000、11-02 2000 |
| v1 | add | 新增两单：11-01 1500、11-03 3000 |
| v2 | **CDC** | 跨日更正：删 11-01 一单 1500，11-02 净增一单 2500 |
| v3 | 无变更 | 仅 commitInfo，无数据文件，版号仍推进 |
| v4 | add | 再新增：11-02 500、11-03 700 |

补传完成后的日汇总：

| order_day | 单量 | 分币合计 |
|---|---|---|
| 2026-11-01 | 1 | 1000 |
| 2026-11-02 | 3 | 5000 |
| 2026-11-03 | 2 | 3700 |

样例的每版 ZIP 在 `samples/backfill_orders/uploads/vN.zip`，按表内相对
路径打包，可直接在补传表单手工选择上传。

## 手工上传

**回退核查**：填写版本区间；多选上传 `_delta_log` 的 `.json` 与
`.checkpoint.parquet`（文件名保持 20 位补零）；上传现存数据文件清单
（支持 `#` 注释、空行、`./` 前缀、URI 编码路径）。

**日汇总补传**：选择一个 `NNN.json` 提交与一个 Parquet ZIP 后提交。
ZIP 内成员路径必须与提交动作中的 `path` 一致（表内相对路径）；缺件、
坏 Parquet、列缺失 / 类型错误 / 空值 / 非法日期都会带文件名报错，整版拒绝。

## 本地开发（无 Docker 时）

```bash
python -m venv --without-pip .venv   # 系统无 ensurepip 时
python .venv/get-pip.py              # 再引导 pip
pip install -r requirements.txt
python -I scripts/generate_samples.py
gunicorn --bind 0.0.0.0:8000 app.web:app
python -I -m pytest tests -q
```

补传库路径可用环境变量 `BACKFILL_DB` 覆盖（默认 `./data/backfill.db`）。

## 设计取舍

- **CDC 优先**：同一版只要出现 `cdc` 动作就只信任 CDC 流，避免与
  add/remove 文件重复计数；无 `cdc` 时回退到 dataChange 动作；
- **只认下一版**：版号必须连续，空库强制从 v0 建表提交开始，从源头保证
  “同一张从 v0 启用 CDF 的表”，后续版本只复查分区 / 列映射 / DV 约束；
- **校验在事务外、写入在事务内**：坏数据不会进入事务，已提交状态永不被
  失败补传污染；事务内只有 SQLite 操作，失败整体回滚；
- **无变更也推进**：版号是补传进度的唯一游标，空提交同样落一行版本记录；
- **回退核查逐版独立重放**：每个目标版本都从基线完整重放一次，缺提交不
  会污染其他版本的结论；只认经典单文件检查点，分片检查点登记为“已忽略”；
- **DV 硬拦截**：删除向量会让“文件存在 ≠ 行数据完整”，超出基于文件 /
  行级聚合的处理能力，因此在协议层和动作层双重检测并拒绝。
