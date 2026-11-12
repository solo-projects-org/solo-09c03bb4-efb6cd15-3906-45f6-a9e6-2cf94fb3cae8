"""订单日汇总补传的核心逻辑。

订单同步中断后，数仓工程师逐版补传材料，本模块把每个 Delta Lake 版本的
净变化累加成按 ``order_day`` 分组的日汇总（单量、分币合计），并与版号一起
在单个 SQLite 事务中提交。

每版补传两份材料：

1. 一个 ``_delta_log`` JSON 提交（``NNN.json``，20 位补零版本号）；
2. 一个 ZIP，内部按**表内相对路径**打包该版引用到的 Parquet 数据文件
   （CDC 文件或 add/remove 的数据文件）。

只接受同一张满足以下条件的表：

- 从版本 0 启用 CDF（``delta.enableChangeDataFeed = true``）；
- 无分区、无列映射（column mapping）、无删除向量（deletion vectors）。

每行一单，非空列为 ``order_day``（``YYYY-MM-DD`` 字符串）与
``amount_cents``（int64）。

计数规则：

- 提交里出现 ``cdc`` 动作时只读取 CDC 文件：``insert`` / ``update_postimage``
  计入，``delete`` / ``update_preimage`` 扣减；
- 否则只读取 ``dataChange=true`` 的 ``add``（加）/ ``remove``（减）文件，
  ``dataChange=false`` 的动作（如压缩）跳过；
- 空库从版本 0 开始收，只收紧邻的下一版；没有任何数据变化的提交也推进版号。

缺件或坏数据时抛出 :class:`BackfillError` 并指出具体文件，数据库与已提交
状态均不改变。
"""

from __future__ import annotations

import io
import json
import os
import re
import sqlite3
import urllib.parse
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

# Delta 日志文件名均为 20 位补零版本号
_COMMIT_RE = re.compile(r"^(\d{20})\.json$")
_DAY_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")

CDC_INSERT = "insert"
CDC_UPDATE_POST = "update_postimage"
CDC_DELETE = "delete"
CDC_UPDATE_PRE = "update_preimage"
CDC_POSITIVE = {CDC_INSERT, CDC_UPDATE_POST}
CDC_NEGATIVE = {CDC_DELETE, CDC_UPDATE_PRE}

ORDER_DAY = "order_day"
AMOUNT_CENTS = "amount_cents"
CHANGE_TYPE = "_change_type"

DV_FEATURE = "deletionVectors"
COLUMN_MAPPING_KEY = "delta.columnMapping.mode"
CDF_KEY = "delta.enableChangeDataFeed"

# 允许的列映射模式：未配置或 none
_COLUMN_MAPPING_ALLOWED = {None, "none"}


class BackfillError(Exception):
    """面向用户的补传错误：消息中必须能定位到具体文件 / 字段。"""


@dataclass
class UploadFile:
    """一次上传的文件（文件名 + 已读入内存的字节）。"""

    filename: str
    data: bytes


@dataclass
class _Action:
    kind: str  # add / remove / cdc / protocol / metadata / commitInfo / other
    path: str | None = None
    data_change: bool = True
    raw: dict = field(default_factory=dict)


@dataclass
class ParsedCommit:
    version: int
    source: str
    actions: list[_Action]
    protocol: dict | None = None
    metadata: dict | None = None
    dv_detected: bool = False


@dataclass
class CommitResult:
    """一版补传的计算结果（事务提交前的内存态）。"""

    version: int
    mode: str  # "cdc" 或 "data_change"
    # order_day -> [单量增量, 分币合计增量]
    deltas: dict[str, list[int]] = field(default_factory=dict)
    files_read: list[str] = field(default_factory=list)
    cdc_files: list[str] = field(default_factory=list)
    data_files: list[str] = field(default_factory=list)
    no_change: bool = False


# --------------------------------------------------------------------------- #
# 路径与文件名
# --------------------------------------------------------------------------- #

def normalize_table_path(raw: Any) -> str:
    """把日志 / ZIP 中的路径归一化为表内相对路径。

    - 去掉前导 ``./`` 与 ``/``，拒绝绝对路径与 ``..`` 穿越；
    - Delta 日志中的路径是 URI 编码的，统一 percent-decode。
    """
    if not isinstance(raw, str) or not raw.strip():
        raise BackfillError(f"数据文件路径不是非空字符串：{raw!r}")
    decoded = urllib.parse.unquote(raw.strip())
    parts: list[str] = []
    for segment in decoded.replace("\\", "/").split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            raise BackfillError(f"数据文件路径包含 '..' 穿越，拒绝处理：{raw!r}")
        parts.append(segment)
    if not parts:
        raise BackfillError(f"数据文件路径归一化后为空：{raw!r}")
    return "/".join(parts)


def commit_version_from_name(filename: str) -> int:
    name = filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    match = _COMMIT_RE.match(name)
    if not match:
        raise BackfillError(
            f"JSON 提交文件名 {name!r} 不合法：应为 20 位补零版本号命名的 "
            f"NNN.json，如 00000000000000000003.json"
        )
    return int(match.group(1))


# --------------------------------------------------------------------------- #
# JSON 提交解析与表约束
# --------------------------------------------------------------------------- #

def _iter_json_objects(text: str, source: str) -> Iterable[dict]:
    """Delta 提交是 ndjson（每行一个动作对象），同时兼容整体 JSON 数组。"""
    stripped = text.strip()
    if not stripped:
        raise BackfillError(f"{source} 是空文件，不是合法的 Delta JSON 提交")
    line_objs: list[dict] = []
    ndjson_ok = True
    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            ndjson_ok = False
            break
        if not isinstance(obj, dict):
            ndjson_ok = False
            break
        line_objs.append(obj)
    if ndjson_ok and line_objs:
        return line_objs
    try:
        whole = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise BackfillError(
            f"{source} 既不是 ndjson 也不是合法 JSON：{exc}"
        ) from exc
    if isinstance(whole, list):
        if not all(isinstance(o, dict) for o in whole):
            raise BackfillError(f"{source} 的 JSON 数组元素必须都是动作对象")
        return whole
    return [whole]


def _protocol_features(protocol: dict) -> set[str]:
    features: set[str] = set()
    for key in ("readerFeatures", "writerFeatures", "features"):
        value = protocol.get(key)
        if isinstance(value, list):
            features.update(str(v) for v in value)
    return features


def _metadata_configuration(metadata: dict) -> dict[str, str]:
    configuration = metadata.get("configuration")
    if not isinstance(configuration, dict):
        return {}
    return {str(k): ("" if v is None else str(v)) for k, v in configuration.items()}


def parse_commit_file(version: int, data: bytes, source: str) -> ParsedCommit:
    """解析一个 JSON 提交文件，同时执行删除向量 / 列映射等表约束检查。"""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BackfillError(f"{source} 不是合法的 UTF-8 文本：{exc}") from exc

    parsed = ParsedCommit(version=version, source=source, actions=[])
    for obj in _iter_json_objects(text, source):
        if "protocol" in obj and isinstance(obj["protocol"], dict):
            parsed.protocol = obj["protocol"]
            if DV_FEATURE in _protocol_features(parsed.protocol):
                parsed.dv_detected = True
        if "metadata" in obj and isinstance(obj["metadata"], dict):
            parsed.metadata = obj["metadata"]
            config = _metadata_configuration(parsed.metadata)
            if config.get(COLUMN_MAPPING_KEY, "none").lower() not in _COLUMN_MAPPING_ALLOWED:
                raise BackfillError(
                    f"{source} 的 metadata 启用了列映射（{COLUMN_MAPPING_KEY}="
                    f"{config.get(COLUMN_MAPPING_KEY)!r}）。本工具只处理无列映射的表。"
                )
            partition_cols = parsed.metadata.get("partitionColumns")
            if isinstance(partition_cols, list) and partition_cols:
                raise BackfillError(
                    f"{source} 的 metadata 声明了分区列 {partition_cols}。"
                    f"本工具只处理无分区表。"
                )
        for kind in ("add", "remove", "cdc"):
            action = obj.get(kind)
            if isinstance(action, dict):
                if action.get("deletionVector") is not None:
                    parsed.dv_detected = True
                path = action.get("path")
                parsed.actions.append(_Action(
                    kind=kind,
                    path=normalize_table_path(path) if path is not None else None,
                    data_change=bool(action.get("dataChange", True)),
                    raw=action,
                ))
    if parsed.dv_detected:
        raise BackfillError(
            f"{source} 的协议特性或 add/remove/cdc 动作带有 deletionVector。"
            f"本工具只处理无删除向量的表。"
        )
    return parsed


def validate_table_bootstrap(parsed: ParsedCommit) -> None:
    """版本 0 必须是同一张表的建表提交：含 protocol + metadata 且启用 CDF。"""
    if parsed.protocol is None:
        raise BackfillError(
            f"{parsed.source} 缺少 protocol 动作：补传必须从版本 0 的建表提交开始，"
            f"且只接受同一张表。"
        )
    if parsed.metadata is None:
        raise BackfillError(
            f"{parsed.source} 缺少 metadata 动作：补传必须从版本 0 的建表提交开始，"
            f"且只接受同一张表。"
        )
    config = _metadata_configuration(parsed.metadata)
    if config.get(CDF_KEY, "").lower() != "true":
        raise BackfillError(
            f"{parsed.source} 的 metadata 未启用 CDF（需要 "
            f"{CDF_KEY}=true）。本工具只处理从版本 0 启用变更数据馈送的表。"
        )
    partition_cols = parsed.metadata.get("partitionColumns")
    if isinstance(partition_cols, list) and partition_cols:
        raise BackfillError(
            f"{parsed.source} 的 metadata 声明了分区列 {partition_cols}，"
            f"本工具只处理无分区表。"
        )
    mapping_mode = config.get(COLUMN_MAPPING_KEY, "none").lower()
    if mapping_mode not in _COLUMN_MAPPING_ALLOWED:
        raise BackfillError(
            f"{parsed.source} 启用了列映射（{COLUMN_MAPPING_KEY}={mapping_mode!r}），"
            f"本工具只处理无列映射的表。"
        )
    if DV_FEATURE in _protocol_features(parsed.protocol):
        raise BackfillError(
            f"{parsed.source} 的 protocol 特性包含 {DV_FEATURE}，"
            f"本工具只处理无删除向量的表。"
        )


# --------------------------------------------------------------------------- #
# ZIP 解包与 Parquet 读取
# --------------------------------------------------------------------------- #

def _safe_zip_member_name(name: str) -> str:
    # zipfile 路径用 / 分隔；统一拒绝盘符、绝对路径与父目录穿越
    if name.startswith("/") or name.startswith("\\") or re.match(r"^[A-Za-z]:", name):
        raise BackfillError(f"ZIP 内成员 {name!r} 是绝对路径 / 盘符路径，拒绝处理")
    normalized = normalize_table_path(name)
    return normalized


def extract_zip(zip_bytes: bytes, source: str) -> dict[str, bytes]:
    """解包 ZIP，返回 {表内相对路径: 文件字节}，并做路径穿越防护。"""
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as exc:
        raise BackfillError(f"{source} 不是合法的 ZIP 文件：{exc}") from exc

    members: dict[str, bytes] = {}
    with archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            normalized = _safe_zip_member_name(info.filename)
            if normalized in members:
                raise BackfillError(
                    f"{source} 中表内路径 {normalized} 对应多个 ZIP 成员，拒绝处理"
                )
            try:
                members[normalized] = archive.read(info)
            except zipfile.BadZipFile as exc:
                raise BackfillError(
                    f"{source} 内成员 {info.filename} 读取失败：{exc}"
                ) from exc
    # 允许空 ZIP：无任何数据变化的提交（如仅含 commitInfo）没有可打包的
    # Parquet，版号仍要推进；缺件错误在引用文件时再按路径报出。
    return members


def _validate_order_day(value: Any, source: str, row_index: int) -> str:
    if not isinstance(value, str):
        raise BackfillError(
            f"{source} 第 {row_index} 行 {ORDER_DAY} 不是字符串（实际类型 "
            f"{type(value).__name__}），要求 YYYY-MM-DD 字符串"
        )
    match = _DAY_RE.match(value)
    if not match:
        raise BackfillError(
            f"{source} 第 {row_index} 行 {ORDER_DAY}={value!r} 不符合 YYYY-MM-DD 格式"
        )
    try:
        date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError as exc:
        raise BackfillError(
            f"{source} 第 {row_index} 行 {ORDER_DAY}={value!r} 不是合法日历日期：{exc}"
        ) from exc
    return value


def _read_order_parquet(data: bytes, source: str, *, is_cdc: bool) -> list[tuple[str, int, str | None]]:
    """读取一个数据 / CDC Parquet，逐行校验后返回 (order_day, amount_cents, change_type)。"""
    try:
        table = pq.read_table(io.BytesIO(data))
    except Exception as exc:  # parquet 损坏 / 非 parquet
        raise BackfillError(f"{source} 无法作为 Parquet 读取：{exc}") from exc

    columns = set(table.column_names)
    missing = [c for c in (ORDER_DAY, AMOUNT_CENTS) if c not in columns]
    if missing:
        raise BackfillError(
            f"{source} 缺少必需列 {missing}；表结构必须包含非空列 "
            f"{ORDER_DAY}（字符串）与 {AMOUNT_CENTS}（int64）"
        )

    day_field = table.schema.field(ORDER_DAY)
    amount_field = table.schema.field(AMOUNT_CENTS)
    if not pa.types.is_string(day_field.type) and not pa.types.is_large_string(day_field.type):
        raise BackfillError(
            f"{source} 的 {ORDER_DAY} 列类型是 {day_field.type}，要求字符串类型"
        )
    if not pa.types.is_int64(amount_field.type):
        raise BackfillError(
            f"{source} 的 {AMOUNT_CENTS} 列类型是 {amount_field.type}，要求 int64"
        )

    if is_cdc:
        if CHANGE_TYPE not in columns:
            raise BackfillError(
                f"{source} 是 CDC 文件但缺少 {CHANGE_TYPE} 列"
            )
        type_field = table.schema.field(CHANGE_TYPE)
        if not pa.types.is_string(type_field.type) and not pa.types.is_large_string(type_field.type):
            raise BackfillError(
                f"{source} 的 {CHANGE_TYPE} 列类型是 {type_field.type}，要求字符串类型"
            )

    days = table.column(ORDER_DAY).combine_chunks().to_pylist()
    amounts = table.column(AMOUNT_CENTS).combine_chunks().to_pylist()
    change_types = (
        table.column(CHANGE_TYPE).combine_chunks().to_pylist()
        if is_cdc else [None] * table.num_rows
    )

    rows: list[tuple[str, int, str | None]] = []
    for idx, (day, amount, change_type) in enumerate(
        zip(days, amounts, change_types), start=1
    ):
        if day is None:
            raise BackfillError(f"{source} 第 {idx} 行 {ORDER_DAY} 为空，该列不允许空值")
        day = _validate_order_day(day, source, idx)
        if amount is None:
            raise BackfillError(
                f"{source} 第 {idx} 行 {AMOUNT_CENTS} 为空，该列不允许空值"
            )
        if not isinstance(amount, int):
            raise BackfillError(
                f"{source} 第 {idx} 行 {AMOUNT_CENTS} 不是整数：{amount!r}"
            )
        if is_cdc:
            if not isinstance(change_type, str) or change_type not in (
                CDC_POSITIVE | CDC_NEGATIVE
            ):
                raise BackfillError(
                    f"{source} 第 {idx} 行 {CHANGE_TYPE}={change_type!r} 非法，"
                    f"只允许 {sorted(CDC_POSITIVE | CDC_NEGATIVE)}"
                )
        rows.append((day, amount, change_type))
    return rows


# --------------------------------------------------------------------------- #
# 单版计算
# --------------------------------------------------------------------------- #

def _require_member(members: dict[str, bytes], path: str, zip_source: str) -> bytes:
    if path not in members:
        raise BackfillError(
            f"ZIP 包 {zip_source} 缺少提交引用的数据文件：{path}（ZIP 内须按表内"
            f"相对路径打包 Parquet）"
        )
    return members[path]


def compute_commit(parsed: ParsedCommit, zip_members: dict[str, bytes],
                   zip_source: str) -> CommitResult:
    """按 CDC 优先 / dataChange 回退规则计算一版的日汇总增量。"""
    result = CommitResult(version=parsed.version, mode="data_change")

    cdc_actions = [a for a in parsed.actions if a.kind == "cdc"]
    data_actions = [
        a for a in parsed.actions
        if a.kind in ("add", "remove") and a.data_change
    ]

    if cdc_actions:
        # 有 cdc 就只读 cdc：add/remove 一律忽略
        result.mode = "cdc"
        for action in cdc_actions:
            if action.path is None:
                raise BackfillError(
                    f"{parsed.source} 中存在没有 path 的 cdc 动作，无法定位文件"
                )
            result.cdc_files.append(action.path)
            data = _require_member(zip_members, action.path, zip_source)
            for day, amount, change_type in _read_order_parquet(
                data, action.path, is_cdc=True
            ):
                sign = 1 if change_type in CDC_POSITIVE else -1
                bucket = result.deltas.setdefault(day, [0, 0])
                bucket[0] += sign
                bucket[1] += sign * amount
            result.files_read.append(action.path)
        return result

    # 无 cdc：只读 dataChange=true 的 add/remove，分别加、减
    for action in data_actions:
        if action.path is None:
            raise BackfillError(
                f"{parsed.source} 中存在没有 path 的 {action.kind} 动作，无法定位文件"
            )
        result.data_files.append(action.path)
        data = _require_member(zip_members, action.path, zip_source)
        sign = 1 if action.kind == "add" else -1
        for day, amount, _ in _read_order_parquet(data, action.path, is_cdc=False):
            bucket = result.deltas.setdefault(day, [0, 0])
            bucket[0] += sign
            bucket[1] += sign * amount
        result.files_read.append(action.path)

    result.no_change = not result.deltas
    return result


def process_upload(commit_file: UploadFile, zip_file: UploadFile) -> CommitResult:
    """Web 层入口：校验文件名、解析提交、解包 ZIP、计算增量（不落库）。"""
    version = commit_version_from_name(commit_file.filename)
    parsed = parse_commit_file(version, commit_file.data,
                               commit_file.filename.rsplit("/", 1)[-1])
    zip_members = extract_zip(zip_file.data,
                              zip_file.filename.rsplit("/", 1)[-1])
    return compute_commit(parsed, zip_members,
                          zip_file.filename.rsplit("/", 1)[-1])


# --------------------------------------------------------------------------- #
# SQLite 持久化（版号与汇总在同一事务提交）
# --------------------------------------------------------------------------- #

_SCHEMA = """
CREATE TABLE IF NOT EXISTS backfill_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_version INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS backfill_versions (
    version INTEGER PRIMARY KEY,
    mode TEXT NOT NULL,
    no_change INTEGER NOT NULL,
    files_read TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS daily_summary (
    order_day TEXT PRIMARY KEY,
    order_count INTEGER NOT NULL,
    amount_total_cents INTEGER NOT NULL
);
"""


def connect(db_path: str, *, timeout: float = 30.0) -> sqlite3.Connection:
    """打开（必要时创建）补传库，并开启 WAL 以支持同版并发提交。"""
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=timeout, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    return conn


def get_state(conn: sqlite3.Connection) -> dict[str, Any]:
    """读取当前已提交状态（版号 + 各日单量 / 分币合计）。"""
    row = conn.execute(
        "SELECT last_version FROM backfill_state WHERE id = 1"
    ).fetchone()
    if row is None:
        last_version = None
    else:
        last_version = row[0]
    daily = [
        {"order_day": r["order_day"],
         "order_count": r["order_count"],
         "amount_total_cents": r["amount_total_cents"]}
        for r in conn.execute(
            "SELECT order_day, order_count, amount_total_cents "
            "FROM daily_summary ORDER BY order_day"
        )
    ]
    versions = [
        {"version": r["version"], "mode": r["mode"],
         "no_change": bool(r["no_change"]), "files_read": json.loads(r["files_read"])}
        for r in conn.execute(
            "SELECT version, mode, no_change, files_read "
            "FROM backfill_versions ORDER BY version"
        )
    ]
    return {"last_version": last_version, "daily": daily, "versions": versions}


def commit_result(conn: sqlite3.Connection, parsed: ParsedCommit,
                  result: CommitResult) -> dict[str, Any]:
    """在单个事务里推进版号并合并日汇总；同版并发不重复入账。

    必须先完成全部文件解析 / 校验（见 :func:`process_upload`），本函数内
    不再触碰上传字节，因此事务里只有 SQLite 操作，失败即整体回滚。
    """
    version = result.version
    # IMMEDIATE：第一时间拿写锁，把同版 / 并发补传串行化
    conn.execute("BEGIN IMMEDIATE")
    try:
        state_row = conn.execute(
            "SELECT last_version FROM backfill_state WHERE id = 1"
        ).fetchone()
        last_version = None if state_row is None else state_row["last_version"]

        if last_version is None:
            # 空库：从版本 0 开始收
            if version != 0:
                raise BackfillError(
                    f"补传库为空，必须先提交版本 0 的建表提交，收到的却是版本 {version}"
                )
            validate_table_bootstrap(parsed)
        else:
            expected = last_version + 1
            if version == last_version:
                raise BackfillError(
                    f"版本 {version} 已经提交过，同版不重复入账；"
                    f"下一版应为 v{expected}"
                )
            if version != expected:
                raise BackfillError(
                    f"版号断档：当前已提交到 v{last_version}，只接受下一版 "
                    f"v{expected}，收到的是 v{version}"
                )

        # 入账日汇总
        for day, (count_delta, amount_delta) in result.deltas.items():
            row = conn.execute(
                "SELECT order_count, amount_total_cents FROM daily_summary "
                "WHERE order_day = ?", (day,)
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO daily_summary(order_day, order_count, amount_total_cents) "
                    "VALUES (?, ?, ?)", (day, count_delta, amount_delta)
                )
            else:
                conn.execute(
                    "UPDATE daily_summary SET order_count = ?, amount_total_cents = ? "
                    "WHERE order_day = ?",
                    (row["order_count"] + count_delta,
                     row["amount_total_cents"] + amount_delta, day),
                )

        conn.execute(
            "INSERT INTO backfill_versions(version, mode, no_change, files_read) "
            "VALUES (?, ?, ?, ?)",
            (version, result.mode, 1 if result.no_change else 0,
             json.dumps(result.files_read, ensure_ascii=False)),
        )
        conn.execute(
            "INSERT INTO backfill_state(id, last_version, updated_at) "
            "VALUES (1, ?, datetime('now')) "
            "ON CONFLICT(id) DO UPDATE SET last_version = excluded.last_version, "
            "updated_at = excluded.updated_at",
            (version,),
        )
        conn.execute("COMMIT")
    except BackfillError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return get_state(conn)


def reset_state(conn: sqlite3.Connection) -> None:
    """清空补传库（仅供本地样例一键重置使用）。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM daily_summary")
        conn.execute("DELETE FROM backfill_versions")
        conn.execute("DELETE FROM backfill_state")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
