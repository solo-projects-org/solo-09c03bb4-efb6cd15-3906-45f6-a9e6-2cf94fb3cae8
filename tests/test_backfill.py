"""backfill 补传核心逻辑测试。

运行（仓库根目录）：
    /tmp/venv/bin/python -I -m pytest tests -q
"""

from __future__ import annotations

import io
import json
import sqlite3
import threading
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app import backfill as bf
from app.backfill import (
    AMOUNT_CENTS,
    BackfillError,
    CommitResult,
    ORDER_DAY,
    UploadFile,
    commit_result,
    compute_commit,
    connect,
    extract_zip,
    get_state,
    parse_commit_file,
    process_upload,
    reset_state,
)

# --------------------------------------------------------------------------- #
# 构造工具
# --------------------------------------------------------------------------- #

ORDER_SCHEMA = pa.schema([
    ("order_day", pa.string()),
    ("amount_cents", pa.int64()),
])
CDC_SCHEMA = pa.schema([
    ("order_day", pa.string()),
    ("amount_cents", pa.int64()),
    ("_change_type", pa.string()),
])


def parquet_bytes(rows, schema=ORDER_SCHEMA, col_map=("order_day", "amount_cents")):
    data = {name: [r[i] for r in rows]
            for i, name in enumerate(col_map)}
    table = pa.table(data, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def cdc_bytes(rows):
    return parquet_bytes(
        rows, schema=CDC_SCHEMA,
        col_map=("order_day", "amount_cents", "_change_type"))


def zip_bytes(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def commit_bytes(version, actions):
    return ("\n".join(json.dumps(a, ensure_ascii=False) for a in actions)
            ).encode("utf-8")


def commit_upload(version, actions) -> UploadFile:
    return UploadFile(filename=f"{version:020d}.json",
                      data=commit_bytes(version, actions))


PROTOCOL = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
METADATA = {"metadata": {
    "id": "t", "partitionColumns": [],
    "configuration": {"delta.enableChangeDataFeed": "true"},
}}


def add(path, data_change=True):
    return {"add": {"path": path, "dataChange": data_change}}


def remove(path, data_change=True):
    return {"remove": {"path": path, "dataChange": data_change}}


def cdc(path):
    return {"cdc": {"path": path, "dataChange": True}}


def p(n):
    return f"data/part-{n:05d}.parquet"


@pytest.fixture()
def db(tmp_path):
    conn = connect(str(tmp_path / "backfill.db"))
    yield conn
    conn.close()


def bootstrap(conn, zip_members=None, extra_actions=None):
    """提交 v0 建表 + 可选首单，返回 (parsed, result)。"""
    members = zip_members or {}
    actions = [PROTOCOL, METADATA] + (extra_actions or [])
    parsed = parse_commit_file(0, commit_bytes(0, actions), "00000000000000000000.json")
    result = compute_commit(parsed, members, "v0.zip")
    commit_result(conn, parsed, result)
    return parsed, result


# --------------------------------------------------------------------------- #
# 文件名 / 路径
# --------------------------------------------------------------------------- #

def test_commit_version_from_name():
    assert bf.commit_version_from_name("00000000000000000007.json") == 7
    assert bf.commit_version_from_name("a/b/00000000000000000000.json") == 0
    with pytest.raises(BackfillError, match="20 位补零"):
        bf.commit_version_from_name("7.json")
    with pytest.raises(BackfillError, match="20 位补零"):
        bf.commit_version_from_name("00000000000000000007.checkpoint.parquet")


def test_normalize_table_path():
    assert bf.normalize_table_path("./data/a.parquet") == "data/a.parquet"
    assert bf.normalize_table_path("data/space%20file.parquet") == "data/space file.parquet"
    assert bf.normalize_table_path("data//a/./b") == "data/a/b"
    with pytest.raises(BackfillError, match=r"\.\."):
        bf.normalize_table_path("data/../secret.parquet")
    with pytest.raises(BackfillError, match="非空字符串"):
        bf.normalize_table_path("")


def test_zip_rejects_traversal_and_bad_zip():
    with pytest.raises(BackfillError, match="绝对路径"):
        extract_zip(zip_bytes({"/etc/passwd": b"x"}), "x.zip")
    with pytest.raises(BackfillError, match=r"\.\."):
        extract_zip(zip_bytes({"../evil.parquet": b"x"}), "x.zip")
    with pytest.raises(BackfillError, match="不是合法的 ZIP"):
        extract_zip(b"not a zip", "x.zip")
    with pytest.raises(BackfillError, match="多个 ZIP 成员"):
        # 同一表内路径的两种写法冲突
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("data/a.parquet", b"1")
            zf.writestr("./data/a.parquet", b"2")
        extract_zip(buf.getvalue(), "x.zip")


def test_empty_zip_allowed_for_no_change_commit(db):
    parsed = parse_commit_file(
        0, commit_bytes(0, [PROTOCOL, METADATA]),
        "00000000000000000000.json")
    result = compute_commit(parsed, {}, "v0.zip")
    assert result.no_change is True
    state = commit_result(db, parsed, result)
    assert state["last_version"] == 0
    assert state["daily"] == []


# --------------------------------------------------------------------------- #
# 表约束
# --------------------------------------------------------------------------- #

def test_bootstrap_requires_protocol_and_metadata(db):
    parsed = parse_commit_file(0, commit_bytes(0, [add(p(0))]),
                               "00000000000000000000.json")
    result = compute_commit(parsed, {p(0): parquet_bytes([("2026-11-01", 10)])}, "z.zip")
    with pytest.raises(BackfillError, match="protocol"):
        commit_result(db, parsed, result)

    parsed = parse_commit_file(0, commit_bytes(0, [PROTOCOL, add(p(0))]),
                               "00000000000000000000.json")
    result = compute_commit(parsed, {p(0): parquet_bytes([("2026-11-01", 10)])}, "z.zip")
    with pytest.raises(BackfillError, match="metadata"):
        commit_result(db, parsed, result)


def test_bootstrap_requires_cdf_enabled(db):
    metadata = {"metadata": {"id": "t", "partitionColumns": [],
                             "configuration": {"delta.enableChangeDataFeed": "false"}}}
    parsed = parse_commit_file(0, commit_bytes(0, [PROTOCOL, metadata]),
                               "00000000000000000000.json")
    with pytest.raises(BackfillError, match="CDF"):
        commit_result(db, parsed, CommitResult(version=0, mode="data_change"))


def test_partitioned_table_rejected():
    metadata = {"metadata": {"id": "t", "partitionColumns": ["order_day"],
                             "configuration": {"delta.enableChangeDataFeed": "true"}}}
    with pytest.raises(BackfillError, match="分区"):
        parse_commit_file(0, commit_bytes(0, [PROTOCOL, metadata]),
                          "00000000000000000000.json")


def test_column_mapping_rejected():
    metadata = {"metadata": {"id": "t", "partitionColumns": [],
                             "configuration": {
                                 "delta.enableChangeDataFeed": "true",
                                 "delta.columnMapping.mode": "name"}}}
    with pytest.raises(BackfillError, match="列映射"):
        parse_commit_file(0, commit_bytes(0, [PROTOCOL, metadata]),
                          "00000000000000000000.json")


def test_deletion_vectors_rejected():
    protocol = {"protocol": {"minReaderVersion": 3, "minWriterVersion": 7,
                             "readerFeatures": ["deletionVectors"],
                             "writerFeatures": ["deletionVectors"]}}
    with pytest.raises(BackfillError, match="删除向量"):
        parse_commit_file(0, commit_bytes(0, [protocol, METADATA]),
                          "00000000000000000000.json")
    bad_add = {"add": {"path": p(0), "dataChange": True,
                       "deletionVector": {"storageType": "u",
                                          "pathOrInlineDv": "x"}}}
    with pytest.raises(BackfillError, match="删除向量"):
        parse_commit_file(1, commit_bytes(1, [bad_add]),
                          "00000000000000000001.json")


def test_empty_database_must_start_at_version_zero(db):
    parsed = parse_commit_file(3, commit_bytes(3, [add(p(0))]),
                               "00000000000000000003.json")
    # 只验证库版号规则：构造一个不触碰文件的空计算结果
    result = CommitResult(version=3, mode="data_change")
    with pytest.raises(BackfillError, match="必须先提交版本 0"):
        commit_result(db, parsed, result)


# --------------------------------------------------------------------------- #
# add/remove 模式
# --------------------------------------------------------------------------- #

def test_add_remove_daily_aggregation(db):
    f0 = parquet_bytes([("2026-11-01", 100), ("2026-11-01", 200),
                        ("2026-11-02", 300)])
    bootstrap(db, {p(0): f0}, [add(p(0))])
    state = get_state(db)
    assert state["daily"] == [
        {"order_day": "2026-11-01", "order_count": 2, "amount_total_cents": 300},
        {"order_day": "2026-11-02", "order_count": 1, "amount_total_cents": 300},
    ]

    f1 = parquet_bytes([("2026-11-01", 50)])
    parsed = parse_commit_file(1, commit_bytes(1, [remove(p(0)), add(p(1))]),
                               "00000000000000000001.json")
    result = compute_commit(parsed, {p(0): f0, p(1): f1}, "v1.zip")
    state = commit_result(db, parsed, result)
    assert state["last_version"] == 1
    by_day = {d["order_day"]: d for d in state["daily"]}
    # day1: 2 单 300 分被扣掉，再补 1 单 50 分
    assert by_day["2026-11-01"] == {"order_day": "2026-11-01",
                                    "order_count": 1, "amount_total_cents": 50}
    assert by_day["2026-11-02"] == {"order_day": "2026-11-02",
                                    "order_count": 0, "amount_total_cents": 0}


def test_datachange_false_actions_are_skipped(db):
    # 压缩提交：dataChange=false 的 add/remove 不应影响汇总
    f0 = parquet_bytes([("2026-11-01", 100)])
    f1 = parquet_bytes([("2026-11-01", 100)])
    bootstrap(db, {p(0): f0}, [add(p(0))])
    parsed = parse_commit_file(
        1, commit_bytes(1, [add(p(1), data_change=False),
                            remove(p(0), data_change=False)]),
        "00000000000000000001.json")
    result = compute_commit(parsed, {p(0): f0, p(1): f1}, "v1.zip")
    assert result.no_change is True
    state = commit_result(db, parsed, result)
    assert state["last_version"] == 1
    assert state["daily"] == [
        {"order_day": "2026-11-01", "order_count": 1, "amount_total_cents": 100}]


def test_missing_referenced_parquet_names_file(db):
    bootstrap(db)
    parsed = parse_commit_file(1, commit_bytes(1, [add(p(1))]),
                               "00000000000000000001.json")
    with pytest.raises(BackfillError, match=p(1)):
        compute_commit(parsed, {}, "v1.zip")


# --------------------------------------------------------------------------- #
# CDC 模式
# --------------------------------------------------------------------------- #

def test_cdc_mode_ignores_add_remove_and_applies_signs(db):
    cdc_path = "_change_data/cdc-00001.parquet"
    rows = [
        ("2026-11-01", 100, "insert"),
        ("2026-11-02", 200, "update_postimage"),
        ("2026-11-02", 80, "update_preimage"),
        ("2026-11-03", 90, "delete"),
    ]
    bootstrap(db)
    parsed = parse_commit_file(1, commit_bytes(1, [
        remove(p(0)), add(p(1)), cdc(cdc_path),
    ]), "00000000000000000001.json")
    members = {
        cdc_path: cdc_bytes(rows),
        p(0): parquet_bytes([("2026-11-01", 9999)]),  # 必须被忽略
        p(1): parquet_bytes([("2026-11-01", 9999)]),  # 必须被忽略
    }
    result = compute_commit(parsed, members, "v1.zip")
    assert result.mode == "cdc"
    assert result.files_read == [cdc_path]
    state = commit_result(db, parsed, result)
    by_day = {d["order_day"]: d for d in state["daily"]}
    assert by_day["2026-11-01"]["order_count"] == 1
    assert by_day["2026-11-01"]["amount_total_cents"] == 100
    assert by_day["2026-11-02"]["order_count"] == 0
    assert by_day["2026-11-02"]["amount_total_cents"] == 120
    assert by_day["2026-11-03"]["order_count"] == -1
    assert by_day["2026-11-03"]["amount_total_cents"] == -90


def test_cdc_unknown_change_type_rejected(db):
    cdc_path = "_change_data/cdc.parquet"
    bootstrap(db)
    parsed = parse_commit_file(1, commit_bytes(1, [cdc(cdc_path)]),
                               "00000000000000000001.json")
    bad = cdc_bytes([("2026-11-01", 10, "upsert")])
    with pytest.raises(BackfillError, match="_change_type"):
        compute_commit(parsed, {cdc_path: bad}, "v1.zip")


# --------------------------------------------------------------------------- #
# Parquet 坏数据：指出文件且不改动状态
# --------------------------------------------------------------------------- #

def test_bad_parquet_reported_with_member_name(db):
    bootstrap(db)
    parsed = parse_commit_file(1, commit_bytes(1, [add(p(1))]),
                               "00000000000000000001.json")
    with pytest.raises(BackfillError, match=p(1)):
        compute_commit(parsed, {p(1): b"not parquet"}, "v1.zip")
    # 状态停留在 v0
    assert get_state(db)["last_version"] == 0


def test_missing_columns_rejected(db):
    bad = parquet_bytes([("2026-11-01", 10)],
                        schema=pa.schema([("order_day", pa.string()),
                                          ("other", pa.int64())]),
                        col_map=("order_day", "other"))
    bootstrap(db)
    parsed = parse_commit_file(1, commit_bytes(1, [add(p(1))]),
                               "00000000000000000001.json")
    with pytest.raises(BackfillError, match="amount_cents"):
        compute_commit(parsed, {p(1): bad}, "v1.zip")


def test_wrong_column_types_rejected(db):
    # order_day 非字符串
    bad_day = parquet_bytes(
        [(20261101, 10)],
        schema=pa.schema([("order_day", pa.int64()),
                          ("amount_cents", pa.int64())]),
        col_map=("order_day", "amount_cents"))
    bootstrap(db)
    parsed = parse_commit_file(1, commit_bytes(1, [add(p(1))]),
                               "00000000000000000001.json")
    with pytest.raises(BackfillError, match="order_day"):
        compute_commit(parsed, {p(1): bad_day}, "v1.zip")

    # amount_cents 非 int64
    bad_amt = parquet_bytes(
        [("2026-11-01", 1.5)],
        schema=pa.schema([("order_day", pa.string()),
                          ("amount_cents", pa.float64())]),
        col_map=("order_day", "amount_cents"))
    with pytest.raises(BackfillError, match="int64"):
        compute_commit(parsed, {p(1): bad_amt}, "v1.zip")


def test_null_values_and_bad_dates_rejected(db):
    bootstrap(db)
    parsed = parse_commit_file(1, commit_bytes(1, [add(p(1))]),
                               "00000000000000000001.json")
    null_day = parquet_bytes(
        [(None, 10)],
        schema=ORDER_SCHEMA, col_map=("order_day", "amount_cents"))
    with pytest.raises(BackfillError, match="order_day 为空"):
        compute_commit(parsed, {p(1): null_day}, "v1.zip")

    null_amt = parquet_bytes(
        [("2026-11-01", None)],
        schema=ORDER_SCHEMA, col_map=("order_day", "amount_cents"))
    with pytest.raises(BackfillError, match="amount_cents 为空"):
        compute_commit(parsed, {p(1): null_amt}, "v1.zip")

    bad_date = parquet_bytes([("2026-13-40", 10)])
    with pytest.raises(BackfillError, match="合法日历日期"):
        compute_commit(parsed, {p(1): bad_date}, "v1.zip")
    assert get_state(db)["last_version"] == 0


# --------------------------------------------------------------------------- #
# 版号推进、断档、重复、事务原子性
# --------------------------------------------------------------------------- #

def test_only_next_version_accepted_and_duplicate_not_double_counted(db):
    f0 = parquet_bytes([("2026-11-01", 100)])
    _, r0 = bootstrap(db, {p(0): f0}, [add(p(0))])

    parsed = parse_commit_file(1, commit_bytes(1, [add(p(1))]),
                               "00000000000000000001.json")
    result = compute_commit(parsed, {p(1): parquet_bytes([("2026-11-01", 50)])},
                            "v1.zip")
    commit_result(db, parsed, result)

    # 同版并发式重提：拒绝，不重复入账
    with pytest.raises(BackfillError, match="已经提交过"):
        commit_result(db, parsed, result)
    by_day = {d["order_day"]: d for d in get_state(db)["daily"]}
    assert by_day["2026-11-01"]["order_count"] == 2
    assert by_day["2026-11-01"]["amount_total_cents"] == 150

    # 跳版拒绝
    parsed9 = parse_commit_file(9, commit_bytes(9, [
        {"commitInfo": {"operation": "OPTIMIZE"}}]),
        "00000000000000000009.json")
    with pytest.raises(BackfillError, match="版号断档"):
        commit_result(db, parsed9, CommitResult(version=9, mode="data_change"))
    assert get_state(db)["last_version"] == 1


def test_no_change_commit_advances_version(db):
    bootstrap(db)  # v0 无数据
    parsed = parse_commit_file(1, commit_bytes(1, [
        {"commitInfo": {"operation": "OPTIMIZE"}}]),
        "00000000000000000001.json")
    result = compute_commit(parsed, {}, "v1.zip")
    assert result.no_change is True
    state = commit_result(db, parsed, result)
    assert state["last_version"] == 1
    assert [v["version"] for v in state["versions"]] == [0, 1]


def test_concurrent_same_version_only_one_commits(db, tmp_path):
    f0 = parquet_bytes([("2026-11-01", 100)])
    bootstrap(db, {p(0): f0}, [add(p(0))])
    db_path = tmp_path / "backfill.db"
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def worker():
        conn = connect(str(db_path))
        try:
            parsed = parse_commit_file(
                1, commit_bytes(1, [add(p(1))]),
                "00000000000000000001.json")
            result = compute_commit(
                parsed, {p(1): parquet_bytes([("2026-11-01", 50)])}, "v1.zip")
            barrier.wait()
            commit_result(conn, parsed, result)
            outcomes.append("committed")
        except BackfillError as exc:
            outcomes.append(f"rejected:{exc}")
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(o.split(":")[0] for o in outcomes) == ["committed", "rejected"]
    state = get_state(db)
    assert state["last_version"] == 1
    by_day = {d["order_day"]: d for d in state["daily"]}
    # 只入了一次账：100 + 50
    assert by_day["2026-11-01"] == {"order_day": "2026-11-01",
                                    "order_count": 2, "amount_total_cents": 150}


def test_reset_state_clears_everything(db):
    f0 = parquet_bytes([("2026-11-01", 100)])
    bootstrap(db, {p(0): f0}, [add(p(0))])
    reset_state(db)
    state = get_state(db)
    assert state == {"last_version": None, "daily": [], "versions": []}


# --------------------------------------------------------------------------- #
# process_upload 端到端（文件名 + ZIP）
# --------------------------------------------------------------------------- #

def test_process_upload_end_to_end(db):
    members = {p(0): parquet_bytes([("2026-11-01", 100)])}
    commit = commit_upload(0, [PROTOCOL, METADATA, add(p(0))])
    zf = UploadFile(filename="v0.zip", data=zip_bytes(members))
    result = process_upload(commit, zf)
    assert result.version == 0
    parsed = parse_commit_file(0, commit.data, commit.filename)
    state = commit_result(db, parsed, result)
    assert state["last_version"] == 0
    assert state["daily"][0]["amount_total_cents"] == 100


def test_process_upload_bad_commit_filename():
    with pytest.raises(BackfillError, match="20 位补零"):
        process_upload(UploadFile("3.json", b"{}"),
                       UploadFile("v3.zip", zip_bytes({})))
