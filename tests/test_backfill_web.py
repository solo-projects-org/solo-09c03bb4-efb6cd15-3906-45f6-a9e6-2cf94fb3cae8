"""补传 Flask 路由层测试（含本地样例逐版补传）。"""

from __future__ import annotations

import io
import json
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app import web
from app.backfill import connect


ORDER_SCHEMA = pa.schema([
    ("order_day", pa.string()),
    ("amount_cents", pa.int64()),
])


def _parquet(rows):
    table = pa.table({
        "order_day": pa.array([r[0] for r in rows], type=pa.string()),
        "amount_cents": pa.array([r[1] for r in rows], type=pa.int64()),
    }, schema=ORDER_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _zip(members):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _commit(version, actions):
    return ("\n".join(json.dumps(a) for a in actions)).encode("utf-8")


PROTOCOL = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
METADATA = {"metadata": {"id": "t", "partitionColumns": [],
                         "configuration": {"delta.enableChangeDataFeed": "true"}}}


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "bf.db")
    monkeypatch.setattr(web, "DB_PATH", db_path)
    web.connect(db_path).close()
    return web.app.test_client()


def _upload(client, version, actions, members):
    return client.post(
        "/backfill",
        data={
            "commit_file": (io.BytesIO(_commit(version, actions)),
                            f"{version:020d}.json"),
            "parquet_zip": (io.BytesIO(_zip(members)), f"v{version}.zip"),
        },
        content_type="multipart/form-data",
    )


def test_index_shows_backfill_entry(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "订单日汇总补传".encode() in resp.data
    assert "补传库为空".encode() in resp.data


def test_backfill_upload_happy_path_and_state_api(client):
    resp = _upload(client, 0, [PROTOCOL, METADATA,
                               {"add": {"path": "data/a.parquet",
                                        "dataChange": True}}],
                   {"data/a.parquet": _parquet([("2026-11-01", 100),
                                                ("2026-11-02", 200)])})
    assert resp.status_code == 200, resp.data
    assert "已提交 v0".encode() in resp.data

    state = client.get("/api/backfill/state").get_json()
    assert state["last_version"] == 0
    assert state["daily"] == [
        {"order_day": "2026-11-01", "order_count": 1, "amount_total_cents": 100},
        {"order_day": "2026-11-02", "order_count": 1, "amount_total_cents": 200},
    ]

    # v1 断档检查：传 v2 应 400 且状态不变
    bad = _upload(client, 2, [{"add": {"path": "data/b.parquet"}}],
                  {"data/b.parquet": _parquet([("2026-11-01", 5)])})
    assert bad.status_code == 400
    assert "版号断档".encode() in bad.data
    assert client.get("/api/backfill/state").get_json()["last_version"] == 0


def test_backfill_missing_file_400_and_unchanged(client):
    _upload(client, 0, [PROTOCOL, METADATA,
                        {"add": {"path": "data/a.parquet"}}],
            {"data/a.parquet": _parquet([("2026-11-01", 100)])})
    resp = _upload(client, 1, [{"add": {"path": "data/missing.parquet"}}], {})
    assert resp.status_code == 400
    assert "data/missing.parquet".encode() in resp.data
    assert client.get("/api/backfill/state").get_json()["last_version"] == 0


def test_backfill_bad_parquet_400_names_file(client):
    _upload(client, 0, [PROTOCOL, METADATA,
                        {"add": {"path": "data/a.parquet"}}],
            {"data/a.parquet": _parquet([("2026-11-01", 100)])})
    resp = _upload(client, 1, [{"add": {"path": "data/bad.parquet"}}],
                   {"data/bad.parquet": b"not parquet"})
    assert resp.status_code == 400
    assert "data/bad.parquet".encode() in resp.data


def test_backfill_missing_form_fields_400(client):
    resp = client.post("/backfill", data={}, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "JSON 提交".encode() in resp.data


def test_sample_backfill_full_sequence(client):
    # 逐版补传内置样例 v0-v4
    for version in range(5):
        resp = client.post(f"/api/backfill/sample/{version}")
        assert resp.status_code == 200, resp.data
    state = client.get("/api/backfill/state").get_json()
    assert state["last_version"] == 4
    by_day = {d["order_day"]: d for d in state["daily"]}
    # v0+v1: day1 两单(1000+1500)，v2 跨日更正后 day1 仅留 1000 一单
    assert by_day["2026-11-01"] == {"order_day": "2026-11-01",
                                    "order_count": 1, "amount_total_cents": 1000}
    # day2: v0 一单 2000 + v2 净 +1 单 2500 + v4 一单 500
    assert by_day["2026-11-02"]["order_count"] == 3
    assert by_day["2026-11-02"]["amount_total_cents"] == 5000
    assert by_day["2026-11-03"]["order_count"] == 2
    assert by_day["2026-11-03"]["amount_total_cents"] == 3700
    modes = {v["version"]: v["mode"] for v in state["versions"]}
    assert modes == {0: "data_change", 1: "data_change", 2: "cdc",
                     3: "data_change", 4: "data_change"}
    assert state["versions"][3]["no_change"] is True

    # 再点一次“下一版”（v5 不存在）应 400
    resp = client.post("/api/backfill/sample/5")
    assert resp.status_code == 400
    assert "样例缺少提交文件".encode() in resp.data

    # 重置后回到空库
    reset = client.post("/api/backfill/reset")
    assert reset.status_code == 200
    assert reset.get_json()["last_version"] is None
    # 重置后必须能从 v0 重新补传
    again = client.post("/api/backfill/sample/0")
    assert again.status_code == 200
    assert again.get_json()["state"]["last_version"] == 0


def test_sample_backfill_out_of_order_rejected(client):
    client.post("/api/backfill/sample/0")
    resp = client.post("/api/backfill/sample/2")
    assert resp.status_code == 400
    assert "版号断档".encode() in resp.data
