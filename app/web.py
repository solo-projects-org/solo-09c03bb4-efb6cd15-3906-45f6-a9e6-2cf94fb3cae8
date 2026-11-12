"""Flask 网页：Delta Lake 版本可恢复性分析 + 订单日汇总补传。

两块功能共用同一服务：

- **回退核查**（``/analyze``）：上传 JSON 提交 / 检查点 / 现存清单，逐版
  重放并核对快照引用文件；
- **补传入口**（``/backfill``）：回退核查页下半部的表单，逐版上传一个
  Delta JSON 提交和一个按表内路径打包的 Parquet ZIP，把净变化累加进 SQLite
  日汇总，版号与汇总在同一事务提交，同版并发不重复入账。

页面也可一键加载仓库内置的本地样例，补传样例覆盖跨日更正（删单日、
插新日）与无变更提交。
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from .analyzer import AnalyzerError, InputFile, analyze
from .backfill import (
    BackfillError,
    UploadFile,
    commit_result,
    connect,
    get_state,
    parse_commit_file,
    process_upload,
    reset_state,
)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024  # 单次请求上限 256MB
# 错误消息是中文，API 直接输出 UTF-8 而非 \uXXXX 转义，便于页面与断言读取
app.json.ensure_ascii = False

SAMPLE_ROOT = Path(os.environ.get("SAMPLE_ROOT",
                                  Path(__file__).resolve().parent.parent / "samples"))
DB_PATH = os.environ.get(
    "BACKFILL_DB",
    str(Path(__file__).resolve().parent.parent / "data" / "backfill.db"),
)

# 样例目录名 -> 页面展示名 / 默认版本区间
SAMPLES = {
    "healthy_table": {
        "label": "健康表样例（0-4 版均可重建）",
        "range": (0, 4),
    },
    "incident_table": {
        "label": "误删事故样例（部分版本缺提交或缺数据文件）",
        "range": (0, 5),
    },
}

# 补传样例：一张从 v0 启用 CDF、无分区 / 列映射 / 删除向量的订单表
BACKFILL_SAMPLE = "backfill_orders"


def _read_uploads(field_name: str) -> list[InputFile]:
    files = []
    for storage in request.files.getlist(field_name):
        if not storage or storage.filename == "":
            continue
        files.append(InputFile(filename=storage.filename,
                               data=storage.read()))
    return files


def _read_single_upload(field_name: str, label: str) -> UploadFile:
    storage = request.files.get(field_name)
    if storage is None or storage.filename == "":
        raise BackfillError(f"请上传{label}")
    return UploadFile(filename=storage.filename, data=storage.read())


def _parse_version(raw: str, field_label: str) -> int:
    raw = (raw or "").strip()
    if raw == "":
        raise AnalyzerError(f"请填写{field_label}")
    try:
        return int(raw)
    except ValueError as exc:
        raise AnalyzerError(f"{field_label}必须是整数，收到：{raw!r}") from exc


def _run_analysis(log_files, manifest_files, from_version, to_version):
    return analyze(log_files=log_files,
                   manifest_files=manifest_files,
                   from_version=from_version,
                   to_version=to_version)


# --------------------------------------------------------------------------- #
# 页面
# --------------------------------------------------------------------------- #

@app.get("/")
def index():
    return render_template("index.html", samples=SAMPLES,
                           backfill_state=get_state(connect(DB_PATH)))


# --------------------------------------------------------------------------- #
# 回退核查
# --------------------------------------------------------------------------- #

@app.post("/analyze")
def analyze_upload():
    try:
        from_version = _parse_version(request.form.get("from_version"), "起始版本")
        to_version = _parse_version(request.form.get("to_version"), "结束版本")
        log_files = _read_uploads("log_files")
        manifest_files = _read_uploads("manifest_files")
        report = _run_analysis(log_files, manifest_files,
                               from_version, to_version)
    except AnalyzerError as exc:
        return render_template("index.html", samples=SAMPLES, error=str(exc),
                               form=request.form,
                               backfill_state=get_state(connect(DB_PATH))), 400
    return render_template("index.html", samples=SAMPLES, report=report,
                           backfill_state=get_state(connect(DB_PATH)))


@app.get("/api/samples")
def list_samples():
    return jsonify({
        key: {"label": meta["label"], "range": list(meta["range"])}
        for key, meta in SAMPLES.items()
    })


@app.post("/api/analyze-sample/<sample_name>")
def analyze_sample(sample_name: str):
    """直接加载服务器本地样例目录，无需手动上传。"""
    if sample_name not in SAMPLES:
        return jsonify({"error": f"未知样例：{sample_name}"}), 404
    sample_dir = SAMPLE_ROOT / sample_name
    if not sample_dir.is_dir():
        return jsonify({"error": f"样例目录不存在：{sample_dir}"}), 404

    try:
        payload = request.get_json(silent=True) or {}
        meta = SAMPLES[sample_name]
        from_version = int(payload.get("from_version", meta["range"][0]))
        to_version = int(payload.get("to_version", meta["range"][1]))

        log_dir = sample_dir / "_delta_log"
        log_files = []
        for path in sorted(log_dir.iterdir()):
            if path.is_file():
                log_files.append(InputFile(filename=path.name,
                                           data=path.read_bytes()))
        manifest_path = sample_dir / "existing_files.txt"
        manifest_files = [InputFile(
            filename=manifest_path.name,
            data=manifest_path.read_bytes(),
        )]
        report = _run_analysis(log_files, manifest_files,
                               from_version, to_version)
    except AnalyzerError as exc:
        return jsonify({"error": str(exc)}), 400
    except (OSError, ValueError) as exc:
        return jsonify({"error": f"读取样例失败：{exc}"}), 500
    return jsonify(report)


# --------------------------------------------------------------------------- #
# 订单日汇总补传
# --------------------------------------------------------------------------- #

def _backfill_page(exc: BackfillError | None = None, status: int = 400):
    payload = {"samples": SAMPLES,
               "backfill_state": get_state(connect(DB_PATH))}
    if exc is not None:
        payload["backfill_error"] = str(exc)
        return render_template("index.html", **payload), status
    return render_template("index.html", **payload)


@app.get("/api/backfill/state")
def backfill_state_api():
    return jsonify(get_state(connect(DB_PATH)))


@app.post("/backfill")
def backfill_upload():
    conn = None
    try:
        commit_upload = _read_single_upload("commit_file", "Delta JSON 提交（NNN.json）")
        zip_upload = _read_single_upload("parquet_zip", "Parquet ZIP 包")

        # 1) 全部文件解析 / 校验在事务外完成：坏数据不改变任何已提交状态
        result = process_upload(commit_upload, zip_upload)
        version = result.version
        parsed = parse_commit_file(
            version, commit_upload.data,
            commit_upload.filename.rsplit("/", 1)[-1],
        )

        # 2) 版号 + 日汇总在单个 SQLite 事务中一起提交
        conn = connect(DB_PATH)
        state = commit_result(conn, parsed, result)
    except BackfillError as exc:
        return _backfill_page(exc)
    except sqlite3.Error as exc:
        app.logger.exception("补传库写入失败")
        return _backfill_page(BackfillError(f"补传库写入失败，状态未改变：{exc}"), 500)
    finally:
        if conn is not None:
            conn.close()
    return render_template("index.html", samples=SAMPLES,
                           backfill_state=state,
                           backfill_success=result)


def _sample_backfill_paths(sample_dir: Path, version: int):
    """定位样例中某版的 JSON 提交与 Parquet ZIP。"""
    commit_path = sample_dir / "_delta_log" / f"{version:020d}.json"
    zip_path = sample_dir / "uploads" / f"v{version}.zip"
    if not commit_path.is_file():
        raise BackfillError(f"样例缺少提交文件：{commit_path.name}")
    if not zip_path.is_file():
        raise BackfillError(f"样例缺少补传 ZIP：{zip_path.name}")
    return commit_path, zip_path


@app.post("/api/backfill/sample/<int:version>")
def backfill_sample(version: int):
    """用本地样例材料补传指定版本，效果与手工上传完全一致。"""
    sample_dir = SAMPLE_ROOT / BACKFILL_SAMPLE
    if not sample_dir.is_dir():
        return jsonify({"error": f"补传样例目录不存在：{sample_dir}"}), 404
    conn = None
    try:
        commit_path, zip_path = _sample_backfill_paths(sample_dir, version)
        commit_upload = UploadFile(filename=commit_path.name,
                                   data=commit_path.read_bytes())
        zip_upload = UploadFile(filename=zip_path.name,
                                data=zip_path.read_bytes())
        result = process_upload(commit_upload, zip_upload)
        parsed = parse_commit_file(version, commit_upload.data, commit_path.name)
        conn = connect(DB_PATH)
        state = commit_result(conn, parsed, result)
    except BackfillError as exc:
        return jsonify({"error": str(exc)}), 400
    except (OSError, sqlite3.Error) as exc:
        return jsonify({"error": f"样例补传失败：{exc}"}), 500
    finally:
        if conn is not None:
            conn.close()
    return jsonify({
        "state": state,
        "applied": {
            "version": result.version,
            "mode": result.mode,
            "no_change": result.no_change,
            "files_read": result.files_read,
            "deltas": {day: {"order_count": d[0],
                             "amount_total_cents": d[1]}
                       for day, d in sorted(result.deltas.items())},
        },
    })


@app.post("/api/backfill/reset")
def backfill_reset():
    """清空补传库（仅用于本地样例演示重新补传）。"""
    conn = None
    try:
        conn = connect(DB_PATH)
        reset_state(conn)
        state = get_state(conn)
    except sqlite3.Error as exc:
        return jsonify({"error": f"重置失败：{exc}"}), 500
    finally:
        if conn is not None:
            conn.close()
    return jsonify(state)


@app.errorhandler(413)
def too_large(_err):
    return render_template("index.html", samples=SAMPLES,
                           error="上传内容超过 256MB 上限",
                           backfill_state=get_state(connect(DB_PATH))), 413


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
