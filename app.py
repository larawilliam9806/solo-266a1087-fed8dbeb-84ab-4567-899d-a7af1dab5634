"""地铁首程客流差分隐私查询服务.

- 按日期上传 CSV(乘客ID, 进站, 出站), 每位乘客仅一行
- 选择进/出站集合查询匹配人数, 结果加 Laplace(0, 2) 噪声(安全随机源)
- 每次新查询扣 0.5 隐私预算, 每日(每个数据日期)总额 2
- 相同日期 + 相同站点集合(与顺序、重复无关)复用原结果, 不再扣费
- 首个成功查询锁定当天源数据, 之后拒绝替换
- 扣费 / 保存结果 / 锁定源数据在同一事务中, 要么全部成功要么全部不生效
"""

import csv
import io
import json
import math
import os
import secrets
import sqlite3
from datetime import datetime, timezone

from flask import Flask, g, jsonify, render_template, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("APP_DB", os.path.join(BASE_DIR, "app.db"))

DAILY_BUDGET = 2.0   # 每日隐私预算总额
QUERY_COST = 0.5     # 每次新查询的预算开销
NOISE_SCALE = 2.0    # Laplace(0, 2)

app = Flask(__name__)
_rng = secrets.SystemRandom()  # 安全随机源

SCHEMA = """
CREATE TABLE IF NOT EXISTS passengers (
    date         TEXT NOT NULL,
    passenger_id TEXT NOT NULL,
    in_station   TEXT NOT NULL,
    out_station  TEXT NOT NULL,
    PRIMARY KEY (date, passenger_id)
);

CREATE TABLE IF NOT EXISTS day_state (
    date         TEXT PRIMARY KEY,
    locked       INTEGER NOT NULL DEFAULT 0,
    budget_spent REAL    NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS queries (
    date         TEXT NOT NULL,
    in_key       TEXT NOT NULL,
    out_key      TEXT NOT NULL,
    in_stations  TEXT NOT NULL,
    out_stations TEXT NOT NULL,
    noisy_count  REAL NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (date, in_key, out_key)
);
"""


def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
    finally:
        conn.close()


init_db()


def get_db():
    """每个请求一个连接; autocommit 模式, 事务用显式 BEGIN IMMEDIATE."""
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


# ---------------------------------------------------------------- 工具函数

def laplace_noise(scale=NOISE_SCALE):
    """用安全随机源生成 Laplace(0, scale) 噪声(逆变换采样)."""
    u = _rng.random() - 0.5  # [-0.5, 0.5)
    if u == 0.0:
        return 0.0
    return -scale * math.copysign(1.0, u) * math.log1p(-2.0 * abs(u))


def normalize_date(raw):
    """校验并规范化日期为 YYYY-MM-DD; 非法返回 None."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def canon_key(stations):
    """站点集合的规范化表示: 去空白、去重、排序, 与顺序和重复选择无关."""
    return json.dumps(sorted({s.strip() for s in stations if s and s.strip()}),
                      ensure_ascii=False)


def parse_station_list(value):
    """接受 JSON 数组或分隔符(逗号/顿号/分号/空白/换行)分隔的字符串."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        items = [str(v) for v in value]
    else:
        text = str(value)
        for sep in ("，", "、", "；", ";", "\n", "\t", " "):
            text = text.replace(sep, ",")
        items = text.split(",")
    seen, out = set(), []
    for item in items:
        s = item.strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


HEADER_ALIASES = {
    "passenger": {"乘客id", "passenger_id", "passengerid", "passenger", "id", "乘客编号"},
    "in": {"进站", "in_station", "instation", "entry", "entry_station", "入站", "进站站点"},
    "out": {"出站", "out_station", "outstation", "exit", "exit_station", "出站站点"},
}


def parse_csv_text(text):
    """解析 CSV 文本, 返回 [(passenger_id, in_station, out_station), ...].

    支持带表头(中/英文列名)或无表头(按 乘客ID,进站,出站 顺序)两种格式.
    乘客ID 重复或字段为空时抛出 ValueError.
    """
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(cell.strip() for cell in r)]
    if not rows:
        raise ValueError("CSV 内容为空")

    first = [c.strip().lower() for c in rows[0]]
    col_idx = {}
    for field, aliases in HEADER_ALIASES.items():
        for i, name in enumerate(first):
            if name in aliases:
                col_idx[field] = i
                break

    has_header = len(col_idx) == 3
    if not has_header:
        if col_idx:
            raise ValueError("CSV 表头无法完整识别, 请使用: 乘客ID, 进站, 出站")
        if len(first) < 3:
            raise ValueError("CSV 至少需要 3 列: 乘客ID, 进站, 出站")
        col_idx = {"passenger": 0, "in": 1, "out": 2}

    data_rows = rows[1:] if has_header else rows
    result, seen = [], set()
    for offset, r in enumerate(data_rows):
        lineno = offset + (2 if has_header else 1)
        if max(col_idx.values()) >= len(r):
            raise ValueError(f"第 {lineno} 行列数不足")
        pid = r[col_idx["passenger"]].strip()
        in_s = r[col_idx["in"]].strip()
        out_s = r[col_idx["out"]].strip()
        if not pid or not in_s or not out_s:
            raise ValueError(f"第 {lineno} 行存在空字段")
        if pid in seen:
            raise ValueError(f"乘客ID 重复: {pid}(每位乘客仅允许一行)")
        seen.add(pid)
        result.append((pid, in_s, out_s))
    if not result:
        raise ValueError("CSV 无有效数据行")
    return result


def get_payload():
    if request.is_json:
        body = request.get_json(silent=True)
        if isinstance(body, dict):
            return body
    return request.form


# ---------------------------------------------------------------- 页面

@app.get("/")
def index():
    return render_template("index.html",
                           daily_budget=DAILY_BUDGET, query_cost=QUERY_COST)


# ---------------------------------------------------------------- 上传

@app.post("/upload")
def upload():
    payload = get_payload()
    date = normalize_date(payload.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确(应为 YYYY-MM-DD)"), 400

    text = None
    file = request.files.get("file")
    if file is not None and file.filename:
        try:
            text = file.read().decode("utf-8-sig")
        except UnicodeDecodeError:
            return jsonify(ok=False, error="CSV 文件编码必须是 UTF-8"), 400
    elif payload.get("csv"):
        text = payload.get("csv")
    if text is None:
        return jsonify(ok=False, error="请上传 CSV 文件或提供 csv 文本"), 400

    try:
        rows = parse_csv_text(text)
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400

    conn = get_db()
    conn.execute("BEGIN IMMEDIATE")
    try:
        state = conn.execute(
            "SELECT locked FROM day_state WHERE date = ?", (date,)).fetchone()
        if state and state["locked"]:
            conn.execute("ROLLBACK")
            return jsonify(ok=False,
                         error=f"{date} 的源数据已被首个成功查询锁定, 拒绝替换"), 409
        # 未锁定: 整日替换
        conn.execute("DELETE FROM passengers WHERE date = ?", (date,))
        conn.executemany(
            "INSERT INTO passengers (date, passenger_id, in_station, out_station)"
            " VALUES (?, ?, ?, ?)",
            [(date, pid, in_s, out_s) for pid, in_s, out_s in rows])
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    return jsonify(ok=True, date=date, rows=len(rows),
                   message=f"已替换 {date} 的源数据, 共 {len(rows)} 行")


# ---------------------------------------------------------------- 查询

@app.post("/query")
def query():
    payload = get_payload()
    date = normalize_date(payload.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确(应为 YYYY-MM-DD)"), 400

    in_stations = parse_station_list(payload.get("in_stations"))
    out_stations = parse_station_list(payload.get("out_stations"))
    if not in_stations or not out_stations:
        return jsonify(ok=False, error="进站集合与出站集合均不能为空"), 400

    in_key, out_key = canon_key(in_stations), canon_key(out_stations)
    conn = get_db()

    # 快速路径: 已存在的结果直接复用, 不扣费(只读, 无需写事务)
    cached = conn.execute(
        "SELECT noisy_count, in_stations, out_stations FROM queries"
        " WHERE date = ? AND in_key = ? AND out_key = ?",
        (date, in_key, out_key)).fetchone()
    if cached:
        return jsonify(ok=True, date=date,
                       in_stations=json.loads(cached["in_stations"]),
                       out_stations=json.loads(cached["out_stations"]),
                       noisy_count=cached["noisy_count"], cached=True)

    # 新查询: 扣费 + 保存结果 + 锁定源数据必须在同一事务中
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 事务内复查, 保证并发下相同查询只生成一份结果
        cached = conn.execute(
            "SELECT noisy_count, in_stations, out_stations FROM queries"
            " WHERE date = ? AND in_key = ? AND out_key = ?",
            (date, in_key, out_key)).fetchone()
        if cached:
            conn.execute("COMMIT")
            return jsonify(ok=True, date=date,
                           in_stations=json.loads(cached["in_stations"]),
                           out_stations=json.loads(cached["out_stations"]),
                           noisy_count=cached["noisy_count"], cached=True)

        state = conn.execute(
            "SELECT budget_spent FROM day_state WHERE date = ?", (date,)).fetchone()
        spent = state["budget_spent"] if state else 0.0
        if spent + QUERY_COST > DAILY_BUDGET + 1e-9:
            conn.execute("ROLLBACK")
            return jsonify(ok=False,
                           error=f"{date} 当日隐私预算不足"
                                 f"(已用 {spent:g}/{DAILY_BUDGET:g}, 本次需 {QUERY_COST:g}),"
                                 " 不返回新结果"), 403

        placeholders_in = ",".join("?" for _ in in_stations)
        placeholders_out = ",".join("?" for _ in out_stations)
        true_count = conn.execute(
            f"SELECT COUNT(*) AS c FROM passengers"
            f" WHERE date = ? AND in_station IN ({placeholders_in})"
            f" AND out_station IN ({placeholders_out})",
            [date, *in_stations, *out_stations]).fetchone()["c"]

        noisy_count = true_count + laplace_noise()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO queries (date, in_key, out_key, in_stations, out_stations,"
            " noisy_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (date, in_key, out_key, json.dumps(in_stations, ensure_ascii=False),
             json.dumps(out_stations, ensure_ascii=False), noisy_count, now))
        conn.execute(
            "INSERT INTO day_state (date, locked, budget_spent) VALUES (?, 1, ?)"
            " ON CONFLICT(date) DO UPDATE SET locked = 1,"
            " budget_spent = budget_spent + excluded.budget_spent",
            (date, QUERY_COST))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    return jsonify(ok=True, date=date, in_stations=in_stations,
                   out_stations=out_stations, noisy_count=noisy_count,
                   cached=False,
                   budget_remaining=round(DAILY_BUDGET - spent - QUERY_COST, 10))


# ---------------------------------------------------------------- 状态接口

@app.get("/api/stations")
def api_stations():
    date = normalize_date(request.args.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确"), 400
    conn = get_db()
    rows = conn.execute(
        "SELECT in_station AS s FROM passengers WHERE date = ?"
        " UNION SELECT out_station FROM passengers WHERE date = ? ORDER BY s",
        (date, date)).fetchall()
    return jsonify(ok=True, date=date, stations=[r["s"] for r in rows])


@app.get("/api/status")
def api_status():
    date = normalize_date(request.args.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确"), 400
    conn = get_db()
    state = conn.execute(
        "SELECT locked, budget_spent FROM day_state WHERE date = ?", (date,)).fetchone()
    rows = conn.execute(
        "SELECT COUNT(*) AS c FROM passengers WHERE date = ?", (date,)).fetchone()["c"]
    spent = state["budget_spent"] if state else 0.0
    return jsonify(ok=True, date=date,
                   locked=bool(state and state["locked"]),
                   rows=rows,
                   budget_spent=spent,
                   budget_remaining=round(DAILY_BUDGET - spent, 10),
                   daily_budget=DAILY_BUDGET, query_cost=QUERY_COST)


@app.get("/api/queries")
def api_queries():
    date = normalize_date(request.args.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确"), 400
    conn = get_db()
    rows = conn.execute(
        "SELECT in_stations, out_stations, noisy_count, created_at"
        " FROM queries WHERE date = ? ORDER BY created_at, rowid", (date,)).fetchall()
    return jsonify(ok=True, date=date, queries=[
        {"in_stations": json.loads(r["in_stations"]),
         "out_stations": json.loads(r["out_stations"]),
         "noisy_count": r["noisy_count"],
         "created_at": r["created_at"]} for r in rows])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), threaded=True)
