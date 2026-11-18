"""地铁首程客流差分隐私查询服务.

- 按日期上传 CSV(乘客ID, 进站, 出站), 每位乘客仅一行
- 每行同时保留原始站名与按当前别名映射归一后的站名
- 上传全量站名别名映射(别名, 标准站名), 一次发布原子地替换映射并
  按原始站名重算所有未锁定日期; 已锁定日期保持原状
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
    date              TEXT NOT NULL,
    passenger_id      TEXT NOT NULL,
    in_station        TEXT NOT NULL,   -- 原始进站名(永不改写)
    out_station       TEXT NOT NULL,   -- 原始出站名(永不改写)
    in_station_norm   TEXT,            -- 按映射归一后的进站名
    out_station_norm  TEXT,            -- 按映射归一后的出站名
    mapping_version   INTEGER,         -- 归一所依据的映射版本
    PRIMARY KEY (date, passenger_id)
);

CREATE TABLE IF NOT EXISTS day_state (
    date            TEXT PRIMARY KEY,
    locked          INTEGER NOT NULL DEFAULT 0,
    budget_spent    REAL    NOT NULL DEFAULT 0,
    mapping_version INTEGER NOT NULL DEFAULT 0  -- 锁定时固定, 该日期只认这份映射
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

-- 别名映射: 别名唯一, 标准站名不能再作为别名(后者由发布逻辑校验).
-- 全局只保留一份"当前生效"的完整映射, 发布即整表替换并递增 version.
CREATE TABLE IF NOT EXISTS station_alias (
    alias      TEXT PRIMARY KEY,
    canonical  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mapping_meta (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    version     INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT,
    alias_count INTEGER NOT NULL DEFAULT 0
);

-- 映射历史快照: 已锁定日期按锁定时的版本归一查询站名, 使结果语义一致.
CREATE TABLE IF NOT EXISTS mapping_version (
    version     INTEGER PRIMARY KEY,
    snapshot    TEXT NOT NULL,        -- JSON {别名: 标准站名}
    created_at  TEXT NOT NULL
);
"""

# 旧库升级: passengers 早期只有原始站名两列, 这里补齐归一列并回填.
MIGRATIONS = (
    ("passengers", "in_station_norm", "TEXT"),
    ("passengers", "out_station_norm", "TEXT"),
    ("passengers", "mapping_version", "INTEGER"),
    ("day_state", "mapping_version", "INTEGER NOT NULL DEFAULT 0"),
)


def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
        for table, column, ctype in MIGRATIONS:
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if column not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ctype}")
        conn.execute(
            "INSERT OR IGNORE INTO mapping_meta (id, version, alias_count)"
            " VALUES (1, 0, 0)")
        # v0 = 空映射(原始站名即标准站名), 作为基线快照
        conn.execute(
            "INSERT OR IGNORE INTO mapping_version (version, snapshot, created_at)"
            " VALUES (0, '{}', ?)",
            (datetime.now(timezone.utc).isoformat(),))
        # 迁移后缺失归一列的旧行: 用空映射(仅去空白)回填, 不改变任何站名语义
        conn.execute(
            "UPDATE passengers SET"
            " in_station_norm = TRIM(in_station),"
            " out_station_norm = TRIM(out_station),"
            " mapping_version = 0"
            " WHERE in_station_norm IS NULL OR out_station_norm IS NULL")
    finally:
        conn.close()


init_db()


def _reconcile_on_startup():
    """进程启动时兜底: 未锁定日期必须与当前生效映射一致."""
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    try:
        conn.row_factory = sqlite3.Row
        reconcile_mapping(conn)
    finally:
        conn.close()


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


# ---------------------------------------------------------------- 别名映射

ALIAS_HEADER_ALIASES = {
    "alias": {"别名", "alias", "station_alias", "原站名", "曾用名", "旧站名"},
    "canonical": {"标准站名", "标准名", "canonical", "canonical_name",
                  "standard", "standard_name", "正式站名", "现站名"},
}


def parse_alias_csv_text(text):
    """解析全量别名映射 CSV, 返回 [(alias, canonical), ...].

    支持带表头(别名, 标准站名 / alias, canonical)或无表头(按两列顺序).
    别名重复、字段为空、标准站名同时作为别名时抛出 ValueError.
    校验是全局的: 标准站名集合与别名集合必须互不相交。
    """
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(cell.strip() for cell in r)]
    if not rows:
        raise ValueError("映射 CSV 内容为空")

    first = [c.strip().lower() for c in rows[0]]
    col_idx = {}
    for field, aliases in ALIAS_HEADER_ALIASES.items():
        for i, name in enumerate(first):
            if name in aliases:
                col_idx[field] = i
                break

    has_header = len(col_idx) == 2
    if not has_header:
        if col_idx:
            raise ValueError("映射 CSV 表头无法完整识别, 请使用: 别名, 标准站名")
        if len(first) < 2:
            raise ValueError("映射 CSV 至少需要 2 列: 别名, 标准站名")
        col_idx = {"alias": 0, "canonical": 1}

    data_rows = rows[1:] if has_header else rows
    entries, alias_seen = [], set()
    for offset, r in enumerate(data_rows):
        lineno = offset + (2 if has_header else 1)
        if max(col_idx.values()) >= len(r):
            raise ValueError(f"第 {lineno} 行列数不足")
        alias = r[col_idx["alias"]].strip()
        canonical = r[col_idx["canonical"]].strip()
        if not alias or not canonical:
            raise ValueError(f"第 {lineno} 行存在空字段")
        if alias in alias_seen:
            raise ValueError(f"别名重复: {alias}(别名必须唯一)")
        alias_seen.add(alias)
        entries.append((alias, canonical))

    if not entries:
        raise ValueError("映射 CSV 无有效数据行")

    canonical_set = {c for _, c in entries}
    clashes = sorted(canonical_set & alias_seen)
    if clashes:
        raise ValueError(
            "标准站名不能再作为别名: " + "、".join(clashes))
    return entries


def load_mapping(conn):
    """读取当前生效的完整别名映射 {别名: 标准站名}."""
    return {r["alias"]: r["canonical"]
            for r in conn.execute("SELECT alias, canonical FROM station_alias")}


def load_mapping_version(conn, version):
    """读取指定版本的历史映射快照.

    已锁定日期只认锁定时的快照; 快照不存在时返回空映射(即不改变站名),
    绝不退回当前映射, 以免把新映射与旧数据混用.
    """
    if version is None:
        return {}
    row = conn.execute(
        "SELECT snapshot FROM mapping_version WHERE version = ?",
        (version,)).fetchone()
    return json.loads(row["snapshot"]) if row else {}


def current_mapping_version(conn):
    row = conn.execute(
        "SELECT version FROM mapping_meta WHERE id = 1").fetchone()
    return row["version"] if row else 0


def apply_mapping(name, mapping):
    """单层归一: 命中别名则换成标准站名, 否则保持原名."""
    return mapping.get(name, name)


def recompute_unlocked(conn, mapping, version, dates=None):
    """按给定映射重算未锁定日期的归一列(始终从原始站名出发).

    锁定日期绝不触碰. 返回归一结果发生实际变化的日期列表.
    """
    sql = (
        "SELECT p.date, p.passenger_id, p.in_station, p.out_station,"
        " p.in_station_norm, p.out_station_norm, p.mapping_version"
        " FROM passengers p"
        " WHERE NOT EXISTS (SELECT 1 FROM day_state d"
        "                   WHERE d.date = p.date AND d.locked = 1)")
    params = []
    if dates is not None:
        if not dates:
            return []
        sql += " AND p.date IN (%s)" % ",".join("?" for _ in dates)
        params.extend(dates)
    sql += " ORDER BY p.date, p.passenger_id"

    changed_dates = set()
    updates = []
    for r in conn.execute(sql, params):
        new_in = apply_mapping(r["in_station"], mapping)
        new_out = apply_mapping(r["out_station"], mapping)
        if (new_in != r["in_station_norm"]
                or new_out != r["out_station_norm"]):
            changed_dates.add(r["date"])
        if (new_in != r["in_station_norm"]
                or new_out != r["out_station_norm"]
                or r["mapping_version"] != version):
            updates.append((new_in, new_out, version,
                            r["date"], r["passenger_id"]))
    if updates:
        conn.executemany(
            "UPDATE passengers SET in_station_norm = ?, out_station_norm = ?,"
            " mapping_version = ? WHERE date = ? AND passenger_id = ?",
            updates)
    return sorted(changed_dates)


def reconcile_mapping(conn):
    """启动自检: 未锁定日期若与当前映射版本不一致, 用当前映射整体重算.

    SQLite 事务保证发布映射与重算同生共死; 这里是额外兜底, 防止任何遗留的
    "新映射 + 旧归一数据" 混用. 不触碰已锁定日期, 也不改动预算.

    版本、映射与待修日期全部在同一写事务内读取, 避免与并发的映射发布
    交错时用旧映射覆盖新结果.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        version = current_mapping_version(conn)
        rows = conn.execute(
            "SELECT DISTINCT p.date FROM passengers p"
            " WHERE NOT EXISTS (SELECT 1 FROM day_state d"
            "                   WHERE d.date = p.date AND d.locked = 1)"
            " AND (p.mapping_version IS NULL OR p.mapping_version <> ?)",
            (version,)).fetchall()
        dates = [r["date"] for r in rows]
        if dates:
            mapping = load_mapping(conn)
            # 仅重写版本不一致(或归一结果不同)的未锁定行; 已锁定行不进入该查询
            recompute_unlocked(conn, mapping, version)
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------- 页面

@app.get("/")
def index():
    return render_template("index.html",
                           daily_budget=DAILY_BUDGET, query_cost=QUERY_COST)


@app.get("/mapping")
def mapping_page():
    return render_template("mapping.html")


# ---------------------------------------------------------------- 上传

def _read_upload_text(payload):
    """从上传文件或文本字段读取 UTF-8 内容; 返回 (text, error)."""
    file = request.files.get("file")
    if file is not None and file.filename:
        try:
            return file.read().decode("utf-8-sig"), None
        except UnicodeDecodeError:
            return None, "CSV 文件编码必须是 UTF-8"
    elif payload.get("csv"):
        return payload.get("csv"), None
    return None, "请上传 CSV 文件或提供 csv 文本"


@app.post("/upload")
def upload():
    payload = get_payload()
    date = normalize_date(payload.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确(应为 YYYY-MM-DD)"), 400

    text, err = _read_upload_text(payload)
    if err:
        return jsonify(ok=False, error=err), 400

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
        # 未锁定: 整日替换. 原始站名原样保存, 同时按当前映射归一.
        mapping = load_mapping(conn)
        version = current_mapping_version(conn)
        conn.execute("DELETE FROM passengers WHERE date = ?", (date,))
        conn.executemany(
            "INSERT INTO passengers (date, passenger_id, in_station, out_station,"
            " in_station_norm, out_station_norm, mapping_version)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(date, pid, in_s, out_s,
              apply_mapping(in_s, mapping), apply_mapping(out_s, mapping), version)
             for pid, in_s, out_s in rows])
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    return jsonify(ok=True, date=date, rows=len(rows), mapping_version=version,
                   message=f"已替换 {date} 的源数据, 共 {len(rows)} 行"
                           f"(按映射 v{version} 归一)")


@app.post("/mapping/publish")
def publish_mapping():
    """发布全量别名映射: 替换整份映射, 并从原始站名重算所有未锁定日期.

    校验失败 → 400, 原映射与数据保持不变.
    校验通过 → 单事务内替换映射 + 重算; 任一步失败整体回滚, 原映射与数据不变.
    """
    payload = get_payload()
    text, err = _read_upload_text(payload)
    if err:
        return jsonify(ok=False, error=err), 400

    try:
        entries = parse_alias_csv_text(text)
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400

    mapping = dict(entries)
    conn = get_db()
    conn.execute("BEGIN IMMEDIATE")
    try:
        new_version = current_mapping_version(conn) + 1
        conn.execute("DELETE FROM station_alias")
        conn.executemany(
            "INSERT INTO station_alias (alias, canonical) VALUES (?, ?)",
            entries)
        changed_dates = recompute_unlocked(conn, mapping, new_version)

        # 本次事务内, 未锁定日期必须全部落在新映射版本上, 否则视为发布失败.
        inconsistent = conn.execute(
            "SELECT COUNT(*) AS c FROM passengers p"
            " WHERE NOT EXISTS (SELECT 1 FROM day_state d"
            "                   WHERE d.date = p.date AND d.locked = 1)"
            " AND (p.mapping_version IS NULL OR p.mapping_version <> ?)",
            (new_version,)).fetchone()["c"]
        if inconsistent:
            raise RuntimeError("映射重算未覆盖全部未锁定行")

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO mapping_version (version, snapshot, created_at)"
            " VALUES (?, ?, ?)",
            (new_version, json.dumps(mapping, ensure_ascii=False), now))
        conn.execute(
            "UPDATE mapping_meta SET version = ?, updated_at = ?, alias_count = ?"
            " WHERE id = 1", (new_version, now, len(entries)))
        conn.execute("COMMIT")
    except Exception as exc:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        app.logger.exception("发布站名映射失败")
        return jsonify(ok=False,
                       error=f"映射发布失败, 已保留原映射与数据({exc})"), 500

    affected = [r["date"] for r in conn.execute(
        "SELECT DISTINCT date FROM passengers"
        " WHERE NOT EXISTS (SELECT 1 FROM day_state d"
        "                   WHERE d.date = passengers.date AND d.locked = 1)"
        " ORDER BY date")]
    locked = [r["date"] for r in conn.execute(
        "SELECT date FROM day_state WHERE locked = 1 ORDER BY date")]
    # 锁定日期中, 若按新映射会发生变化则计入"跳过"; 但仍不改动任何一行
    skipped = set()
    for r in conn.execute(
            "SELECT p.date, p.in_station, p.out_station, p.in_station_norm,"
            " p.out_station_norm FROM passengers p"
            " JOIN day_state d ON d.date = p.date AND d.locked = 1"):
        if (apply_mapping(r["in_station"], mapping) != r["in_station_norm"]
                or apply_mapping(r["out_station"], mapping)
                != r["out_station_norm"]):
            skipped.add(r["date"])

    return jsonify(ok=True, version=new_version, alias_count=len(entries),
                   changed_dates=changed_dates,
                   affected_dates=affected,
                   locked_dates=locked,
                   skipped_changed_dates=sorted(skipped),
                   message=f"已发布映射 v{new_version}, 共 {len(entries)} 条别名;"
                           f"重算 {len(affected)} 个未锁定日期,"
                           f"其中 {len(changed_dates)} 个日期归一结果有变化;"
                           f"跳过 {len(locked)} 个已锁定日期"
                           f"(其中 {len(skipped)} 个若重算会变化)")


@app.get("/api/mapping")
def api_mapping():
    conn = get_db()
    rows = conn.execute(
        "SELECT alias, canonical FROM station_alias ORDER BY alias").fetchall()
    meta = conn.execute(
        "SELECT version, updated_at, alias_count FROM mapping_meta"
        " WHERE id = 1").fetchone()
    return jsonify(ok=True,
                   version=meta["version"] if meta else 0,
                   updated_at=meta["updated_at"] if meta else None,
                   alias_count=meta["alias_count"] if meta else 0,
                   entries=[{"alias": r["alias"], "canonical": r["canonical"]}
                            for r in rows])


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

    conn = get_db()

    def resolve(conn):
        """确定该日期应使用的映射及其版本.

        已锁定日期固定使用锁定时的映射快照(其后发布的映射不再影响它);
        未锁定日期使用当前映射. 两种情况都只会得到一份完整映射.
        """
        state = conn.execute(
            "SELECT locked, mapping_version FROM day_state WHERE date = ?",
            (date,)).fetchone()
        if state and state["locked"]:
            version = state["mapping_version"]
            return version, load_mapping_version(conn, version)
        # 未锁定: 当前版本号与映射表在同一事务内读取, 二者始终一致
        return current_mapping_version(conn), load_mapping(conn)

    def canonicalize(mapping):
        """把合作方提交的站名按该日期的映射归一, 保证查询同一站不会拆开."""
        return ([apply_mapping(s, mapping) for s in in_stations],
                [apply_mapping(s, mapping) for s in out_stations])

    # 快速路径: 已存在的结果直接复用, 不扣费. 用只读事务读取, 保证映射版本与
    # 缓存行来自同一快照.
    conn.execute("BEGIN")
    try:
        _, mapping = resolve(conn)
        norm_in, norm_out = canonicalize(mapping)
        in_key, out_key = canon_key(norm_in), canon_key(norm_out)
        cached = conn.execute(
            "SELECT noisy_count, in_stations, out_stations FROM queries"
            " WHERE date = ? AND in_key = ? AND out_key = ?",
            (date, in_key, out_key)).fetchone()
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    if cached:
        return jsonify(ok=True, date=date,
                       in_stations=json.loads(cached["in_stations"]),
                       out_stations=json.loads(cached["out_stations"]),
                       noisy_count=cached["noisy_count"], cached=True)

    # 新查询: 扣费 + 保存结果 + 锁定源数据必须在同一事务中
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 事务内复查, 保证并发下相同查询只生成一份结果
        version, mapping = resolve(conn)
        norm_in, norm_out = canonicalize(mapping)
        in_key, out_key = canon_key(norm_in), canon_key(norm_out)
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

        placeholders_in = ",".join("?" for _ in norm_in)
        placeholders_out = ",".join("?" for _ in norm_out)
        # 查询按归一后的站名匹配; 未锁定日期已按当前映射整体重算, 锁定日期
        # 保留首次查询时的归一快照, 因此同一日期只会使用同一份完整映射.
        true_count = conn.execute(
            f"SELECT COUNT(*) AS c FROM passengers"
            f" WHERE date = ? AND in_station_norm IN ({placeholders_in})"
            f" AND out_station_norm IN ({placeholders_out})",
            [date, *norm_in, *norm_out]).fetchone()["c"]

        noisy_count = true_count + laplace_noise()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO queries (date, in_key, out_key, in_stations, out_stations,"
            " noisy_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (date, in_key, out_key, json.dumps(norm_in, ensure_ascii=False),
             json.dumps(norm_out, ensure_ascii=False), noisy_count, now))
        conn.execute(
            "INSERT INTO day_state (date, locked, budget_spent, mapping_version)"
            " VALUES (?, 1, ?, ?)"
            " ON CONFLICT(date) DO UPDATE SET locked = 1,"
            " budget_spent = budget_spent + excluded.budget_spent,"
            " mapping_version = excluded.mapping_version",
            (date, QUERY_COST, version))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    return jsonify(ok=True, date=date, in_stations=norm_in,
                   out_stations=norm_out, noisy_count=noisy_count,
                   cached=False, mapping_version=version,
                   budget_remaining=round(DAILY_BUDGET - spent - QUERY_COST, 10))


# ---------------------------------------------------------------- 状态接口

@app.get("/api/stations")
def api_stations():
    date = normalize_date(request.args.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确"), 400
    conn = get_db()
    rows = conn.execute(
        "SELECT in_station_norm AS s FROM passengers WHERE date = ?"
        " UNION SELECT out_station_norm FROM passengers WHERE date = ? ORDER BY s",
        (date, date)).fetchall()
    return jsonify(ok=True, date=date, stations=[r["s"] for r in rows])


@app.get("/api/status")
def api_status():
    date = normalize_date(request.args.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确"), 400
    conn = get_db()
    state = conn.execute(
        "SELECT locked, budget_spent, mapping_version FROM day_state"
        " WHERE date = ?", (date,)).fetchone()
    rows = conn.execute(
        "SELECT COUNT(*) AS c FROM passengers WHERE date = ?", (date,)).fetchone()["c"]
    spent = state["budget_spent"] if state else 0.0
    current_version = current_mapping_version(conn)
    # 锁定日期沿用锁定时的映射快照, 未锁定日期使用当前映射
    effective_version = (state["mapping_version"]
                         if state and state["locked"] else current_version)
    return jsonify(ok=True, date=date,
                   locked=bool(state and state["locked"]),
                   rows=rows,
                   mapping_version=effective_version,
                   current_mapping_version=current_version,
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


_reconcile_on_startup()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), threaded=True)
