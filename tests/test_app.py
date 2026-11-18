import os
import sqlite3
import tempfile
import threading

import pytest

_tmpdir = tempfile.mkdtemp(prefix="metrodp-")
os.environ["APP_DB"] = os.path.join(_tmpdir, "test.db")

import app as app_module  # noqa: E402

CSV_V1 = "乘客ID,进站,出站\np1,A,X\np2,A,X\np3,B,X\np4,A,Y\np5,C,Z\n"
CSV_V2 = "乘客ID,进站,出站\np1,A,X\np2,B,Y\n"


@pytest.fixture(autouse=True)
def clean_db():
    conn = sqlite3.connect(os.environ["APP_DB"])
    conn.executescript(
        "DELETE FROM queries; DELETE FROM day_state; DELETE FROM passengers;"
        "DELETE FROM station_alias; DELETE FROM mapping_version;"
        "UPDATE mapping_meta SET version = 0, updated_at = NULL, alias_count = 0;")
    conn.close()
    yield


@pytest.fixture
def client():
    return app_module.app.test_client()


def upload(client, date, csv_text):
    return client.post("/upload", data={"date": date, "csv": csv_text})


def query(client, date, ins, outs):
    return client.post("/query", json={"date": date, "in_stations": ins,
                                       "out_stations": outs})


def status(client, date):
    return client.get(f"/api/status?date={date}").get_json()


def publish(client, csv_text):
    return client.post("/mapping/publish", data={"csv": csv_text})


def stations(client, date):
    return client.get(f"/api/stations?date={date}").get_json()["stations"]


def db_rows(date):
    conn = sqlite3.connect(os.environ["APP_DB"])
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT passenger_id, in_station, out_station, in_station_norm,"
        " out_station_norm, mapping_version FROM passengers"
        " WHERE date = ? ORDER BY passenger_id", (date,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


MAP_V1 = "别名,标准站名\n西直门站,西直门\n国贸站,国贸\n"
MAP_V2 = "别名,标准站名\n西直门站,西直门\n国贸站,国贸\n望京站,望京\n"


# ------------------------------------------------------------- 上传与锁定

def test_upload_ok_and_replace_before_lock(client):
    r = upload(client, "2026-11-01", CSV_V1)
    assert r.status_code == 200 and r.get_json()["rows"] == 5
    # 锁定前可整日替换
    r = upload(client, "2026-11-01", CSV_V2)
    assert r.status_code == 200 and r.get_json()["rows"] == 2
    st = status(client, "2026-11-01")
    assert st["rows"] == 2 and not st["locked"]


def test_upload_rejected_after_lock(client):
    upload(client, "2026-11-02", CSV_V1)
    r = query(client, "2026-11-02", ["A"], ["X"])
    assert r.status_code == 200
    r = upload(client, "2026-11-02", CSV_V2)
    assert r.status_code == 409
    assert "锁定" in r.get_json()["error"]
    # 源数据未被改动
    assert status(client, "2026-11-02")["rows"] == 5


def test_duplicate_passenger_rejected(client):
    r = upload(client, "2026-11-03", "乘客ID,进站,出站\np1,A,X\np1,B,Y\n")
    assert r.status_code == 400
    assert "重复" in r.get_json()["error"]


def test_csv_without_header_and_english_header(client):
    r = upload(client, "2026-11-04", "p1,A,X\np2,B,Y\n")
    assert r.status_code == 200 and r.get_json()["rows"] == 2
    r = upload(client, "2026-11-05", "passenger_id,in_station,out_station\np1,A,X\n")
    assert r.status_code == 200 and r.get_json()["rows"] == 1


# ------------------------------------------------------------- 站名归一

def test_upload_keeps_raw_and_normalizes_with_current_mapping(client):
    assert publish(client, MAP_V1).status_code == 200
    r = upload(client, "2026-12-01", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    assert r.status_code == 200
    row = db_rows("2026-12-01")[0]
    assert row["in_station"] == "西直门站"          # 原始站名保留
    assert row["out_station"] == "国贸站"
    assert row["in_station_norm"] == "西直门"        # 归一后站名
    assert row["out_station_norm"] == "国贸"
    assert stations(client, "2026-12-01") == ["国贸", "西直门"]


def test_publish_recomputes_unlocked_dates_from_raw_names(client):
    upload(client, "2026-12-02", "乘客ID,进站,出站\np1,西直门站,国贸站\np2,望京站,国贸站\n")
    # 未发布映射时, 站名保持原名
    assert stations(client, "2026-12-02") == ["国贸站", "望京站", "西直门站"]
    r = publish(client, MAP_V1).get_json()
    assert r["ok"] and "2026-12-02" in r["changed_dates"]
    assert stations(client, "2026-12-02") == ["国贸", "望京站", "西直门"]
    # 再次发布新增别名: 仍从原始站名重算
    r = publish(client, MAP_V2).get_json()
    assert r["ok"] and "2026-12-02" in r["changed_dates"]
    assert stations(client, "2026-12-02") == ["国贸", "望京", "西直门"]
    rows = db_rows("2026-12-02")
    assert {r["in_station"] for r in rows} == {"西直门站", "望京站"}


def test_replace_mapping_removes_old_alias_and_uses_raw_name(client):
    upload(client, "2026-12-03", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    publish(client, MAP_V1)
    assert stations(client, "2026-12-03") == ["国贸", "西直门"]
    # 替换为不含"西直门站"的新映射: 该站回到原始站名, 而不是保留旧归一结果
    r = publish(client, "别名,标准站名\n国贸站,国贸\n").get_json()
    assert r["ok"]
    assert stations(client, "2026-12-03") == ["国贸", "西直门站"]


def test_locked_date_keeps_rows_stations_and_results(client):
    upload(client, "2026-12-04", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    publish(client, MAP_V1)
    first = query(client, "2026-12-04", ["西直门"], ["国贸"]).get_json()
    assert first["noisy_count"] is not None
    before_rows = db_rows("2026-12-04")
    before_stations = stations(client, "2026-12-04")
    before_queries = client.get("/api/queries?date=2026-12-04").get_json()

    # 新映射会改写该已锁定日期的站名, 但仍必须原样跳过
    r = publish(client, "别名,标准站名\n西直门站,西直门广场\n国贸站,国贸\n").get_json()
    assert r["ok"]
    assert "2026-12-04" in r["locked_dates"]
    assert "2026-12-04" in r["skipped_changed_dates"]
    assert "2026-12-04" not in r["changed_dates"]
    assert "2026-12-04" not in r["affected_dates"]
    assert db_rows("2026-12-04") == before_rows
    assert stations(client, "2026-12-04") == before_stations
    assert client.get("/api/queries?date=2026-12-04").get_json() == before_queries
    # 已发布结果不被重算: 相同查询仍复用原值
    again = query(client, "2026-12-04", ["西直门"], ["国贸"]).get_json()
    assert again["cached"] is True
    assert again["noisy_count"] == first["noisy_count"]


def test_upload_after_publish_uses_new_mapping_but_keeps_raw(client):
    publish(client, MAP_V1)
    upload(client, "2026-12-05", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    publish(client, MAP_V2)
    upload(client, "2026-12-05", "乘客ID,进站,出站\np1,望京站,国贸站\n")
    rows = db_rows("2026-12-05")
    assert rows[0]["in_station"] == "望京站"
    assert rows[0]["in_station_norm"] == "望京"
    assert stations(client, "2026-12-05") == ["国贸", "望京"]


def test_invalid_mapping_keeps_old_mapping_and_data(client):
    upload(client, "2026-12-06", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    publish(client, MAP_V1)
    before = db_rows("2026-12-06")

    # 别名重复
    r = publish(client, "别名,标准站名\n西直门站,西直门\n西直门站,国贸\n")
    assert r.status_code == 400 and "别名重复" in r.get_json()["error"]
    # 标准站名又作别名
    r = publish(client, "别名,标准站名\n西直门站,西直门\n西直门,西直门\n")
    assert r.status_code == 400 and "不能再作为别名" in r.get_json()["error"]
    # 空字段
    assert publish(client, "别名,标准站名\n,西直门\n").status_code == 400

    assert db_rows("2026-12-06") == before
    assert client.get("/api/mapping").get_json()["version"] == 1
    assert client.get("/api/mapping").get_json()["alias_count"] == 2


def test_mapping_publish_failure_rolls_back(client, monkeypatch):
    upload(client, "2026-12-07", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    publish(client, MAP_V1)
    before = db_rows("2026-12-07")

    def boom(*_a, **_k):
        raise RuntimeError("模拟写入失败")

    monkeypatch.setattr(app_module, "recompute_unlocked", boom)
    r = publish(client, MAP_V2)
    assert r.status_code == 500 and r.get_json()["ok"] is False
    # 映射与数据均回滚
    info = client.get("/api/mapping").get_json()
    assert info["version"] == 1 and info["alias_count"] == 2
    assert db_rows("2026-12-07") == before
    assert stations(client, "2026-12-07") == ["国贸", "西直门"]


def test_restart_reconciles_unlocked_dates_only(client):
    upload(client, "2026-12-08", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    upload(client, "2026-12-09", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    publish(client, MAP_V1)
    query(client, "2026-12-08", ["西直门"], ["国贸"])
    # 模拟崩溃遗留: 未锁定日期的归一数据停留在旧映射
    conn = sqlite3.connect(os.environ["APP_DB"])
    conn.execute(
        "UPDATE passengers SET in_station_norm = '西直门站',"
        " out_station_norm = '国贸站', mapping_version = 0 WHERE date = ?",
        ("2026-12-09",))
    conn.commit()
    conn.close()
    app_module.init_db()
    app_module._reconcile_on_startup()
    # 未锁定日期改用当前映射, 不混用
    assert stations(client, "2026-12-09") == ["国贸", "西直门"]
    # 已锁定日期保持原状
    assert stations(client, "2026-12-08") == ["国贸", "西直门"]
    assert db_rows("2026-12-08")[0]["in_station_norm"] == "西直门"
    # 既有预算不受影响
    assert status(client, "2026-12-08")["budget_spent"] == 0.5


def test_mapping_page_served(client):
    assert client.get("/mapping").status_code == 200


def test_query_accepts_alias_and_canonical_alike(client):
    publish(client, MAP_V1)
    upload(client, "2026-12-12",
           "乘客ID,进站,出站\np1,西直门站,国贸站\np2,西直门,国贸\n")
    # 别名与标准站名归一到同一集合, 不会把同一站拆开
    by_alias = query(client, "2026-12-12", ["西直门站"], ["国贸站"]).get_json()
    assert by_alias["ok"]
    assert by_alias["in_stations"] == ["西直门"]
    assert by_alias["out_stations"] == ["国贸"]
    by_canonical = query(client, "2026-12-12", ["西直门"], ["国贸"]).get_json()
    assert by_canonical["cached"] is True
    assert by_canonical["noisy_count"] == by_alias["noisy_count"]
    # 只扣一次费
    assert status(client, "2026-12-12")["budget_spent"] == 0.5


def test_locked_date_queries_use_snapshot_mapping(client):
    publish(client, MAP_V1)
    upload(client, "2026-12-13", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    first = query(client, "2026-12-13", ["西直门"], ["国贸"]).get_json()
    publish(client, "别名,标准站名\n西直门站,西直门新站\n")
    # 锁定日期仍按锁定时的映射理解查询站名: 新别名不该改写旧日期语义
    again = query(client, "2026-12-13", ["西直门"], ["国贸"]).get_json()
    assert again["cached"] is True and again["noisy_count"] == first["noisy_count"]
    # 新映射下的标准站名在锁定日期上不会凭空命中
    r = query(client, "2026-12-13", ["西直门新站"], ["国贸"])
    assert r.status_code in (200, 403)


def test_concurrent_publish_upload_query_never_mixes_versions(client):
    """发布与上传、查询交错: 每个日期只能落在完整的旧映射或新映射上."""
    upload(client, "2026-12-10", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    upload(client, "2026-12-11", "乘客ID,进站,出站\np1,西直门站,国贸站\n")
    publish(client, MAP_V1)
    query(client, "2026-12-11", ["西直门"], ["国贸"])
    locked_before = db_rows("2026-12-11")

    errors = []

    def publisher(n):
        c = app_module.app.test_client()
        try:
            body = publish(c, MAP_V1 if n % 2 == 0 else MAP_V2).get_json()
            if not body["ok"]:
                errors.append(body)
        except Exception as exc:  # pragma: no cover - 失败时给出线索
            errors.append(repr(exc))

    def uploader(n):
        c = app_module.app.test_client()
        try:
            r = upload(c, "2026-12-10",
                       "乘客ID,进站,出站\np1,西直门站,国贸站\np2,望京站,望京站\n")
            if r.status_code not in (200, 409):
                errors.append(r.get_json())
        except Exception as exc:  # pragma: no cover
            errors.append(repr(exc))

    def querier():
        c = app_module.app.test_client()
        try:
            r = query(c, "2026-12-10", ["望京"], ["望京"])
            if r.status_code not in (200, 403):
                errors.append(r.get_json())
        except Exception as exc:  # pragma: no cover
            errors.append(repr(exc))

    threads = ([threading.Thread(target=publisher, args=(i,)) for i in range(8)]
               + [threading.Thread(target=uploader, args=(i,)) for i in range(8)]
               + [threading.Thread(target=querier) for _ in range(4)])
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    conn = sqlite3.connect(os.environ["APP_DB"])
    conn.row_factory = sqlite3.Row
    version = conn.execute(
        "SELECT version FROM mapping_meta WHERE id = 1").fetchone()["version"]
    mixed = conn.execute(
        "SELECT COUNT(*) AS c FROM passengers p"
        " WHERE NOT EXISTS (SELECT 1 FROM day_state d"
        "                   WHERE d.date = p.date AND d.locked = 1)"
        " AND p.mapping_version <> ?", (version,)).fetchone()["c"]
    conn.close()
    if not status(client, "2026-12-10")["locked"]:
        assert mixed == 0
    # 已锁定日期保持发布前的快照
    assert db_rows("2026-12-11") == locked_before



# ------------------------------------------------------------- 查询与缓存

def test_query_returns_noisy_only(client):
    upload(client, "2026-11-06", CSV_V1)
    r = query(client, "2026-11-06", ["A"], ["X"])
    body = r.get_json()
    assert r.status_code == 200 and body["ok"]
    assert isinstance(body["noisy_count"], float)
    assert "true_count" not in body and "count" not in body
    assert status(client, "2026-11-06")["budget_spent"] == 0.5
    assert status(client, "2026-11-06")["locked"]


def test_same_query_reused_regardless_of_order_and_duplicates(client):
    upload(client, "2026-11-07", CSV_V1)
    r1 = query(client, "2026-11-07", ["A", "B"], ["X", "Y"]).get_json()
    r2 = query(client, "2026-11-07", ["B", "A", "A"], ["Y", "X", "X"]).get_json()
    assert r1["noisy_count"] == r2["noisy_count"]
    assert r2["cached"] is True
    # 第二次未扣费
    assert status(client, "2026-11-07")["budget_spent"] == 0.5


def test_budget_limit_four_queries(client):
    upload(client, "2026-11-08", CSV_V1)
    ok, rejected = 0, 0
    for i in range(6):
        r = query(client, "2026-11-08", [f"S{i}"], ["X"])
        if r.status_code == 200:
            ok += 1
        else:
            rejected += 1
            assert r.status_code == 403
            assert "预算不足" in r.get_json()["error"]
    assert ok == 4 and rejected == 2  # 2 / 0.5 = 4
    assert status(client, "2026-11-08")["budget_spent"] == 2.0
    # 预算用完后, 已缓存的查询仍然返回原值
    r = query(client, "2026-11-08", ["S0"], ["X"])
    assert r.status_code == 200 and r.get_json()["cached"] is True


def test_budget_is_per_date(client):
    upload(client, "2026-11-09", CSV_V1)
    upload(client, "2026-11-10", CSV_V1)
    for _ in range(4):
        assert query(client, "2026-11-09", [f"S{_}"], ["X"]).status_code == 200
    # 另一个日期仍有完整预算
    assert query(client, "2026-11-10", ["S0"], ["X"]).status_code == 200


# ------------------------------------------------------------- 并发

def test_concurrent_distinct_queries_never_overspend(client):
    upload(client, "2026-11-11", CSV_V1)
    results = []

    def worker(i):
        c = app_module.app.test_client()
        results.append(query(c, "2026-11-11", [f"T{i}"], ["X"]).status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(200) == 4
    assert results.count(403) == 8
    assert status(client, "2026-11-11")["budget_spent"] == 2.0


def test_concurrent_identical_query_single_result(client):
    upload(client, "2026-11-12", CSV_V1)
    bodies = []

    def worker():
        c = app_module.app.test_client()
        bodies.append(query(c, "2026-11-12", ["A", "B"], ["X"]).get_json())

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(b["ok"] for b in bodies)
    assert len({b["noisy_count"] for b in bodies}) == 1  # 只有一份结果
    assert status(client, "2026-11-12")["budget_spent"] == 0.5  # 只扣一次
    conn = sqlite3.connect(os.environ["APP_DB"])
    n = conn.execute("SELECT COUNT(*) FROM queries WHERE date='2026-11-12'").fetchone()[0]
    conn.close()
    assert n == 1


# ------------------------------------------------------------- 持久化

def test_restart_keeps_results_and_budget(client):
    upload(client, "2026-11-13", CSV_V1)
    r1 = query(client, "2026-11-13", ["A"], ["X"]).get_json()
    # 模拟重启: 重新初始化数据库连接(文件不变)
    app_module.init_db()
    client2 = app_module.app.test_client()
    r2 = query(client2, "2026-11-13", ["A"], ["X"]).get_json()
    assert r2["cached"] is True and r2["noisy_count"] == r1["noisy_count"]
    assert status(client2, "2026-11-13")["budget_spent"] == 0.5
    # 重启后上传仍被拒绝
    assert upload(client2, "2026-11-13", CSV_V2).status_code == 409


# ------------------------------------------------------------- 噪声分布

def test_laplace_noise_distribution():
    samples = [app_module.laplace_noise() for _ in range(20000)]
    mean = sum(samples) / len(samples)
    var = sum((s - mean) ** 2 for s in samples) / len(samples)
    assert abs(mean) < 0.1            # 均值 ≈ 0
    assert abs(var - 8.0) < 0.8       # Laplace(0,2) 方差 = 2 * 2^2 = 8
