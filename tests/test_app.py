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
    conn.executescript("DELETE FROM queries; DELETE FROM day_state; DELETE FROM passengers;")
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
