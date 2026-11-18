# 地铁每日首程客流 · 差分隐私查询平台

地铁数据分析员向合作研究方提供每日首程客流统计的网页服务。基于 **Flask + SQLite**,
对每次新查询的结果添加 **Laplace(0, 2)** 噪声(安全随机源 `secrets.SystemRandom`),
并按 **每次 0.5、每日总额 2** 的隐私预算计费。查询页只显示加噪结果。

## 快速启动

### Docker Compose(推荐)

```bash
docker compose up --build
```

访问 http://localhost:8000 。SQLite 数据库保存在名为 `appdata` 的卷中,
容器重启后已发布结果、预算消耗与锁定状态均不丢失。

### 本地运行

```bash
pip install -r requirements.txt
python app.py            # 开发模式, 监听 8000 端口
# 或: gunicorn --bind 0.0.0.0:8000 --workers 2 --threads 8 app:app
```

数据库路径由环境变量 `APP_DB` 指定(默认 `./app.db`, 容器内为 `/data/app.db`)。

## 功能与规则

| 规则 | 实现 |
| --- | --- |
| 按日期上传 CSV(乘客ID, 进站, 出站), 每位乘客仅一行 | `POST /upload`, 乘客ID 重复即拒绝; 支持中/英文表头或无表头 |
| 锁定前可整日替换, 首个成功查询后拒绝替换 | `day_state.locked`, 替换与锁定检查在同一事务内 |
| 选择进/出站集合查询匹配人数 | `POST /query`, 集合与顺序、重复选择无关(排序去重后规范化) |
| 新查询加 Laplace(0, 2) 噪声, 扣 0.5 预算, 每日总额 2 | 逆变换采样 + `day_state.budget_spent` |
| 相同查询复用原结果, 不再扣费 | `queries` 表主键 `(date, in_key, out_key)`, 命中即返回 |
| 并发不超支、相同查询只生成一份结果 | `BEGIN IMMEDIATE` 写事务串行化 + 事务内复查 + 主键约束 |
| 预算不足不返回新结果 | 事务内检查, 不足即回滚并返回 403 |
| 扣费/保存结果/锁定源数据同生共死 | 三者在**同一 SQLite 事务**中提交或回滚 |
| 响应丢失、重启后重查仍返回原值 | 先提交事务再响应; 结果持久化于 SQLite |

## API

### `POST /upload`
表单字段: `date`(YYYY-MM-DD), `file`(CSV 文件) 或 `csv`(CSV 文本)。
- `200` `{ok, rows}` 替换成功
- `400` CSV 非法(空字段、乘客ID 重复、列数不足等)
- `409` 当天源数据已被首个查询锁定

### `POST /query`
JSON: `{"date": "2026-11-01", "in_stations": ["西直门"], "out_stations": ["国贸", "望京"]}`
- `200` `{ok, noisy_count, cached, budget_remaining?}` — 只返回加噪人数
- `403` 当日预算不足, 不返回新结果
- 相同日期 + 相同站点集合(任意顺序/重复)返回首次发布的同一数值

### 其他
- `GET /api/stations?date=` 当天出现过的全部站点(供页面多选)
- `GET /api/status?date=` 行数、锁定状态、预算消耗
- `GET /api/queries?date=` 当天已发布的全部加噪结果

## 测试

```bash
pip install pytest
python -m pytest tests/ -q
```

覆盖: 上传替换与锁定、规范化复用(乱序/重复)、预算上限(4 次新查询)、
并发不超支(多线程/多进程)、相同查询仅一份结果、重启后结果保持、
Laplace(0, 2) 噪声分布(均值 0、方差 8)。
