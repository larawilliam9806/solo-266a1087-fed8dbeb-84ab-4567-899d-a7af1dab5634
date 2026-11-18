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
| 每行保留原始站名, 另存归一后站名 | `passengers.in_station/out_station` + `*_norm` + `mapping_version` |
| 发布映射即整份替换并从原名重算未锁定日期 | `POST /mapping/publish`, 单事务内完成 |
| 已锁定日期的源行/站名/已发布结果不变 | 重算只覆盖未锁定日期, 页面列出被跳过的日期 |
| 映射与上传、查询交错时不混用版本 | `BEGIN IMMEDIATE` + `day_state.mapping_version`/`mapping_version` 快照 |

## 站名归一(别名映射)

地铁站更名后, 同一站在首程 CSV 里会出现不同名称。若按站查询时不归一,
合作方按标准站名查询会漏掉仍用旧名的行, 同一站被拆成多个结果。

- 每行**始终保留原始站名**(`in_station` / `out_station`), 另存归一后的站名
  (`in_station_norm` / `out_station_norm`) 及所依据的映射版本。
- 在 `/mapping`(站名归一页, `POST /mapping/publish`)上传**全量**别名映射
  CSV(两列: `别名, 标准站名`)。发布即**整份替换**映射, 并**从原始站名**重算
  所有**未锁定**日期的归一列。以后上传的源数据同样先存原名、再按当前映射归一;
  再次发布映射仍从原名重算, 不会叠加旧映射。
- 映射校验在发布前完成: 别名必须唯一, **标准站名不能再作为别名**(即标准站名
  与别名两个集合不相交), 空字段/列数不足均拒绝。校验失败返回 400, 原映射与
  数据保持不变。
- **已锁定日期**的源行、可选站名(`/api/stations`)与已发布加噪结果一律保持原状;
  发布响应与页面会列出被跳过的锁定日期(以及其中若重算本会变化的日期), 和
  归一结果实际发生变化的未锁定日期。
- 查询时, 合作方提交的站名会先按该日期对应的映射归一(别名与标准站名等价),
  因此同一站的别名/标准名不会产生不同结果。
- 映射发布与重算在**同一 `BEGIN IMMEDIATE` 事务**中提交或回滚: 交错的上传与
  查询只能看到完整的旧映射或完整的新映射。每个日期的映射版本记录在
  `day_state.mapping_version`, 映射快照存入 `mapping_version` 表。
- 进程启动时(`init_db` 之后)做一次自检: 未锁定日期若与当前映射版本不一致,
  用当前映射整体重算; 已锁定日期、既有预算不受影响。

发布映射示例:

```bash
curl -F 'file=@mapping.csv' http://localhost:8000/mapping/publish
# {"ok":true,"version":3,"alias_count":12,
#  "changed_dates":["2026-11-01"],"affected_dates":["2026-11-01","2026-11-02"],
#  "locked_dates":["2026-10-30"],"message":"..."}
```

## API

### `POST /upload`
表单字段: `date`(YYYY-MM-DD), `file`(CSV 文件) 或 `csv`(CSV 文本)。
- `200` `{ok, rows, mapping_version}` 替换成功并记录所用映射版本
- `400` CSV 非法(空字段、乘客ID 重复、列数不足等)
- `409` 当天源数据已被首个查询锁定

### `POST /mapping/publish`
表单字段: `file`(映射 CSV) 或 `csv`(文本), 两列 `别名,标准站名`。
- `200` `{ok, version, alias_count, changed_dates, affected_dates, locked_dates, skipped_changed_dates, message}`
- `400` 映射无效(别名重复、标准站名又作别名、空字段等), 原映射不变
- `500` 发布失败已回滚, 原映射与数据不变

### `POST /query`
JSON: `{"date": "2026-11-01", "in_stations": ["西直门"], "out_stations": ["国贸", "望京"]}`
- `200` `{ok, noisy_count, cached, budget_remaining?}` — 只返回加噪人数
- `403` 当日预算不足, 不返回新结果
- 相同日期 + 相同站点集合(任意顺序/重复)返回首次发布的同一数值

### 其他
- `GET /api/stations?date=` 当天出现过的全部站点(已归一, 供页面多选)
- `GET /api/status?date=` 行数、锁定状态、映射版本、预算消耗
- `GET /api/queries?date=` 当天已发布的全部加噪结果
- `GET /api/mapping` 当前生效映射版本、更新时间与全部别名对
- `GET /mapping` 站名归一页(上传并发布映射, 显示改动与跳过的日期)

## 测试

```bash
pip install pytest
python -m pytest tests/ -q
```

覆盖: 上传替换与锁定、规范化复用(乱序/重复)、预算上限(4 次新查询)、
并发不超支(多线程/多进程)、相同查询仅一份结果、重启后结果保持、
Laplace(0, 2) 噪声分布(均值 0、方差 8), 以及站名归一: 原名保留与映射归一、
发布重算未锁定日期、替换映射后从原名重算、锁定日期原样、映射无效/发布失败回滚、
启动自检与发布交错时不混用映射版本。
