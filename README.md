# 管线吹扫靶板颗粒判级 API

纯 Python 标准库 + SQLite 实现，无第三方运行时依赖。API 围绕“不可变吹扫段版本、证据撤销但审计不消失、并发上传不重复累计”设计。

## 启动

```bash
PARTICLE_API_DB=/tmp/particle.sqlite3 \
PARTICLE_API_HOST=127.0.0.1 \
PARTICLE_API_PORT=8000 \
python3 -m particle_api.server
```

运行测试：

```bash
python3 -m unittest -q tests_particle_api
```

## 核心规则

1. **吹扫段版本不可变**
   - `segment_no + canonical(config)` 的 SHA-256 为 `version_hash`。
   - 阈值、粒径区间、压力采样要求等任何配置变化都会生成新版本。
   - 连续合格次数按 `segment_version_id` 独立保存和重算。

2. **有效暴露区**
   - 有效格数 = 总格数 - `unexposed_cells`。
   - 有效面积 = 有效格数 × 单板登记的 `cell_area_m2`。
   - 每个粒径区间：颗粒数 / 有效面积 = 单位面积颗粒量。

3. **判级**
   - 任一面积密度超限：`FAIL`。
   - 任一网格局域密度超过 `grid_limits_particles_per_m2`：`FAIL`，返回行优先序最小失败网格 `minimum_failing_grid`。
   - 重叠使用、时钟倒退、照片错位、局部污染、暴露时长不足或压力证据不完整：`INVALID`。
   - `FAIL`/`INVALID` 都中断并撤销该不可变版本当前连续合格计数；原事件保留，新增重判事件和审计事件。

4. **并发/重复上传**
   - 判级输入生成稳定 `upload_hash`，`exposures.upload_hash` 唯一。
   - 写事务使用 `BEGIN IMMEDIATE` 和进程内写锁；同一块靶板的同一证据并发上传只插入一次，计数不重复累计。
   - 时间重叠但不是同一证据的，会保留两条暴露记录，并追加 `OVERLAP` 硬标记。

5. **时钟倒退**
   - 压力时间戳早于该版本已知最高水位时记录 `REGRESSION`。
   - 与倒退区间相交的暴露自动追加 `CLOCK_REGRESSION`，重判并清零连续合格。
   - 调用 resync 后，区间内迟到补录的暴露仍保持硬标记；审计链不删除。

6. **下一次有效取样窗口**
   - 响应给出段窗口、指定靶板复用窗口、压力采样要求、所需前置条件（时钟同步/靶板洗消）。
   - 活跃时钟倒退期间不返回可用窗口。

## 端点

### 注册不可变吹扫段版本

`POST /api/segments`

```json
{
  "segment_no": "SP-100",
  "config": {
    "size_bins": [
      {"lo_um": 0, "hi_um": 100},
      {"lo_um": 100, "hi_um": null}
    ],
    "area_limits_particles_per_m2": [1000, 100],
    "grid_limits_particles_per_m2": [null, 100],
    "min_exposure_seconds": 60,
    "photo_tolerance_seconds": 5,
    "target_exposure_seconds": 60,
    "pressure_min_samples": 3,
    "pressure_required_coverage": 0.95,
    "pressure_max_gap_seconds": 25,
    "segment_cooldown_seconds": 0,
    "plate_cooldown_seconds": 10
  }
}
```

`hi_um: null` 表示无上界。网格限值为 `null` 时该区间使用面积限值作为每平方米密度判据。

### 登记靶板

`POST /api/plates`

```json
{
  "plate_code": "T-01",
  "grid_rows": 2,
  "grid_cols": 2,
  "cell_area_m2": 0.01
}
```

### 上传压差采样

`POST /api/pressure-samples`

```json
{
  "version_hash": "…",
  "samples": [
    {"sampled_at": "2026-09-15T10:00:00Z", "pressure_pa": 120},
    {"sampled_at": "2026-09-15T10:00:20Z", "pressure_pa": 121}
  ]
}
```

重复时间戳被忽略；倒退时间戳会创建时钟事件并触发相关暴露重判。

### 靶板计数判级

`POST /api/exposures/grade`

```json
{
  "version_hash": "…",
  "plate_code": "T-01",
  "mount_position": "A",
  "installed_at": "2026-09-15T10:00:00Z",
  "removed_at": "2026-09-15T10:01:00Z",
  "photo_at": "2026-09-15T10:01:02Z",
  "unexposed_cells": [3],
  "contaminated_cells": [],
  "cell_counts": [
    {"cell_index": 0, "by_bin": [0, 2]},
    {"cell_index": 1, "by_bin": [0, 1]}
  ],
  "actor": "inspector"
}
```

响应包括：

- `judgement_basis`：判级公式和规则；
- `effective_exposure`：有效格与有效面积；
- `particle_bins`：各粒径区间总量、单位面积量、限值；
- `minimum_failing_grid`：行优先序最小失败网格（行列号、计数、超限区间）；
- `pressure`：覆盖率、最大采样间隔、失败原因；
- `consecutive_passes` 与 `streak_bound_to_version_hash`；
- `next_valid_sampling_window`：下一次有效安装/取样窗口。

### 事后标记局部污染或其他硬失效

`POST /api/exposures/{id}/flags`

```json
{
  "kind": "LOCAL_CONTAMINATION",
  "cell_index": 2,
  "note": "oil spot",
  "actor": "inspector"
}
```

该操作只追加标记和新判级事件，不删除旧计数、旧判级或审计。

### 洗消靶板

`POST /api/plates/decontaminate`

```json
{
  "plate_code": "T-01",
  "decontaminated_at": "2026-09-15T11:00:00Z",
  "actor": "team-a",
  "note": "verified cleaning"
}
```

### 时钟倒退与恢复

- `POST /api/clock/regressions`
- `POST /api/clock/resync`

### 查询

- `GET /api/exposures/{id}`：暴露与最新判级；
- `GET /api/streak?version_hash=…`：绑定到不可变版本的连续合格次数；
- `GET /api/next-window?version_hash=…&plate_code=T-01&at=2026-09-15T11:00:00Z`；
- `GET /api/audit?limit=100`；
- `GET /api/audit/verify`：重算哈希链。

## SQLite 审计与不可变性

- 业务表（除当前 streak 指针表）均有 `BEFORE UPDATE/DELETE` 触发器。
- `audit_log` 包含 `seq`、`prev_hash`、规范化 JSON payload、`entry_hash`。
- 当前连续合格次数由全部不可变判级事件按安装/拆卸顺序重算，因此撤销某个早期合格证据后，后续连续状态可确定性恢复。
- 表使用外键、唯一约束和 WAL；进程内写锁序列化写请求。
