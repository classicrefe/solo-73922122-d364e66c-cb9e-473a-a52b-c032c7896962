# Traffic PITR Recovery API（交通数仓误删恢复）

路口分钟计数被值班员误删后，需要从**物理备份 + WAL 归档**把删除前的记录找回来补数；
恢复期间主库采集不中断。本服务在启动时对主库做一次 `pg_basebackup` 物理基准备份，
主库持续归档 WAL；用户在任意时刻创建**命名恢复点**，只有当该点所需的 WAL 全部归档
后才允许选择它恢复；每次恢复都从基准备份**重建一个全新的隔离只读副本**，重放 WAL 到
目标恢复点即停，供查询补数。

## 架构

```
┌──────────────┐   WAL 归档(archive_command)   ┌───────────────┐
│  db (主库)    │ ───────────────────────────▶ │  wal_archive  │
│  traffic 库   │                              │  (共享卷)      │
│  单表:        │ ◀──────── pg_basebackup ───┐ │               │
│  traffic_     │                            │ │               │
│  minute_      │        ┌───────────────────┴─┴─────────────┐ │
│  counts       │        │  api (FastAPI)                    │ │
└──────────────┘        │  · 启动时物理基准备份 (/backups)    │ │
       ▲                │  · 恢复点就绪检查 (WAL 连续性)       │ │
       │ 主库采集不中断   │  · 恢复执行器: 每次从基准备份重建    │ │
       └────────────────│    隔离副本, 重放 WAL 到恢复点暂停   │ │
                        │    (/recovery_work/rN, 只读热备)     │ │
                        └─────────────────────────────────────┘ │
```

- **db**：`postgres:16`，开启 `archive_mode=on`、`archive_timeout=10s`，WAL 归档到共享卷。
  源库只有一张业务表 `traffic_minute_counts(intersection_id, minute_utc, vehicle_count)`。
- **api**：`postgres:16` 镜像 + FastAPI。启动时执行 `pg_basebackup -X stream` 建立物理
  基准备份；恢复时把基准备份复制到全新目录，配置 `restore_command` +
  `recovery_target_name` + `recovery_target_action=pause`，以热备只读方式启动在
  独立端口（容器内 127.0.0.1:55433）。
- API 自身的台账（恢复点、恢复任务）存在独立 SQLite（api_state 卷），**不污染源库**，
  源库始终只有一张业务表。

## 快速开始

```bash
docker compose up --build
# 等待 api 就绪后:
# 1) 打开演示入口 http://localhost:8000/docs （Swagger UI，根路径 / 会重定向到这里）
# 2) 或跑一键演示脚本（需要 curl 和 jq）:
./scripts/demo.sh
```

演示脚本完整走一遍：模拟采集写入 → 创建恢复点 `before_delete` → 等待 WAL 归档齐全 →
模拟误删最近 10 分钟 → 主库继续写入新路口数据 → 发起 PITR 恢复 → 从只读副本查回被删
记录 → 断言副本不含恢复点之后的提交、主库数据未受影响 → **切换到第二个恢复点** →
**注入“旧副本停止失败”故障验证新恢复被 409 拒绝、旧副本继续按原恢复点供数且目录保留** →
**清除故障后不重启直接重试成功**。也可在 `/docs` 页面手动调用 `/demo/faults` 与
`/recoveries` 完成同样演示。

## API 一览（详见 /docs）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 服务与主库连通性 |
| GET | `/backup` | 启动基准备份状态（running/ready/failed + 错误） |
| POST | `/demo/generate` | 模拟采集：生成 路口×分钟 计数（可指定前缀/数量） |
| POST | `/demo/delete` | 模拟误删：删除最近 N 分钟数据 |
| GET | `/primary/traffic` | 查主库当前数据（证明主库一直在采集） |
| GET | `/demo/overview` | 主库数据、备份、恢复点、恢复任务总览 |
| GET/PUT | `/demo/faults` | 查看/设置演示故障开关（模拟旧副本停止失败、副本启动失败） |
| POST | `/restore-points?name=...` | 创建命名恢复点（`pg_create_restore_point` + 强制切 WAL） |
| GET | `/restore-points` | 恢复点列表：`pending`（WAL 未齐）/ `ready`（可选）/ `expired`（早于基准备份） |
| POST | `/recoveries` | 发起恢复 `{"restore_point": "name"}`，202 异步任务；同一时刻只允许一项在途 |
| GET | `/recoveries` / `/recoveries/{id}` | 恢复任务状态：running/success/failed/superseded + 错误详情 |
| GET | `/recoveries/current` | 当前对外服务的恢复（无则 404） |
| GET | `/replica/traffic` | 查询已恢复只读副本（未就绪/已失败返回 409） |

## 恢复语义（对照需求）

- **启动即备份**：api 启动时后台执行 `pg_basebackup -X stream`（失败自动重试 10 次，
  状态见 `/backup`）；主库 WAL 由 `archive_command` 持续归档。
- **恢复点门控**：创建恢复点时记录其 LSN 并 `pg_switch_wal()`；后台任务持续校验
  「基准备份结束 LSN → 恢复点 LSN」之间每一个 WAL 段都已在归档目录中，齐全才置
  `ready`；未就绪的点发起恢复返回 409。早于基准备份重做起点的点标记 `expired`。
- **物理恢复**：每次恢复都 `cp` 基准备份到全新目录 `recovery_work/rN`，写
  `recovery.signal` 与 PITR 配置后启动独立 postgres——不是逻辑留史/影子表。
- **到达目标才报成功**：轮询副本直到 `pg_last_wal_replay_lsn() >= 目标 LSN`，
  并对业务表执行一次真实查询确认可查询，才置 `success` 并开放查询。
- **失败即报、不留残缺**：缺 WAL（`restore_command` 失败 → postgres FATAL）、
  恢复进程退出（pg_ctl 探活 + 后台看门狗每 3s 巡检）都会把任务置 `failed` 并记录
  postgres 日志尾部；失败任务的目录被清理，**不开放残缺副本，也不回退到上一次副本**。
  重试 = 再 POST 一次，不需要重启 api。
- **严格退役（修复重点）**：切换恢复点时，必须先确认旧副本 postmaster **确已退出**，
  才能删除其目录、才能受理新恢复：
  - 探活以 `postmaster.pid` + `/proc/<pid>/cmdline` 匹配数据目录为准（防 pid 复用误判），
    `pg_ctl status` 仅在 pid 文件存在但无法解析时作第二意见；
  - 停止依次尝试 `pg_ctl -m fast`、`pg_ctl -m immediate` 并在每步后复查进程，
    最后兜底直接发信号；**只有复查确认进程没了才 `rmtree` 目录**；
  - 停止失败且进程仍活：抛 `ReplicaStillRunning`，保留旧目录与固定端口占用，
    `POST /recoveries` 返回 **409 不受理新任务**；旧副本健康则继续只按**原恢复点**供数，
    若已不健康则看门狗/查询路径明确置 failed 并停供（任务行仍可在 `/recoveries` 查询）；
  - 新恢复启动前还会探测固定端口（默认 55433），端口仍被占则拒绝启动，双重保险；
  - 故障排除（杀掉僵死 postmaster 等）后直接重试同一请求即可，**无需重启服务**。
- **重启不清活副本**：api 重启时不再无条件删除目录——
  - 活的健康副本（已重放到目标）：**重新接管**，继续按原恢复点供数，目录不删；
  - 活的在途恢复（还没到目标）：后台 watcher 接续等待到目标，不重复起副本抢端口；
  - 死副本/无主目录：任务置 failed、清服务指针、删残留目录，之后仍可正常切换恢复点。
- **单一在途**：存在 running 任务时再发起返回 409（物理恢复单项在途保持不变）。
- **不混数据**：每次恢复都是基准备份的全新拷贝，先后两个恢复点的数据互不干扰；
  旧副本只有在被严格退役成功后才标记 superseded。
- **主库不阻塞**：恢复只读归档卷与基准备份，对主库零写入、零锁，主库持续采集。

## 环境变量（api 服务）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE` | db/5432/postgres/postgres/traffic | 主库连接 |
| `REPLICA_PORT` | 55433 | 副本监听端口（容器内 127.0.0.1） |
| `RECOVERY_TIMEOUT_SEC` | 180 | 等待重放到目标点的超时 |
| `REPLICA_RUN_AS` | postgres | 副本进程运行用户（容器内 root 经 gosu 降权；本地裸跑可置空） |
| `BACKUP_ON_STARTUP` | 1 | 启动时是否做基准备份（预置备份的测试场景可置 0） |
| `WAL_ARCHIVE_DIR/BACKUP_DIR/RECOVERY_ROOT/STATE_DB` | /wal_archive /backups/base /recovery_work /state/api.db | 存储路径 |
| `PG_CTL_STOP_TIMEOUT` | 60 | 每档停止（fast/immediate）等待秒数 |
| `SIGNAL_WAIT_SEC` | 10 | 兜底直连发信号后等待退出秒数 |
| `STOP_RECHECK_SLEEP` | 1 | 两次停止尝试之间的复查间隔秒数 |

## 已验证 / 未验证（诚实清单）

本沙箱**内核级禁止一切跨进程信号**（实测 `os.kill(pid, SIGTERM)` → EPERM，
而 `kill(pid, 0)` 探活可用、`/proc` 可读），因此无法运行真实 docker / postgres；
但这恰好能确定性复现“停止失败、进程仍活”的故障现场。测试用一个写
`postmaster.pid`、占用固定 TCP 端口的 Python 假 postmaster 代替 postgres，用协作式
TCP 关闭通道模拟 `pg_ctl stop` 成功（SIGTERM 仍作为真实路径的兜底保留），用
`FORCE_SHUTDOWN` 通道模拟人工 kill -9。运行：`cd tests && PYTHONPATH=../api python3 -m pytest -q`。

已验证（32 项 pytest 全部通过，真实子进程 + 真实文件系统）：

- ✅ 正常退役：活 postmaster 经停止并**复查确认退出**后目录才删除；死副本的残留目录直接清理
- ✅ **核心缺陷回归**：停止失败且进程仍活 → `ReplicaStillRunning`，目录（含数据文件）保留、
  固定端口仍被占用；`POST /recoveries` 返回 409、不建新任务、旧副本仍按原恢复点供数
- ✅ 混合场景：死残留目录被清理的同时存在卡住的活副本 → 409 且服务指针/success 台账零改动
- ✅ 故障开关 `PUT /demo/faults` 可确定性复现上述拒绝路径（HTTP 层 409 断言）
- ✅ **不重启重试**：人工清掉僵死进程后再次 POST 即受理，旧任务 superseded、旧目录删除、
  新任务成功；任务历史始终可在 `/recoveries` 查询
- ✅ 副本启动失败（故障注入）→ 任务 failed、目录清理、无副本供数（`/replica/traffic` 409）、
  一次性开关自动复位
- ✅ 健康副本意外退出 → 看门狗 3s 内置 failed、明确停供；查询路径同样触发停供
- ✅ **服务重启清理**：活的健康副本被重新接管（目录不删、继续按原恢复点供数）；
  活的在途恢复被 watcher 接续到 success；死副本置 failed 并清目录，之后可正常切换；
  无台账的活孤儿目录同样不删
- ✅ pid 复用防护：pid 文件指向存活但非本数据目录 postmaster 的进程时判定为未运行
- ✅ 单一在途 409、恢复点不存在 404、未就绪 409、无副本时查询 409
- ✅ `/health`、`/docs`、`/openapi.json`、`/` 重定向、故障开关读写
- ✅ LSN/WAL 段名计算单测、全部 Python 与 shell 语法检查、compose YAML 结构校验

未能在本环境验证（沙箱无 docker 且禁止跨进程信号所致，相关逻辑已按 docker 场景走查）：

- ⚠️ `docker compose up --build` 的真实构建与端到端联调（Dockerfile/compose 沿用原已评审
  结构，仅新增无额外系统依赖的两个 HTTP 端点与三个可调停止参数，默认值与原行为一致）
- ⚠️ 真实 postgres 上 `pg_ctl fast/immediate` 与直连信号兜底的停机效果（测试以等价的
  协作关闭 + 真实 SIGTERM 尝试覆盖；docker 内同 uid root + gosu 场景信号链路正常）
- ⚠️ `pg_basebackup` 成功路径与热备副本接受连接后的完整 PITR 闭环（沿用原有实现，
  本次未改动备份与 WAL 重放路径）

## 限制

- 单 api 进程、单副本端口：同一时刻一个恢复、一个副本（需求即如此）。
- 基准备份为启动时一份；api 重启会重取备份，此前创建的恢复点会标记 `expired`
  （但重启前已存活的健康副本会被重新接管、继续按原恢复点供数）。
- 恢复点名称唯一（重复创建返回 409）；WAL 归档卷只增不减，演示规模无清理策略。
- `/demo/faults` 为演示用故障注入开关，生产部署应通过反向代理限制访问或移除该路由。
- 无认证鉴权，仅限内网演示。
