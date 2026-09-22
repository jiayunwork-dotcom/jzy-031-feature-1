# 分布式速率限制与流量整形网关平台

浏览器可视化配置限流规则、实时观察放行/拒绝曲线，后端 Go 网关对每一笔进入请求做真正的限流判定。
范围只覆盖**限流规则的配置、判定与观测**：不做真实业务转发，也不计费。

* **五种限流算法**可按规则选用：令牌桶、漏桶、固定窗口、滑动窗口（相邻窗口时间加权）、滑动日志（精确时间戳）。
* **多维 AND 组合**：客户端标识 / 接口路径 / 来源分组任意组合，不同维度值各自独立计数、互不串号。
* **四级配额并存**：全局 / 分组 / 接口 / 客户端逐级校验，任一级超限即拒绝，并在响应里说明是哪一级、哪条规则挡下的。
* **Redis 共享计数**：所有判定逻辑在单个 Lua 脚本内原子完成，多个网关实例共享同一份配额，并发与横向扩容都不超发。
* **规则热生效**：改阈值无需重启，经 Redis Pub/Sub 推送到所有实例；规则持久化在 PostgreSQL，重启后语义不变。
* **规则灰度放量（canary）**：一次改动可先只让一小部分判定主体按新版本判定、其余继续走旧版本；比例随时 10%→50%→100% 调整，到 100% 收口全量，调回 0 或一键中止则全部流量瞬间回到旧版本。分桶确定性、可复现（FNV-1a 哈希到 100 个桶），放量单调只增不减、不重新洗牌；新旧两个版本各自独立占用配额计数，互不挪用；灰度状态同样持久化 PostgreSQL 并经 Redis Pub/Sub 多实例热同步，重启不丢。
* **实时观测台**：每秒放行/拒绝曲线、各级令牌余量或窗口占用、最近被拒请求命中的规则与级别，并可在页面上发压/回放流量；灰度期间新版本与旧版本的放行/拒绝走势、配额占用分别呈现。

---

## 一键启动

```bash
docker compose up -d --build
```

启动后：

| 服务 | 地址 |
| --- | --- |
| 前端控制台 | http://localhost:8081 |
| 后端 API / 网关判定入口 | http://localhost:8080 |
| Redis 7 | localhost:6379 |
| PostgreSQL 16 | localhost:5432 |

横向扩容（多实例共享 Redis 配额）：

```bash
docker compose up -d --scale backend=3
```

---

## 快速体验（启动后照做）

1. 打开 http://localhost:8081，点「新建规则」。
2. **令牌桶突发**：算法选令牌桶，维度勾 `client_id`，客户端级填速率 2/s、桶容量 5；在右侧发压板选「瞬时突发 ×6」，可看到前 5 个被吃掉、随后按 2/s 放行。
3. **漏桶整形**：换漏桶，同样 burst=5；判定序列里每个放行请求带 `release_in_ms`（500ms 等间隔），输出被整形成平滑速率，而令牌桶瞬时全放。
4. **窗口边界对比**：分别建固定窗口与滑动窗口（阈值 50、窗口 1s），在窗口尾与下一窗口初各打一批；固定窗口跨边界放两倍（50+50），滑动窗口不会（50+少量）。
5. **多客户端隔离**：规则维度勾 `client_id + api_path`，A 客户端打满 /orders 不影响 B 客户端，也不影响 A 自己的其他接口。
6. **四级配额**：一条规则同时配置 global/group/api/client，任一级最紧就由那一级挡下，429 响应里写明 `level` 与 `reason`。
7. **灰度放量**：选中一条已存在的规则，编辑出更紧的新版本（例如客户端配额 100→20），在底部填初始放量比例 10 并点「以灰度方式保存这次改动」。右侧出现灰度控制条与新旧两版各自的曲线；在发压板把「分散到 N 个判定主体」设为 100 回放，可看到约 10% 主体走新版、其余走旧版，反复回放分流不变。逐步把比例调到 50、100，已在新桶里的主体不会被洗回去；点「中止放量/回退」则全部流量瞬间回到旧版本。

---

## 灰度放量（canary）是怎么判定的

灰度分桶与版本选择是一个**独立、纯函数、可单测的层**（`internal/canary/`），不掺进判定循环：

* **主体（subject）**：取规则声明的那组维度值，按声明顺序拼成确定性身份（如 `client_id=42|api_path=/x`），整次放量期间由旧版本维度锚定。
* **分桶**：`bucket = FNV-1a(长度前缀(ruleID) + 长度前缀(subject)) mod 100`，落在固定的 `[0,99]`。
* **版本选择**：`bucket < percent → canary`，否则 `stable`。因为新版集合是前缀 `[0,percent)`，比例从小调大时只增不删地把主体搬进新版本，**绝不重新洗牌**；比例为 0 全部走旧版，100 全部走新版。
* **跨实例一致**：归属只依赖 `(ruleID, subject, percent)`，任何实例、任何时刻算出的结果都一样，同一笔请求落到哪个网关都进同一版本。
* **配额隔离**：新版本的计数键多一段 `canary`（`rl:{id}:canary:level:...`），与旧版本 `rl:{id}:level:...` 完全是两套计数；中止后新版计数留在 Redis 但不再被任何判定读取。
* **响应可见**：放行/拒绝响应里每条命中规则都带 `version`（`stable` / `canary`），便于排查「为什么同一接口两个客户体验不一样」。

---

## 五种算法的可测差异

判定脚本在 `internal/engine/`，每个算法一个文件、一段独立 Lua：

| 算法 | 状态 | 行为 |
| --- | --- | --- |
| 令牌桶 `token_bucket.go` | tokens / last | 恒速补令牌，桶满则弃；空闲补满，故突发能吃掉整个桶容量 |
| 漏桶 `leaky_bucket.go` | level / last | 恒速漏出，入桶超容即拒；放行请求按 FIFO 以 `release_in_ms` 等间隔释放，输出平滑 |
| 固定窗口 `fixed_window.go` | w / c | 整秒窗口计数，跨窗清零；窗口切换瞬间可放约两倍 |
| 滑动窗口 `sliding_window.go` | a / ca / cb | 相邻两窗口按时间占比加权，边界处旧窗口线性淡出，无双倍突发 |
| 滑动日志 `sliding_log.go` | ZSET(timestamp) | 精确保留窗口内每次命中，逐条逐出过期记录 |

同一条规则的四个级别用一段 **N-key 原子脚本**（`*_multi.go`）一次判定：先看全部级别，
全部有余量才在同一脚本里一起扣减；第一个超限的级别即拒绝原因。因此：

* 多级别同时逼近上限时，结果确定、可复现，与检查顺序无关；
* 两个并发请求（或两个网关实例）不可能同时读到旧值而各自放行。

---

## 模块划分

```
backend/
  cmd/server/main.go              进程入口：连 Redis/PG、加载+热更新规则、起 HTTP
  internal/
    model/rule.go                 规则领域模型与配置校验（非法规则在此被拒）
    config/                       环境变量配置
    engine/
      engine.go                   算法管理器（EVALSHA + 回退）
      token_bucket.go             令牌桶 Lua
      leaky_bucket.go             漏桶 Lua
      fixed_window.go             固定窗口 Lua
      sliding_window.go           滑动窗口 Lua
      sliding_log.go              滑动日志 Lua
      *_multi.go                  各算法的四级 N-key 原子脚本
      multi.go                    TryMulti 调度
    matcher/matcher.go            多维 AND 匹配 + 独立维度值 Redis key（含版本隔离键）
    canary/canary.go              灰度分桶 + 纯函数版本选择 + Rollout 校验（独立一层，可单测）
    quota/checker.go              灰度版本选择 + 四级配额级联判定 + 统计
    store/rule_store.go           PostgreSQL 持久化 + 内存缓存 + Pub/Sub 热更新
    store/rollout_store.go        灰度状态（比例/新旧两版内容）PG 持久化 + Pub/Sub 热同步
    stats/stats.go                每秒放行/拒绝时序、最近拒绝日志（按 stable/canary 分版本）
    api/
      server.go                   路由、网关判定入口 /api/gateway/check
      rules.go                    规则 CRUD（严格 JSON 校验）
      rollout.go                  灰度启动 / 调比例 / 全量收口 / 中止
      metrics.go                  时序/拒绝/概览（支持 version=stable|canary）
      state.go                    余量与活动 key（新旧两版分别 peek）
      replay.go                   发压/回放（可分散到 N 个主体观察分流）
frontend/
  src/
    App.vue
    components/
      RuleList.vue                规则列表
      RuleEditor.vue              规则编辑组件（算法/维度/各级阈值/初始灰度比例）
      CanaryPanel.vue             灰度比例控制（预设百分比/全量收口/中止回退）
      ObserverPanel.vue           实时观测面板（新旧两版曲线/余量/拒绝表）
      TrafficGenerator.vue        发压与回放（多主体分流，按版本着色）
      LiveChart.vue               SVG 实时曲线
    lib/{api.ts,types.ts}
```

---

## HTTP 接口摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET/POST | `/api/rules` | 列表 / 创建（非法配置返回 400 与原因） |
| GET/PUT/DELETE | `/api/rules/:id` | 查看 / 更新（热生效；有进行中放量时直接改旧规则返回 409）/ 删除 |
| GET | `/api/rollouts` | 列出所有进行中的灰度（比例 + 新旧两版内容） |
| POST | `/api/rules/:id/rollout` | 启动灰度：`{percent:10, canary:{...新规则...}}` |
| PUT | `/api/rules/:id/rollout` | 调整比例 `{percent:50}`，可顺带替换 `canary` 内容 |
| POST | `/api/rules/:id/rollout/promote` | 100% 收口：新版本成为唯一规则，放量结束 |
| DELETE | `/api/rules/:id/rollout` | 中止放量：全部流量瞬间回到旧版本 |
| POST | `/api/gateway/check` | **真正的判定入口**，body：`client_id/api_path/group`，放行 200、拒绝 429（均带 `version`） |
| GET | `/api/metrics/timeseries?rule_id=&seconds=&version=stable\|canary` | 每秒 allow/deny（按版本） |
| GET | `/api/metrics/denies?rule_id=&version=` | 最近被拒请求（不带 version 取两版合计） |
| GET | `/api/rules/:id/state` | 当前余量 / 窗口占用 / 活动 key（灰度时含两版 peek） |
| POST | `/api/tools/replay` | 按批发压，返回每笔判定序列；`distinct_clients=N` 分散到 N 个主体观察分流 |

429 响应示例：

```json
{
  "allowed": false,
  "rule_id": "...",
  "rule_name": "four",
  "level": "global",
  "reason": "rule \"four\" level global quota exceeded (limit=3)"
}
```

---

## 自动化测试

测试用真实 Redis + 嵌入式 PostgreSQL 16（非 mock）跑完整链路。

```bash
cd backend
go test ./... -p 1
```

> 需要本机 Redis（默认 `localhost:6379`，可用 `REDIS_ADDR` 覆盖）。
> 嵌入式 PG 二进制首次运行会自动下载并缓存到 `/tmp/embedded-pg16-cache`。

锁定的行为：

* **令牌桶**突发恰好放到桶容量、之后按恒定速率补充；
* **漏桶**突发只进桶容量，放行按等间隔 `release_in_ms` 平滑释放；
* **固定窗口**边界可放两倍，**滑动窗口**边界不会；
* **滑动日志**精确逐出过期命中（整窗过期与逐条过期）；
* 多维 AND 组合下不同维度值独立计数不串号；
* 四级配额任一级超限都由对应级别挡下并说明原因；
* 多级同阈值并发争抢时结果确定、可复现（拒绝总归因第一级）；
* 并发（含模拟两个网关实例共享一份 Redis）计数绝不超发；
* 规则热更新即刻生效；
* 非法配置（阈值非正、维度值缺失、算法未知、窗口非法等）全部被拒并给出原因。

灰度放量锁定的行为（真实 Redis + 嵌入式 PostgreSQL，非 mock）：

* **主体稳定**：比例不变期间，同一个判定主体反复判定始终落在同一版本；
* **单调放量**：比例从小调大时，原已在新版本的主体全部保留、只新增不剔除，不重新洗牌；
* **可复现 / 跨实例一致**：给定 `(规则, 比例, 主体)` 版本归属唯一确定，多个独立判定路径（模拟多实例）算出的归属完全一致；
* **配额隔离**：新旧两版各自独立计数，新版客户端打满不影响同一主体在旧版的余量（Redis 键物理分离），反之亦然；
* **中止 / 归零即回退**：中止或比例归零后全部流量立即回到旧版本，新版计数不再影响任何判定；
* **比例校验**：越界（<0、>100）与非数值比例一律 400 拒绝并说明原因；灰度中的非法新版本同样按既有规则校验被拒；
* **重启不丢**：放量（哪条规则、当前比例、新旧两版内容）持久化 PostgreSQL，重启或新实例加入后按原比例继续分流，已在新桶的主体依旧在新桶；
* **多实例热同步**：任一实例调整比例，其余实例经 Redis Pub/Sub 即时按新比例分流。

---

## 本地开发（不用 Docker）

```bash
# Redis 与 Postgres 指向本机
export REDIS_ADDR=localhost:6379
export POSTGRES_DSN='postgres://rl:rl@localhost:5432/ratelimit?sslmode=disable'
cd backend && go run ./cmd/server

cd frontend && npm install && npm run dev   # http://localhost:5173，/api 代理到 8080
```
