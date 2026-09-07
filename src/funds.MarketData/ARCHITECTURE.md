# funds.MarketData 架构规范

本文件是 `src/funds.MarketData` 的结构约定，新增/修改 provider 与 router 时必须遵循。

## 1. 分层

```
main.py              应用装配：日志、FastAPI 创建、路由挂载、探针注册、lifespan
core/                横切能力（无业务）：config / cache / health / dispatcher /
                     routing / filters / exceptions / models / calendar /
                     timeseries_cache（通用时序 Parquet 增量缓存引擎）
providers/           数据源实现（业务所在），按域分目录：
                     exchanges/ 交易所 · funds/ 基金 · quotes/ 行情 · misc/ 杂项
routers/             HTTP 薄层：只做参数解析与异常包装，业务一律下沉到 provider
schemas/             （暂空，归属待定）
scripts/ tests/       工具与测试
```

约束：routers 不写业务逻辑；providers 不散落 `os.getenv`（统一走 `core.config`）。

## 2. 三词命名契约（横跨整个 providers/）

| 后缀     | 职责                                   | 业务取数 | 健康注册 | 示例                                    |
| -------- | -------------------------------------- | -------- | -------- | --------------------------------------- |
| Provider | 业务门面（对外消费入口，一域/交易所一个） | 有（组合） | 视需要 | `FundNavProvider` `SseProvider`         |
| Source   | 原子取数源（拉真实数据）                 | 有       | 无       | `EastmoneySource` `SseFundListSource`   |
| Probe    | 健康探针（只判断通不通/结构对不对）       | 无       | 是       | `EastmoneyProbe` `CmtidpProbe`          |

核心原则：

- **`probe` 语义专属于健康检查**。取数类不得再「身兼 `probe()`」。
- 原子取数源（`Source`）与健康探针（`Probe`）**拆成两个类**，靠同一个 `source`
  字符串关联（如 `sse-fund-list`）。
- 探针必须做**语义校验**（校验返回结构本身，而非仅 HTTP 200）——「站点健康 ≠ 接口健康」，
  上游改签名/加验证/改返回结构都要能被探针识别。
- **探针必须复用 Source 的取数/解析代码路径**（调用 Source 方法，必要时给方法加
  `page_size` 等轻量参数），禁止复制 URL/解析逻辑另写一份——否则探测请求与业务请求
  会各自漂移，探针绿而业务断；且针对 Source 的测试天然覆盖探针路径。

## 3. 健康检查注册

- 探针继承 `providers.base.SourceProbe`，实现 `name`/`category`/`probe()`。
- 在 `main.py` 的 `_register_probes()` 实例化并 `register_probe(...)` 后，即进入
  `/health/sources` 面板、被 `probe_loop` 轮询、支持手动 `/health/sources/{name}/probe`。

**健康检查的单位是「上游接口」，不是「我方方法」，也不是笼统的「源」。**

外部 API 的参数列表、验证方式、返回结构随时可能单独变化或暂停访问，
所以 **1 个上游接口（一个 URL 契约）→ 1 个 Probe**（语义校验该接口的返回结构）：

- 我方 N 个方法命中同一上游接口 → 共享那 1 个 Probe；
- 某方法引入新的上游接口 → 该接口必须有对应的新 Probe；
- 一个源（如 eastmoney）下有多个上游接口（`lsjz` 与 `Fund_JJJZ_Data`）→ 各配一个
  Probe（`EastmoneyProbe` / `EastmoneyJjjzProbe`），缺一即该业务路径可能静默失效。

新增取数方法时的判断顺序：先看它命中的上游接口是否已有 Probe——有则复用，
没有则补一个，而不是机械地「一个方法一个 Probe」。

## 4. 待决 / 已知漂移（不要在本轮顺带改）

- 路由前缀统一：`/api` 与 `/api/v1`、裸路径并存，新接口统一走 `/api/v1`，旧接口代理兼容。
- quotes/ 四源走 `core.dispatcher` 熔断，与 `core.health` 双体系并行，待收敛。
- `schemas/` 空目录与 `core/models.py` 归属待定。
- `base_nav.py` 抽象已规范为 `FundNavSource`，业务门面收敛至 `FundNavProvider`（位于 `fund_nav.py`）。

## 5. 禁止事项

- providers / routers 内散落 `os.getenv`、散落硬编码上游地址（须走 `core.config`）。
- 取数类内嵌 `probe()`（取数与探测必须分离）。
- router 内写业务逻辑。

## 6. 易错点清单（迁移/新增数据源时逐条自查）

以下每条都对应真实踩过的坑或对比 C# 实现发现的偏差，新增/修改数据源时必须逐条核对：

1. **「最新一期」不得硬编码今天**。净值类数据按交易日发布：须用「最近交易日」，
   且交易日 15:00 前应取前一交易日（对齐 C# FundNetValueCollectionJob 的 T/T-1 语义，
   收盘前当日净值未发布）；查询为空时应有回退，而不是把空集当正常结果返回。
2. **空值行过滤要对齐 C# 契约**。C# 侧按 `NetValue is not null` 过滤无净值行；
   Python 侧漏过滤会返回脏行（同一基金出现「null 行 + 有效行」两条），夸大 count。
3. **全量列表必须做完整性校验**。当取数结果被下游用于关键决策（如 C# 用沪深基金
   列表做「不在列表 → 标记退市」），上游偶发截断（SZSE ShowReport 实测丢 100+ 行）
   会造成误伤——须做行数下限校验或失败重试，绝不把截断数据当全量返回。
4. **上游请求契约以 C# 已验证实现为基准**。aoData 列定义、mDataProp、iColumns 等
   参数不得凭感觉改写（上游目前忽略列定义返回全量对象，但一旦上游开始依赖这些
   参数过滤列，自行改写就会静默踩坑）。迁移 = 忠实对齐，不是重写。
5. **分页上限以上游实测为准并注释记录**。各接口真实分页上限不同且常与文档不符
   （lsjz=20、Fund_JJJZ_Data=20000、CMTIDP=5000），凭文档假设会翻页失败。
6. **外部输入先 strip 再匹配**。query 参数带尾随空格（`"cmtidp "`）曾直接 404 类报错；
   source、code 等入参须 `strip()` + 规范大小写后再查表。
7. **探针禁止复制实现**。Probe 必须复用 Source 的取数/解析代码路径（见核心原则），
   否则探测请求与业务请求各自漂移、测试覆盖不了。
8. **上游偶发不稳定的兜底策略要显式**。对已知不稳定的接口（xlsx 截断、限频），
   在 Source 层显式处理（重试/校验），不指望调用方自己发现。