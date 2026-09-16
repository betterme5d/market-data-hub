# market-data-hub

行情数据与浏览器爬虫服务仓库（自 `funds_pro` 单仓库拆出，git 历史经 filter-repo 迁移，作者/日期/提交信息完整保留）。

## 服务

| 目录 | 服务 | 容器名 | 说明 |
|---|---|---|---|
| `src/market-data` | Python FastAPI（uvicorn，:8080） | `market_data` | 统一行情/净值/K线数据代理，集成 AkShare、YFinance、东财、雪球、沪深交易所等上游 |
| `src/browser-proxy` | Python（FastAPI + Playwright） | `browser-proxy` | 浏览器爬虫主体（容器内 :8098），供网关按需拉起 |
| `src/browser-proxy` | Python（FastAPI） | `browser_proxy_gateway` | 浏览器爬虫与 Cookie 网关（:8081），负责唤醒/空闲关停爬虫容器 |

两个服务通过 HTTP 协作：MarketData 经 `BROWSER_PROXY_URL` 调用爬虫网关。

## 本地运行

```bash
# MarketData
pip install -r src/market-data/requirements.txt
uvicorn main:app --app-dir src/market-data --port 8080

# Playwright 网关
pip install -r src/browser-proxy/requirements.txt
python src/browser-proxy/gateway.py
```

## Docker Compose

编排文件与 funds 仓库同款约定，三个服务：`market-data`、`browser-proxy-gateway`、`browser-proxy`，
**无外部中间件依赖**（缓存是进程内实现，见下）。

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
curl http://127.0.0.1:8080/health
```

- 宿主端口：`market-data` :8080、`browser-proxy-gateway` :8081；`browser-proxy` 不发布端口，只走容器网内 `browser-proxy:8098`。
- 网关挂载 `/var/run/docker.sock`，按需唤醒 `browser-proxy`，空闲 300s（`BROWSER_PROXY_IDLE_LIMIT`）后自动关停；因此该容器 `restart: "no"`，不要改成 `unless-stopped`。
- `market-data` 的缓存分三层（见 `src/market-data/core/state_store.py`）：**进程内存**（10 秒级报价短路缓存、健康指标窗口）、**`data/state/*.json`**（熔断状态、雪球 Cookie、yfinance 复权锚点，跨重启保留）、**`data/cache/*.parquet`**（净值/K线/份额的时序增量缓存）。
- 运行时状态全部落在宿主 `src/market-data/data/`（bind mount），不入库：**换机器/迁服务时必须一起搬**，否则退市水位线、上市日期要重新全量拉取，熔断状态与雪球 Cookie 也要重建。
- 无 Valkey/Redis 依赖：单容器单进程部署下进程内缓存与原实现行为等价，代价是「多 worker / 多副本之间不共享状态」——需要水平扩容时再回补共享存储。
- 需要改代码即时生效（含容器内跑测试）时用开发编排：

```bash
docker compose -f docker-compose-dev.yml up -d --build
docker compose -f docker-compose-dev.yml exec market-data python -m pytest -q
```

- 本服务不再需要 Valkey；funds 编排里的 `funds_valkey` 仍是 C# 侧自己的缓存（单设备登录 Token、集思录/雪球 Token 等），两者互不影响。
- 与 funds 编排同机协作：funds 侧把 `MarketData__BaseUrl` / `AkShare__BaseUrl` 指向 `http://<本机>:8080`、`PlaywrightGateway__BaseUrl` / `Xueqiu__AuthServiceUrl` 指向 `http://<本机>:8081`；或把两套 compose 的 default 网络统一成同一个 external 网络后按容器名直连。

单服务镜像仍可单独构建（build context 即各服务目录）：

```bash
docker build -t marketdata/app src/market-data
docker build -f src/browser-proxy/Dockerfile.gateway -t marketdata/browser-proxy-gateway src/browser-proxy
docker build -f src/browser-proxy/Dockerfile -t marketdata/browser-proxy src/browser-proxy
```

## 配置

复制 `.env.example` 为 `.env`。变量清单与默认值见 `src/market-data/core/config.py`。
容器间地址（`BROWSER_PROXY_URL`）由 compose 注入，`.env` 里的同名项只在宿主机裸跑时生效；
状态目录由 `STATE_DIR` 控制（默认 `data/state`，容器内落在 bind mount 的 `/app/data/state`）。

## 测试

```bash
cd src/market-data && python -m pytest
```

## 架构

详见 `src/market-data/ARCHITECTURE.md`。

## 历史说明

- 本仓库历史由 funds_pro 拆分而来，仅保留触及 `src/marketdata` / `src/browser-proxy` 的提交。
- 原仓库中同时修改其他模块的混合提交，此处只保留这两个目录内的变更。
- 拆分前的完整历史见原仓库 funds_pro 及其镜像备份（2026-09-16，拆分时 538 提交）。