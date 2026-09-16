# marketdata-hub

行情数据与浏览器爬虫服务仓库（自 `funds_pro` 单仓库拆出，git 历史经 filter-repo 迁移，作者/日期/提交信息完整保留）。

## 服务

| 目录 | 服务 | 说明 |
|---|---|---|
| `src/marketdata` | Python FastAPI（uvicorn，:8080） | 统一行情/净值/K线数据代理，集成 AkShare、YFinance、东财、雪球、沪深交易所等上游 |
| `src/browser-proxy` | Python（FastAPI + Playwright） | 浏览器爬虫与 Cookie 网关（`:8081`），供 MarketData / 后端获取 Cookie |

两个服务通过 HTTP 协作：MarketData 经 `PLAYWRIGHT_GATEWAY_URL` 调用 Playwright 网关。

## 本地运行

```bash
# MarketData
pip install -r src/marketdata/requirements.txt
uvicorn main:app --app-dir src/marketdata --port 8080

# Playwright 网关
pip install -r src/browser-proxy/requirements.txt
python src/browser-proxy/gateway.py
```

## Docker

build context 即各服务目录：

```bash
docker build -t marketdata/app src/marketdata
docker build -f src/browser-proxy/Dockerfile.gateway -t marketdata/browser-proxy-gateway src/browser-proxy
docker build -f src/browser-proxy/Dockerfile -t marketdata/browser-proxy src/browser-proxy
```

## 配置

复制 `.env.example` 为 `.env`。变量清单与默认值见 `src/marketdata/core/config.py`。
运行时缓存与状态落在 `src/marketdata/data/`（不入库，部署时需持久化或首启重建）。

## 测试

```bash
cd src/marketdata && python -m pytest
```

## 架构

详见 `src/marketdata/ARCHITECTURE.md`。

## 历史说明

- 本仓库历史由 funds_pro 拆分而来，仅保留触及 `src/marketdata` / `src/browser-proxy` 的提交。
- 原仓库中同时修改其他模块的混合提交，此处只保留这两个目录内的变更。
- 拆分前的完整历史见原仓库 funds_pro 及其镜像备份（2026-09-16，拆分时 538 提交）。
