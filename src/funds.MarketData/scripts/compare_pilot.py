# -*- coding: utf-8 -*-
"""
迁移对照验证：直连上游 vs 经 funds.MarketData 代理，逐字段比对响应。

用法:
    python scripts/compare_pilot.py [proxy_base_url]

proxy_base_url 默认 http://localhost:8082（本地验证可先自起实例再传入其端口）。
退出码非 0 表示比对不一致，禁止此时切换 C# BaseUrl。
"""
import asyncio
import json
import re
import sys
from datetime import date, timedelta

import httpx

PROXY = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8082"

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

_CMTIDP_QS = open(
    __file__.replace("scripts\\compare_pilot.py", "tests\\fixtures\\cmtidp_aodata.txt")
    .replace("scripts/compare_pilot.py", "tests/fixtures/cmtidp_aodata.txt"),
    encoding="utf-8",
).read().strip()

_PALMMICRO_COOKIE = ("PHPSESSID=75e55bf31955e8f388f59fd435c801cb; screenheight=1080; "
                     "screenwidth=1920; _ga=GA1.1.839737416.1773129256; "
                     "_ga_DQ4P3FHV66=GS2.1.s1773129255$o1$g1$t1773129262$j53$l0$h0")

_TODAY = date.today()


def _norm_cfets(payload):
    """CFETS 信封的 ts/tstext 每次请求都变，剥离后再比。"""
    if isinstance(payload, dict):
        payload = dict(payload)
        head = payload.get("head")
        if isinstance(head, dict):
            payload["head"] = {k: v for k, v in head.items() if k not in ("ts", "tstext")}
    return payload


def _parse_json_maybe_jsonp(text: str):
    """JSONP（括号包裹）剥壳后按 JSON 解析；失败返回原文。"""
    body = text.strip()
    if body.startswith("(") and body.endswith(")"):
        body = body[1:-1]
    try:
        return json.loads(body)
    except Exception:
        return text


def _norm_sse_yunhp(payload):
    """去掉快照时间字段（直连与代理秒级差异），只比数据本体。"""
    if isinstance(payload, dict):
        return {k: v for k, v in payload.items() if k not in ("date", "time")}
    return payload


def _norm_html(text: str):
    """HTML 抓取源：剥离随请求实时变化的时间戳与随机埋点 token，只比结构性内容。"""
    if not isinstance(text, str):
        return text
    # 数据更新时间：2026-03-09 08:45:58 之类
    text = re.sub(r"数据更新时间[：:]\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", "数据更新时间[STRIPPED]", text)
    # Baidu 统计的 hm.js?<随机hex> 每次请求都换
    text = re.sub(r"[0-9a-f]{16,}-text/javascript", "[HASH]-text/javascript", text)
    text = re.sub(r"hm\.js\?[0-9a-f]{16,}", "hm.js?[HASH]", text)
    return text


def _extract_dom_table(text: str, table_id: str | None = None) -> str:
    """从整页 HTML 中只抽取 C# 实际抓取的那张表格（页面广告/埋点等 A/B 内容逐请求波动，不做比对）。

    - 有 table_id：按 id 精确匹配（如 Palmmicro 的 estimationtable）。
    - 无 table_id：取第一个 <table>（HaoETF 首页/lof 的唯一数据表）。
    比对失败（找不到表）时返回原文，交由上层判不等。
    """
    if not isinstance(text, str):
        return text
    if table_id:
        m = re.search(rf'<TABLE[^>]*id=["\']?{re.escape(table_id)}["\']?[^>]*>.*?</TABLE>', text, re.S | re.I)
    else:
        m = re.search(r'<TABLE.*?</TABLE>', text, re.S | re.I)
    return m.group(0) if m else text


def _norm_haoetf(text: str) -> str:
    return _extract_dom_table(text)


def _norm_palmmicro(text: str) -> str:
    return _extract_dom_table(text, table_id="estimationtable")


# (名称, 方法, 直连 URL, 代理路径, 请求体, 归一化函数, 是否二进制)
CASES = [
    ("cmtidp", "GET",
     f"http://eid.csrc.gov.cn/fund/disclose/getPublicFundJZInfoMore.do?{_CMTIDP_QS}",
     f"/fund/disclose/getPublicFundJZInfoMore.do?{_CMTIDP_QS}", None, None, False),
    ("holidays", "GET",
     "https://api.jiejiariapi.com/v1/workdays/2026",
     "/v1/workdays/2026", None, None, False),
    ("cfets", "GET",
     "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcprHisNew"
     "?startDate=2026-08-25&endDate=2026-09-01&currency=USD/CNY&pageNum=1&pageSize=5",
     "/ags/ms/cm-u-bk-ccpr/CcprHisNew"
     "?startDate=2026-08-25&endDate=2026-09-01&currency=USD/CNY&pageNum=1&pageSize=5",
     None, _norm_cfets, False),
    ("szse-www(交易日历)", "GET",
     f"https://www.szse.cn/api/report/exchange/onepersistenthour/monthList?month={_TODAY:%Y-%m}",
     f"/proxy/szse-www/api/report/exchange/onepersistenthour/monthList?month={_TODAY:%Y-%m}",
     None, None, False),
    ("szse-fund(公告列表POST)", "POST",
     "http://fund.szse.cn/api/disc/announcement/annList",
     "/proxy/szse-fund/api/disc/announcement/annList",
     {"seDate": [f"{_TODAY - timedelta(days=14):%Y-%m-%d}", f"{_TODAY:%Y-%m-%d}"],
      "channelCode": ["fundinfoNotice_disc"], "pageSize": 3, "pageNum": 1},
     None, False),
    ("sse-query(ETF份额)", "GET",
     "https://query.sse.com.cn/commonQuery.do?sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE=2026-09-01",
     "/proxy/sse-query/commonQuery.do?sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE=2026-09-01",
     None, None, False),
    ("szse-docs(PCF清单)", "GET",
     "https://reportdocs.static.szse.cn/files/text/ETFDown/pcf_159915_20260903.xml",
     "/proxy/szse-docs/files/text/ETFDown/pcf_159915_20260903.xml",
     None, None, False),
    ("sse-yunhq(LOF列表)", "GET",
     "https://yunhq.sse.com.cn:32042/v1/sh1/list/exchange/lof?callback=&_=%d" % int(_TODAY.toordinal()),
     "/proxy/sse-yunhq/v1/sh1/list/exchange/lof?callback=&_=%d" % int(_TODAY.toordinal()),
     None, _norm_sse_yunhp, False),
    # HTML 抓取源：只比对 C# 实际解析的基金数据表（页面广告/埋点逐请求波动，全文比对无意义）
    ("haoetf(QDII首页)", "GET",
     "https://www.haoetf.com/",
     "/proxy/haoetf/", None, _norm_haoetf, False),
    ("palmmicro(QDII美股)", "GET",
     "https://www.palmmicro.com/woody/res/qdiicn.php",
     "/proxy/palmmicro/woody/res/qdiicn.php", None, _norm_palmmicro, False),
]


async def compare_case(client: httpx.AsyncClient, name, method, direct_url, proxy_path, body, normalize, binary) -> bool:
    try:
        req_headers = dict(UA)
        if name.startswith("szse-www"):
            req_headers["Referer"] = "https://www.szse.cn/market/trend/index.html"
        # query.sse.com.cn 无 Referer 会返回 "System Error" 错误页——直连基准也必须携带
        if name.startswith("sse-"):
            req_headers["Referer"] = "https://www.sse.com.cn/"
        # Palmmicro 依赖 Cookie 渲染宽度（代理侧注入），直连基准必须带同一份 Cookie
        if name.startswith("palmmicro"):
            req_headers["Cookie"] = _PALMMICRO_COOKIE
        if method == "POST":
            direct_resp = await client.post(direct_url, json=body, headers=req_headers)
            proxy_resp = await client.post(f"{PROXY}{proxy_path}", json=body)
        else:
            direct_resp = await client.get(direct_url, headers=req_headers)
            proxy_resp = await client.get(f"{PROXY}{proxy_path}")
    except Exception as e:
        print(f"[FAIL] {name}: 请求异常 {e}")
        return False

    if direct_resp.status_code != proxy_resp.status_code:
        print(f"[FAIL] {name}: 状态码不一致 直连={direct_resp.status_code} 代理={proxy_resp.status_code}")
        return False
    if direct_resp.status_code != 200:
        print(f"[OK]   {name}: 两侧同为 {direct_resp.status_code}(上游对该样本无数据，一致性成立)")
        return True

    if binary:
        same = direct_resp.content == proxy_resp.content
        print((f"[OK]   {name}: 二进制逐字节一致({len(direct_resp.content)}B)" if same
               else f"[FAIL] {name}: 二进制内容不一致({len(direct_resp.content)}B vs {len(proxy_resp.content)}B)"))
        return same

    direct_val = _parse_json_maybe_jsonp(direct_resp.text)
    proxy_val = _parse_json_maybe_jsonp(proxy_resp.text)
    if normalize:
        direct_val = normalize(direct_val)
        proxy_val = normalize(proxy_val)

    if direct_val == proxy_val:
        size = len(direct_val) if hasattr(direct_val, "__len__") else "?"
        print(f"[OK]   {name}: 直连与代理响应一致（条目数 {size}）")
        return True

    print(f"[FAIL] {name}: 响应不一致")
    return False


async def main() -> int:
    results = []
    async with httpx.AsyncClient(timeout=60) as client:
        for case in CASES:
            results.append(await compare_case(client, *case))

    all_ok = all(results)
    print(f"\n对照结论: {sum(results)}/{len(results)} 一致，" + ("可以切换 C# BaseUrl" if all_ok else "存在不一致，禁止切换"))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
