import asyncio
import os
import sys
import json
import urllib.request
import urllib.parse
import time
import ssl

# 将父目录加入 sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# 模拟或真实的缓存文件路径
MOCK_FILE = os.path.join(os.path.dirname(__file__), "mock_valuations.json")


async def get_raw_data_from_api():
    """从东财接口抓取原始数据，并保存到本地"""
    print("开始从东财 API 请求原始数据...")
    params_0 = {
        "type": "0",
        "sort": "3",
        "orderType": "desc",
        "canbuy": "0",
        "pageIndex": "1",
        "pageSize": "40000",
        "callback": "",
        "_": str(int(time.time() * 1000)),
    }
    params_9 = {
        "type": "9",
        "sort": "3",
        "orderType": "desc",
        "canbuy": "0",
        "pageIndex": "1",
        "pageSize": "40000",
        "callback": "",
        "_": str(int(time.time() * 1000) + 1),
    }

    def fetch_sync(params):
        url = f"https://api.fund.eastmoney.com/FundGuZhi/GetFundGZList?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={
                "Referer": "https://fund.eastmoney.com/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
        )
        try:
            context = ssl._create_unverified_context()
            with urllib.request.urlopen(req, timeout=15, context=context) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as ex:
            print(f"请求失败 params={params}: {ex}")
            return {}

    loop = asyncio.get_event_loop()
    # 避免并发封锁，串行获取
    data_0 = await loop.run_in_executor(None, fetch_sync, params_0)
    await asyncio.sleep(0.5)
    data_9 = await loop.run_in_executor(None, fetch_sync, params_9)

    raw_data = {"data_0": data_0, "data_9": data_9}
    with open(MOCK_FILE, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=2)
    print(f"原始数据已成功保存至: {MOCK_FILE}")
    return raw_data


def load_local_data():
    """加载本地保存的原始数据"""
    if not os.path.exists(MOCK_FILE):
        return None
    with open(MOCK_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


async def debug_get_valuations(target_fund_code: str):
    """
    使用本地数据运行 get_valuations 的清洗过滤逻辑，
    并对 target_fund_code 打印详细的判断轨迹。
    """
    sys.stdout.reconfigure(encoding="utf-8")
    raw_data = load_local_data()
    if not raw_data:
        # 如果本地没有数据，就请求一次
        raw_data = await get_raw_data_from_api()

    list_0 = raw_data.get("data_0", {}).get("Data", {}).get("list", []) or []
    list_9 = raw_data.get("data_9", {}).get("Data", {}).get("list", []) or []

    print(f"\n加载数据成功: list_0 长度={len(list_0)}, list_9 长度={len(list_9)}")

    # 1. 查找目标基金在原始数据中是否存在
    found_in_0 = [x for x in list_0 if x.get("bzdm") == target_fund_code]
    found_in_9 = [x for x in list_9 if x.get("bzdm") == target_fund_code]

    print(f"目标基金 {target_fund_code} 在原始数据中的状态:")
    print(f"  - 在 list_0 (type=0) 中找到数量: {len(found_in_0)}")
    for i, item in enumerate(found_in_0):
        print(
            f"    第 {i+1} 个匹配: 名称='{item.get('jjjc')}', gsz='{item.get('gsz')}', dwjz='{item.get('dwjz')}'"
        )
    print(f"  - 在 list_9 (type=9) 中找到数量: {len(found_in_9)}")
    for i, item in enumerate(found_in_9):
        print(
            f"    第 {i+1} 个匹配: 名称='{item.get('jjjc')}', gsz='{item.get('gsz')}', dwjz='{item.get('dwjz')}'"
        )

    if not found_in_0 and not found_in_9:
        print(
            f"警告：原始数据中完全没有找到基金代码为 {target_fund_code} 的记录！请检查代码是否正确，或原始数据是否完整。"
        )

    # 2. 模拟合并与过滤过程
    merged = {}
    print("\n开始模拟 get_valuations 合并与清洗逻辑...")

    for item in list_0 + list_9:
        bzdm = item.get("bzdm")
        jjjc = item.get("jjjc") or ""

        is_target = bzdm == target_fund_code

        if is_target:
            print(f"\n[Trace {bzdm}] 发现目标基金: 名称='{jjjc}'")

        if not bzdm:
            if is_target:
                print(f"[Trace {bzdm}] 过滤原因: bzdm 为空")
            continue

        # 核心过滤 1: 是否以 16 或 5 开头
        code_prefix_ok = bzdm.startswith("16") or bzdm.startswith("5")
        if not code_prefix_ok:
            if is_target:
                print(f"[Trace {bzdm}] 过滤原因: 代码不以 16 或 5 开头")
            continue

        # 核心过滤 2: 联接/连接 基金过滤
        is_lianjie = "联接" in jjjc or "连接" in jjjc
        has_lof = "LOF" in jjjc
        has_a = "A" in jjjc
        if is_lianjie and (not has_lof and not has_a):
            if is_target:
                print(
                    f"[Trace {bzdm}] 过滤原因: 命中了联接基金过滤逻辑 -> '联接/连接' in 名称, 且 (not 'LOF' and not 'A')"
                )
                print(f"  - 名称是否含联接/连接: {is_lianjie}")
                print(f"  - 名称是否含LOF: {has_lof}")
                print(f"  - 名称是否含A: {has_a}")
            continue

        # 核心过滤 3: ETF 基金过滤
        is_etf = "ETF" in jjjc.upper()
        has_lof_upper = "LOF" in jjjc.upper()
        has_a_upper = "A" in jjjc.upper()
        if (
            is_etf
            and not ("联接" in jjjc or "连接" in jjjc)
            and not (has_lof_upper and has_a_upper)
        ):
            if is_target:
                print(
                    f"[Trace {bzdm}] 过滤原因: 命中了 ETF 过滤逻辑 -> 名称含有 'ETF' 且不含 '联接' 或者 '连接' 且不含 'LOF' 和 'A'"
                )
                print(f"  - 名称是否含ETF: {is_etf}")
                print(f"  - 名称是否含LOF: {has_lof_upper}")
                print(f"  - 名称是否含A: {has_a_upper}")
            continue

        # 核心过滤 4: 上交所场外老基金前缀 (519, 530, 540, 550)
        is_old_sh = bzdm.startswith("5") and bzdm.startswith(
            ("519", "530", "540", "550")
        )
        if is_old_sh:
            if is_target:
                print(
                    f"[Trace {bzdm}] 过滤原因: 命中了上交所场外老基金前缀 (519, 530, 540, 550)"
                )
            continue

        if is_target:
            print(f"[Trace {bzdm}] 成功通过第一阶段过滤，加入 merged 字典中")

        merged[bzdm] = item

    # 第一阶段合并过滤结果汇总
    print(f"\n[Trace {target_fund_code}] 第一阶段过滤与合并结束：")
    if target_fund_code in merged:
        print(
            f"  => 该基金【成功进入】第一阶段合并后的字典 (merged)，准备进入第二阶段。"
        )
    else:
        print(f"  => 该基金【已被过滤/未进入】第一阶段合并后的字典 (merged)。")

    # 3. 数据清洗和标准化过滤
    def to_float(val):
        if not val or val in ("---", "-", None):
            return None
        try:
            return float(str(val).replace("%", "").replace(",", "").strip())
        except ValueError:
            return None

    result_list = []
    print("\n开始模拟第二阶段数据清洗与 gsz 校验...")
    for bzdm, item in merged.items():
        is_target = bzdm == target_fund_code
        est_val = to_float(item.get("gsz"))

        if is_target:
            print(f"[Trace {bzdm}] 第二阶段校验:")
            print(f"  - 原始 gsz 字段值: '{item.get('gsz')}'")
            print(f"  - 转换后的 est_val: {est_val}")

        if est_val is None:
            if is_target:
                print(
                    f"[Trace {bzdm}] 过滤原因: est_val (gsz 估值) 转换为 float 后为 None (例如：为空, '-' 或 '---')"
                )
            continue

        if is_target:
            print(f"[Trace {bzdm}] 成功通过所有过滤，最终被保留！")

        result_list.append(
            {
                "fund_code": bzdm,
                "fund_name": item.get("jjjc"),
                "fund_type": item.get("FType"),
                "net_value": to_float(item.get("dwjz")),
                "estimated_value": est_val,
                "estimated_growth_rate": to_float(item.get("gszzl")),
                "valuation_date": item.get("gzrq"),
                "update_date": "MOCK_TIME",
            }
        )

    print(f"\n模拟结束。过滤后最终返回基金总数: {len(result_list)}")

    # 打印目标基金最终输出结果（若存在）
    print(f"\n[Trace {target_fund_code}] 第二阶段数据清洗与校验结束：")
    final_target_items = [x for x in result_list if x["fund_code"] == target_fund_code]
    if final_target_items:
        print(f"  => 该基金【最终保留】，清洗后的标准化数据如下：")
        print(json.dumps(final_target_items[0], indent=2, ensure_ascii=False))
    else:
        print(f"  => 该基金【最终被过滤】，最终结果列表中【不包含】此基金。")


if __name__ == "__main__":
    target_code = "160105"
    if len(sys.argv) > 1:
        target_code = sys.argv[1]

    asyncio.run(debug_get_valuations(target_code))
