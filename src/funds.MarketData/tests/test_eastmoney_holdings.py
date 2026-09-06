import asyncio
import os
import sys
import json
import pytest

pytestmark = pytest.mark.integration  # 需要真实外部网络，默认跳过

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from providers.funds.eastmoney import EastmoneySource

def find_col_indices(headers, is_bond=False):
    indices = {}
    for i, h in enumerate(headers):
        h_clean = h.strip()
        if "代码" in h_clean:
            indices["code"] = i
        elif "名称" in h_clean:
            indices["name"] = i
        elif "占净值" in h_clean or "比例" in h_clean:
            indices["percent"] = i
        elif "持股数" in h_clean or "持股数量" in h_clean:
            indices["shares"] = i
        elif "市值" in h_clean or "持仓市值" in h_clean:
            indices["amount"] = i
            
    # Fallback default values
    if "code" not in indices:
        indices["code"] = 1
    if "name" not in indices:
        indices["name"] = 2
        
    if is_bond:
        if "percent" not in indices:
            indices["percent"] = 3
        if "amount" not in indices:
            indices["amount"] = 4
    else:
        if "percent" not in indices:
            indices["percent"] = 4
        if "shares" not in indices:
            indices["shares"] = 5
        if "amount" not in indices:
            indices["amount"] = 6
            
    return indices

async def test_main():
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    provider = EastmoneySource()
    
    # 1. Fetch 160105 in 2026
    print("Fetching portfolio for 160105 in 2026...")
    res_160105 = await provider.get_portfolio("160105", 2026)
    print(f"Symbol: {res_160105['symbol']}, Year: {res_160105['year']}")
    for p in res_160105['portfolios']:
        print(f"Report Date: {p['report_date']} ({p['quarter_name']})")
        for h in p['holdings'][:5]:
            print(f"  [{h['asset_type']}] Rank {h['rank']}: {h['symbol_code']} {h['symbol_name']} - pct: {h['holding_percent']}%, shares: {h['holding_shares']}万股, amt: {h['holding_amount']}万元")

    # 2. Fetch 160127 in 2025
    print("\nFetching portfolio for 160127 in 2025...")
    res_160127 = await provider.get_portfolio("160127", 2025)
    print(f"Symbol: {res_160127['symbol']}, Year: {res_160127['year']}")
    for p in res_160127['portfolios']:
        print(f"Report Date: {p['report_date']} ({p['quarter_name']})")
        for h in p['holdings'][:5]:
            print(f"  [{h['asset_type']}] Rank {h['rank']}: {h['symbol_code']} {h['symbol_name']} - pct: {h['holding_percent']}%, shares: {h['holding_shares']}万股, amt: {h['holding_amount']}万元")




if __name__ == "__main__":
    asyncio.run(test_main())




if __name__ == "__main__":
    asyncio.run(test_main())
