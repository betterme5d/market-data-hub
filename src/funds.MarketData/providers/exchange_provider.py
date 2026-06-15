import logging
import httpx
import pandas as pd
import io

logger = logging.getLogger(__name__)

class ExchangeProvider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
            'Referer': 'https://www.sse.com.cn/'
        }

    async def fetch_sse_funds(self) -> list:
        """
        Fetch SSE ETF and LOF funds.
        """
        results = []
        
        # ETF
        etf_url = "https://query.sse.com.cn/commonSoaQuery.do?isPagination=true&pageHelp.pageSize=10000&pageHelp.pageNo=1&pageHelp.beginPage=1&pageHelp.cacheSize=1&pageHelp.endPage=1&pagecache=false&sqlId=FUND_LIST&fundType=00&subClass=01%2C02%2C03%2C04%2C06%2C08%2C09%2C31%2C32%2C33%2C34%2C35%2C36%2C37%2C38&order="
        # LOF
        lof_url = "https://query.sse.com.cn/commonSoaQuery.do?isPagination=true&pageHelp.pageSize=10000&pageHelp.pageNo=1&pageHelp.beginPage=1&pageHelp.cacheSize=1&pageHelp.endPage=1&pagecache=false&sqlId=FUND_LIST&fundType=10&subClass=11%2C14%2C15&order="

        async with httpx.AsyncClient(verify=False) as client:
            try:
                # Fetch ETF
                resp_etf = await client.get(etf_url, headers=self.headers, timeout=30)
                if resp_etf.status_code == 200:
                    data = resp_etf.json()
                    for item in data.get('result', []):
                        results.append({
                            "fund_code": item.get('fundCode'),
                            "fund_name": item.get('secNameFull') or item.get('fundAbbr'),
                            "fund_type": "ETF",
                            "exchange": "SH"
                        })
                
                # Fetch LOF
                resp_lof = await client.get(lof_url, headers=self.headers, timeout=30)
                if resp_lof.status_code == 200:
                    data = resp_lof.json()
                    for item in data.get('result', []):
                        results.append({
                            "fund_code": item.get('fundCode'),
                            "fund_name": item.get('secNameFull') or item.get('fundAbbr'),
                            "fund_type": "LOF",
                            "exchange": "SH"
                        })
            except Exception as e:
                logger.error(f"Error fetching SSE funds: {e}")

        return results

    async def fetch_szse_funds(self) -> list:
        """
        Fetch SZSE ETF and LOF funds from Excel endpoints.
        """
        results = []
        
        # ETF
        etf_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1945&tab1PAGENO=1&random=0.6034564625944207&TABKEY=tab1"
        # LOF
        lof_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1945_LOF&tab1PAGENO=1&random=0.6993308271523111&TABKEY=tab1"

        async with httpx.AsyncClient(verify=False) as client:
            try:
                # Fetch ETF
                resp_etf = await client.get(etf_url, timeout=30)
                if resp_etf.status_code == 200:
                    df = pd.read_excel(io.BytesIO(resp_etf.content), engine='openpyxl')
                    # Assuming 1st col is Code, 2nd col is Name
                    for _, row in df.iterrows():
                        code = str(row.iloc[0]).strip()
                        name = str(row.iloc[1]).strip()
                        if code and code != 'nan':
                            # ensure 6 digits
                            try:
                                code = str(int(float(code))).zfill(6)
                            except:
                                code = code.zfill(6)
                            results.append({
                                "fund_code": code,
                                "fund_name": name,
                                "fund_type": "ETF",
                                "exchange": "SZ"
                            })

                # Fetch LOF
                resp_lof = await client.get(lof_url, timeout=30)
                if resp_lof.status_code == 200:
                    df = pd.read_excel(io.BytesIO(resp_lof.content), engine='openpyxl')
                    for _, row in df.iterrows():
                        code = str(row.iloc[0]).strip()
                        name = str(row.iloc[1]).strip()
                        if code and code != 'nan':
                            try:
                                code = str(int(float(code))).zfill(6)
                            except:
                                code = code.zfill(6)
                            results.append({
                                "fund_code": code,
                                "fund_name": name,
                                "fund_type": "LOF",
                                "exchange": "SZ"
                            })
            except Exception as e:
                logger.error(f"Error fetching SZSE funds: {e}")

        return results

    async def fetch_all_exchange_funds(self) -> list:
        """
        Fetch and combine all funds from SSE and SZSE.
        """
        sse_funds = await self.fetch_sse_funds()
        szse_funds = await self.fetch_szse_funds()
        return sse_funds + szse_funds
