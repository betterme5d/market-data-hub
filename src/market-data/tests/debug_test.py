import os
import sys
import asyncio
import json
import pandas as pd
from datetime import datetime

# 将父目录加入 sys.path 以便导入 main
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# 1. 尝试加载 .env.development 配置文件
def load_env_dev():
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env.development")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value
        print(f"Loaded environment from: {os.path.abspath(env_path)}")


load_env_dev()

# 2. 导入 main 中的接口函数
# 注意：导入时 main.py 会执行全局变量初始化（探针注册、后台探测循环等）
from routers.quotes import get_quote, get_history
from routers.health import health


async def run_tests():
    target = "1699.T"

    print(f"\n{'='*20} Testing Health Directly {'='*20}")
    h_res = await health()
    print(f"Health: {json.dumps(h_res, indent=2)}")

    print(f"\n{'='*20} Testing Quote Directly: {target} {'='*20}")
    try:
        q_res_raw = await get_quote(target)
        q_res = q_res_raw.model_dump() if hasattr(q_res_raw, "model_dump") else q_res_raw
        print(f"Price: {q_res.get('price')} (Date: {q_res.get('date')})")
        print(f"OHL: {q_res.get('open')}/{q_res.get('high')}/{q_res.get('low')}")
    except Exception as e:
        print(f"Quote Error: {e}")

    print(f"\n{'='*20} Testing History Directly: {target} {'='*20}")
    try:
        # 模拟 FastAPI 的 Query 参数调用
        hist_res = await get_history(
            symbol=target, period="1mo", interval="1d", start=None, end=None, adj="hfq"
        )
        data = hist_res.get("data", [])
        if data:
            df = pd.DataFrame(data)
            print("\nFirst 5 rows (sorted descending):")
            cols = ["Date", "Open", "High", "Low", "Close", "Adj Close", "PreClose"]
            print(df[cols].head())
        else:
            print("No history data.")
    except Exception as e:
        print(f"History Error: {e}")


if __name__ == "__main__":
    asyncio.run(run_tests())
