import sys
import os
import unittest
from unittest.mock import patch, AsyncMock

# 确保能导入 providers
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

class TestKraneSharesProvider(unittest.IsolatedAsyncioTestCase):
    @patch("httpx.AsyncClient.get")
    async def test_get_premium_discount_success(self, mock_get):
        from unittest.mock import MagicMock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            [1780272000000, 0.0125],
            [1780617600000, -0.0035]
        ]
        mock_get.return_value = mock_response

        # 由于 providers.kraneshares 尚未实现，导入此模块将报错，达到 failing test 的目的
        from providers.kraneshares import KraneSharesProvider
        provider = KraneSharesProvider()
        res = await provider.get_premium_discount("7615", "2026-06-01", "2026-06-05")
        
        self.assertEqual(len(res), 2)
        self.assertEqual(res["2026-06-01"], 0.0125)
        self.assertEqual(res["2026-06-05"], -0.0035)

if __name__ == "__main__":
    unittest.main()
