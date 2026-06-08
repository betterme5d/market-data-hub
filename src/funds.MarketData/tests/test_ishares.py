import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# 确保能导入 providers
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

class TestIsharesProvider(unittest.IsolatedAsyncioTestCase):
    @patch("httpx.AsyncClient.get")
    async def test_get_premium_discount_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        # 模拟网页中 Highcharts 数据结构
        mock_response.text = """
        <html>
          <script>
            var seriesData = [
              { x: Date.UTC(2026, 5, 1), y: Number((-0.19).toFixed(2)) },
              { x: Date.UTC(2026, 5, 2), y: 0.15 }
            ];
          </script>
        </html>
        """
        mock_get.return_value = mock_response

        # 由于 providers.ishares 还未创建，此处导入会报错，实现 failing test
        from providers.ishares import IsharesProvider
        provider = IsharesProvider()
        res = await provider.get_premium_discount("239458/ishares-core", "2026-06-01", "2026-06-05")
        
        self.assertEqual(len(res), 2)
        # Date.UTC(2026, 5, 1) 的月份 5 在 JS 中代表 6月 (0-indexed)
        self.assertEqual(res["2026-06-01"], -0.0019)
        self.assertEqual(res["2026-06-02"], 0.0015)

    @patch("httpx.AsyncClient.get")
    async def test_get_premium_discount_json_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        # 模拟新版 Astro 渲染中 HTML 包含 componentprops 属性
        mock_response.text = """
        <html>
          <body>
            <walrus-render-on-client componentprops="{&quot;containersByNameMap&quot;:{&quot;premium-discount-chart&quot;:{&quot;dataPointsByNameMap&quot;:{&quot;premiumDiscountChartData&quot;:{&quot;asOfDate&quot;:[20260601,20260602],&quot;value&quot;:[&quot;-0.19&quot;,&quot;0.15&quot;]}}}}}">
            </walrus-render-on-client>
          </body>
        </html>
        """
        mock_get.return_value = mock_response

        from providers.ishares import IsharesProvider
        provider = IsharesProvider()
        res = await provider.get_premium_discount("239458/ishares-core", "2026-06-01", "2026-06-05")
        
        self.assertEqual(len(res), 2)
        self.assertEqual(res["2026-06-01"], -0.0019)
        self.assertEqual(res["2026-06-02"], 0.0015)

if __name__ == "__main__":
    unittest.main()
