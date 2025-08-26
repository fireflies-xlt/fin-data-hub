import unittest
import pytest

from fin_data_hub.data.tushare.tushare_data import sync_trade_calendar_data


class TestTushareDataIntegration(unittest.TestCase):
    """Tushare 数据同步集成测试"""
    
    @pytest.skip(reason="跳过集成测试，需要真实的数据库和API")
    def test_sync_trade_calendar_data_integration(self):
        """测试交易日历数据同步 - 集成测试"""
        result = sync_trade_calendar_data()
        assert result is not None
    
if __name__ == '__main__':
    unittest.main()
