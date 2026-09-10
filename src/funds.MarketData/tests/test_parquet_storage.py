# -*- coding: utf-8 -*-
import os
import shutil
import tempfile
import pytest
from core.timeseries_cache.storage import ParquetStorageEngine

# 测试产物落在系统临时目录：仓库目录被 dev 容器挂载并 watch，
# 在源码树内反复建/删目录会让 uvicorn 的 StatReload 看门狗 rglob 撞上已消失的目录而崩溃
TEST_CACHE_DIR = os.path.join(tempfile.gettempdir(), f"funds_parquet_store_test_{os.getpid()}")


@pytest.fixture(autouse=True)
def clean_test_cache():
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)
    yield
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)


def test_partition_path():
    engine = ParquetStorageEngine(base_dir=TEST_CACHE_DIR)
    p_path, m_path = engine.get_paths("fund_nav", "510300", {"source": "eastmoney"})
    expected_dir = TEST_CACHE_DIR.replace("\\", "/")
    assert f"{expected_dir}/fund_nav/source=eastmoney/510300.parquet" in p_path.replace("\\", "/")
    assert f"{expected_dir}/fund_nav/source=eastmoney/510300.meta.json" in m_path.replace("\\", "/")

    # 多维正交排序一致性
    p_path2, _ = engine.get_paths("quotes_kline", "000001.SZ", {"source": "tencent", "adj": "hfq", "interval": "1d"})
    normalized = p_path2.replace("\\", "/")
    assert "adj=hfq" in normalized
    assert "interval=1d" in normalized
    assert "source=tencent" in normalized


def test_write_and_read_records():
    engine = ParquetStorageEngine(base_dir=TEST_CACHE_DIR)
    records = [
        {"nav_date": "2024-01-02", "unit_nav": 3.5, "accum_nav": 3.6},
        {"nav_date": "2024-01-03", "unit_nav": 3.55, "accum_nav": 3.65},
    ]
    engine.write_records("fund_nav", "510300", records, date_column="nav_date", dimensions={"source": "eastmoney"})

    # 全量读取
    read_all = engine.read_records("fund_nav", "510300", date_column="nav_date", dimensions={"source": "eastmoney"})
    assert len(read_all) == 2
    assert read_all[0]["nav_date"] == "2024-01-02"
    assert read_all[0]["unit_nav"] == 3.5

    # 区间过滤读取
    read_filtered = engine.read_records(
        "fund_nav", "510300", start_date="2024-01-03", end_date="2024-01-03",
        date_column="nav_date", dimensions={"source": "eastmoney"}
    )
    assert len(read_filtered) == 1
    assert read_filtered[0]["nav_date"] == "2024-01-03"


def test_upsert_merge_and_deduplicate():
    engine = ParquetStorageEngine(base_dir=TEST_CACHE_DIR)
    initial = [
        {"nav_date": "2024-01-02", "unit_nav": 3.5},
        {"nav_date": "2024-01-03", "unit_nav": 3.55},
    ]
    engine.write_records("fund_nav", "510300", initial, date_column="nav_date")

    # 写入包含重叠覆盖与新增日期
    new_data = [
        {"nav_date": "2024-01-03", "unit_nav": 3.56},  # 更新
        {"nav_date": "2024-01-04", "unit_nav": 3.60},  # 新增
        {"nav_date": "2024-01-01", "unit_nav": 3.48},  # 前向新增
    ]
    engine.write_records("fund_nav", "510300", new_data, date_column="nav_date")

    merged = engine.read_records("fund_nav", "510300", date_column="nav_date")
    assert len(merged) == 4
    # 验证排序与去重覆写
    assert [r["nav_date"] for r in merged] == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
    assert merged[2]["unit_nav"] == 3.56


def test_metadata_read_and_write():
    engine = ParquetStorageEngine(base_dir=TEST_CACHE_DIR)
    meta = {
        "intervals": [["2024-01-01", "2024-05-31"]],
        "row_count": 100
    }
    engine.write_metadata("fund_nav", "510300", meta, dimensions={"source": "eastmoney"})

    loaded = engine.read_metadata("fund_nav", "510300", dimensions={"source": "eastmoney"})
    assert loaded is not None
    assert loaded["intervals"] == [["2024-01-01", "2024-05-31"]]
    assert loaded["row_count"] == 100
