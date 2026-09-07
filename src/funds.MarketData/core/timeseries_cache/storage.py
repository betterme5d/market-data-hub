# -*- coding: utf-8 -*-
"""
Parquet 列式存储引擎：支持 Hive-style 多维分区目录、Schema 宽容合并与原子文件覆盖。
"""
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class ParquetStorageEngine:
    """基于 Parquet + JSON 元数据的时序持久化存储引擎。"""

    def __init__(self, base_dir: str | Path = "data/cache"):
        self.base_dir = Path(base_dir)

    def get_paths(
        self,
        namespace: str,
        key: str,
        dimensions: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, str]:
        """
        获取指定数据项的 Parquet 文件路径与 Meta JSON 路径。
        维度字典会自动按 Key 排序生成 Hive-style 目录 (例如 source=eastmoney/adj=hfq)。
        """
        folder = self.base_dir / namespace
        if dimensions:
            for k, v in sorted(dimensions.items()):
                folder = folder / f"{k}={v}"

        p_path = folder / f"{key}.parquet"
        m_path = folder / f"{key}.meta.json"
        return str(p_path), str(m_path)

    def read_metadata(
        self,
        namespace: str,
        key: str,
        dimensions: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """读取元数据 JSON 文件。若不存在返回 None。"""
        _, m_path = self.get_paths(namespace, key, dimensions)
        if not os.path.exists(m_path):
            return None
        try:
            with open(m_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read metadata for {namespace}/{key}: {e}")
            return None

    def write_metadata(
        self,
        namespace: str,
        key: str,
        metadata: Dict[str, Any],
        dimensions: Optional[Dict[str, str]] = None,
    ) -> None:
        """原子写入元数据 JSON 文件。"""
        _, m_path = self.get_paths(namespace, key, dimensions)
        os.makedirs(os.path.dirname(m_path), exist_ok=True)
        tmp_path = f"{m_path}.tmp.{uuid.uuid4().hex}"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, m_path)
        except Exception as e:
            logger.error(f"Failed to write metadata for {namespace}/{key}: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def read_records(
        self,
        namespace: str,
        key: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        date_column: str = "date",
        dimensions: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        读取 Parquet 中的时序记录，支持按日期过滤与返回字典列表。
        """
        p_path, _ = self.get_paths(namespace, key, dimensions)
        if not os.path.exists(p_path):
            return []

        try:
            df = pd.read_parquet(p_path, engine="pyarrow")
            if df.empty:
                return []

            if date_column in df.columns:
                # 统一转为字符串方便比较
                df[date_column] = df[date_column].astype(str)
                if start_date:
                    df = df[df[date_column] >= str(start_date)]
                if end_date:
                    df = df[df[date_column] <= str(end_date)]
                df = df.sort_values(by=[date_column], ascending=True)

            # 将 NaN 替换为 None，便于序列化为 JSON
            df_clean = df.where(pd.notnull(df), None)
            return df_clean.to_dict(orient="records")
        except Exception as e:
            logger.error(f"Failed to read parquet records from {p_path}: {e}")
            return []

    def write_records(
        self,
        namespace: str,
        key: str,
        records: List[Dict[str, Any]],
        date_column: str = "date",
        dimensions: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        原子追加写入记录：读出既有数据 -> Concat -> 按 date_column 去重覆写 -> 升序排序 -> 原子覆写。
        返回最终总行数。
        """
        if not records:
            # 没有新记录直接返回现有行数
            p_path, _ = self.get_paths(namespace, key, dimensions)
            if os.path.exists(p_path):
                try:
                    df_ex = pd.read_parquet(p_path, engine="pyarrow")
                    return len(df_ex)
                except Exception:
                    return 0
            return 0

        p_path, _ = self.get_paths(namespace, key, dimensions)
        os.makedirs(os.path.dirname(p_path), exist_ok=True)
        tmp_path = f"{p_path}.tmp.{uuid.uuid4().hex}"

        new_df = pd.DataFrame(records)
        if date_column in new_df.columns:
            new_df[date_column] = new_df[date_column].astype(str)

        if os.path.exists(p_path):
            try:
                existing_df = pd.read_parquet(p_path, engine="pyarrow")
                if date_column in existing_df.columns:
                    existing_df[date_column] = existing_df[date_column].astype(str)
                combined = pd.concat([existing_df, new_df], ignore_index=True)
            except Exception as e:
                logger.warning(f"Error reading existing parquet {p_path}, overwriting: {e}")
                combined = new_df
        else:
            combined = new_df

        if date_column in combined.columns:
            combined = combined.drop_duplicates(subset=[date_column], keep="last")
            combined = combined.sort_values(by=[date_column], ascending=True)

        try:
            combined.to_parquet(tmp_path, index=False, engine="pyarrow")
            os.replace(tmp_path, p_path)
            return len(combined)
        except Exception as e:
            logger.error(f"Failed to write parquet for {p_path}: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
