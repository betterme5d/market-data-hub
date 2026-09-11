# -*- coding: utf-8 -*-
"""
Parquet 列式存储引擎：支持 Hive-style 多维分区目录、Schema 宽容合并与原子文件覆盖。
"""
import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# 缓存路径令牌白名单：只允许字母/数字/下划线/点/连字符。
# 维度值（如 adj/interval）与 key 都可能来自请求参数，原样拼接会被 ../../ 穿越到其它命名空间
#（跨命名空间写文件 = 缓存投毒），因此统一在存储层做最后一道拦截。
_SAFE_PATH_TOKEN = re.compile(r"^[A-Za-z0-9_.\-]+$")


def _ensure_safe_path_token(kind: str, value) -> str:
    """校验缓存路径片段；含分隔符、上跳或非法字符一律抛 ValueError。"""
    text = str(value)
    if (
        not text
        or text in (".", "..")
        or "/" in text
        or "\\" in text
        or not _SAFE_PATH_TOKEN.match(text)
    ):
        raise ValueError(f"unsafe {kind} for cache path: {value!r}")
    return text


class ParquetStorageEngine:
    """基于 Parquet + JSON 元数据的时序持久化存储引擎。

    <b>同 key 写入串行化</b>：write_records 是"读全文件 → 拼接 → 去重 → 原子覆写"，
    必须保证同一只标的同时只有一个写入者。基金份额的全市场扇出是线程池并发写
    （一次约 1500 只基金），而多个重叠的扇出批次（逐只基金补录连发、批量与抽屉的
    优先队列并发）会同时写同一批文件——没有这把锁时后写的会覆盖先写的，
    实测两个线程各写 100 天，3 轮里 2 轮只剩 100 天。
    """

    def __init__(self, base_dir: str | Path = "data/cache"):
        self.base_dir = Path(base_dir)
        self._write_locks: Dict[str, threading.Lock] = {}
        self._write_locks_guard = threading.Lock()

    def _get_write_lock(self, path: str) -> threading.Lock:
        """按文件粒度取写锁（进程内）。"""
        with self._write_locks_guard:
            lock = self._write_locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self._write_locks[path] = lock
            return lock

    @staticmethod
    def _atomic_replace(tmp_path: str, target_path: str, retries: int = 5) -> None:
        """
        os.replace 的带重试封装。Windows 上目标文件被正在读它的线程占用时
        （pandas 读 parquet）会抛 WinError 5 Access is denied，重试即可；Linux 无此问题。
        """
        for attempt in range(retries):
            try:
                os.replace(tmp_path, target_path)
                return
            except PermissionError:
                if attempt == retries - 1:
                    raise
                time.sleep(0.05 * (attempt + 1))

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
        folder = self.base_dir / _ensure_safe_path_token("namespace", namespace)
        if dimensions:
            for k, v in sorted(dimensions.items()):
                dim_key = _ensure_safe_path_token("dimension key", k)
                dim_val = _ensure_safe_path_token(f"dimension value of {k}", v)
                folder = folder / f"{dim_key}={dim_val}"

        safe_key = _ensure_safe_path_token("key", key)
        p_path = folder / f"{safe_key}.parquet"
        m_path = folder / f"{safe_key}.meta.json"

        # 双保险：解析后的路径必须仍在 base_dir 之内
        base_resolved = Path(self.base_dir).resolve()
        target_resolved = Path(p_path).resolve()
        if target_resolved != base_resolved and base_resolved not in target_resolved.parents:
            raise ValueError(f"cache path escapes base dir: {target_resolved}")

        return str(p_path), str(m_path)

    @staticmethod
    def _read_metadata_unlocked(m_path: str) -> Optional[Dict[str, Any]]:
        """读取元数据文件（不加锁；并发控制由调用方负责）。"""
        if not os.path.exists(m_path):
            return None
        try:
            with open(m_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read metadata {m_path}: {e}")
            return None

    def read_metadata(
        self,
        namespace: str,
        key: str,
        dimensions: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """读取元数据 JSON 文件。若不存在返回 None。"""
        _, m_path = self.get_paths(namespace, key, dimensions)
        return self._read_metadata_unlocked(m_path)

    def _write_metadata_unlocked(self, m_path: str, metadata: Dict[str, Any]) -> None:
        """原子写入元数据文件（不加锁；并发控制由调用方负责）。"""
        os.makedirs(os.path.dirname(m_path), exist_ok=True)
        tmp_path = f"{m_path}.tmp.{uuid.uuid4().hex}"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            self._atomic_replace(tmp_path, m_path)
        except Exception as e:
            logger.error(f"Failed to write metadata {m_path}: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def write_metadata(
        self,
        namespace: str,
        key: str,
        metadata: Dict[str, Any],
        dimensions: Optional[Dict[str, str]] = None,
    ) -> None:
        """原子写入元数据 JSON 文件（同 key 串行）。"""
        _, m_path = self.get_paths(namespace, key, dimensions)
        with self._get_write_lock(m_path):
            self._write_metadata_unlocked(m_path, metadata)

    def update_metadata(
        self,
        namespace: str,
        key: str,
        mutator: Callable[[Dict[str, Any]], Dict[str, Any]],
        dimensions: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """在「读-改-写全程持有同 key 文件锁」的前提下更新元数据，返回更新后的元数据。

        用于并发落盘场景（如份额全市场扇出的多线程写入）：自己 read + write 会出现
        「双方都读到旧值 → 各自合并 → 后写覆盖先写」而丢更新（覆盖区间回退）。
        """
        _, m_path = self.get_paths(namespace, key, dimensions)
        with self._get_write_lock(m_path):
            current = self._read_metadata_unlocked(m_path) or {}
            updated = mutator(dict(current))
            self._write_metadata_unlocked(m_path, updated)
            return updated

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

            # 将 NaN / NaT 替换为标准 Python None，避免 Pydantic 校验与 JSON 序列化异常
            raw_records = df.to_dict(orient="records")
            return [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in raw_records]
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

        # 读-改-写整段持锁：并发写同一 key 时，先读到的写入者会覆盖掉后写入者刚落的行
        with self._get_write_lock(p_path):
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
                self._atomic_replace(tmp_path, p_path)
                return len(combined)
            except Exception as e:
                logger.error(f"Failed to write parquet for {p_path}: {e}")
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            raise
