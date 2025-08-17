"""Specifications for processed output variables.

This module contains specifications to use for output variables from processed
MarU data, such as produced by the script `maru_xlsx_to_parquet.py`.
"""
import typing as tp

import polars as pl

from cicero_maru_utils.labels.columns import MaruCol

from .types import (
    OutputVarSpec,
)
