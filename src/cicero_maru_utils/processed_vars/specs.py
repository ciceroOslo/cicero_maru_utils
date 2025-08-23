"""Specifications for processed output variables.

This module contains specifications to use for output variables from processed
MarU data, such as produced by the script `maru_xlsx_to_parquet.py`.
"""
from collections.abc import Mapping
import enum
import typing as tp

import polars as pl
import polars.selectors as cs

from cicero_maru_utils.labels.columns import MaruCol

from .types import (
    OutputVarSpec,
)



class StavangerOutputVarNames(enum.StrEnum):
    """Names of output variables for Stavanger."""
    ENERGY_SUM_KWH = 'maru_energibehov_sum_kwh'
    ENERGY_PER_PHASE_KWH = 'maru_energibehov_per_fase_kwh'
    ENERGY_PER_GT_KWH = 'maru_energibehov_per_gt_kwh'
    FUEL_SUM_TONN = 'maru_fuel_sum_tonn'
    FUEL_PER_GT_TONN = 'maru_fuel_per_gt_tonn'

group_by_common: tp.Final[tp.Sequence[str]] = (
    MaruCol.municipality_name,
    MaruCol.vessel_type,
    MaruCol.municipality_voyage_type,
    MaruCol.year,
)


def _process_energy_sum_kwh(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process energy sum in kWh."""
    if output_value_col is None:
        output_value_col = maru_cols.energy_kwh
    return (
        df
        .group_by(*group_by_common)
        .agg(
            pl.sum(maru_cols.energy_kwh).alias(output_value_col)
        )
        .sort(cs.exclude(cs.by_name(output_value_col)))
    )


def _process_energy_per_phase_kwh(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process energy per phase in kWh."""
    if output_value_col is None:
        output_value_col = maru_cols.energy_kwh
    return (
        df
        .group_by(*group_by_common, maru_cols.phase)
        .agg(
            pl.sum(maru_cols.energy_kwh).alias(output_value_col)
        )
        .sort(cs.exclude(cs.by_name(output_value_col)))
    )


def _process_energy_per_gt_kwh(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process energy per gross tonne group in kWh."""
    if output_value_col is None:
        output_value_col = maru_cols.energy_kwh
    return (
        df
        .group_by(*group_by_common, maru_cols.gt_group)
        .agg(
            pl.sum(maru_cols.energy_kwh).alias(output_value_col)
        )
        .sort(cs.exclude(cs.by_name(output_value_col)))
    )


def _process_fuel_sum_tonn(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process fuel sum in tonnes."""
    if output_value_col is None:
        output_value_col = maru_cols.fuel
    return (
        df
        .group_by(*group_by_common)
        .agg(
            pl.sum(maru_cols.fuel).alias(output_value_col)
        )
        .sort(cs.exclude(cs.by_name(output_value_col)))
    )


def _process_fuel_per_gt_tonn(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process fuel per GT tonn."""
    if output_value_col is None:
        output_value_col = maru_cols.fuel
    return (
        df
        .group_by(*group_by_common, maru_cols.gt_group)
        .agg(
            pl.sum(maru_cols.fuel).alias(output_value_col)
        )
        .sort(cs.exclude(cs.by_name(output_value_col)))
    )



stavanger_output_specs_202508: tp.Final[Mapping[str, OutputVarSpec]] = {
    StavangerOutputVarNames.ENERGY_SUM_KWH: OutputVarSpec(
        name=StavangerOutputVarNames.ENERGY_SUM_KWH,
        sheet_name=StavangerOutputVarNames.ENERGY_SUM_KWH,
        processing_func=_process_energy_sum_kwh
    ),
    StavangerOutputVarNames.ENERGY_PER_PHASE_KWH: OutputVarSpec(
        name=StavangerOutputVarNames.ENERGY_PER_PHASE_KWH,
        sheet_name=StavangerOutputVarNames.ENERGY_PER_PHASE_KWH,
        processing_func=_process_energy_per_phase_kwh
    ),
    StavangerOutputVarNames.ENERGY_PER_GT_KWH: OutputVarSpec(
        name=StavangerOutputVarNames.ENERGY_PER_GT_KWH,
        sheet_name=StavangerOutputVarNames.ENERGY_PER_GT_KWH,
        processing_func=_process_energy_per_gt_kwh
    ),
    StavangerOutputVarNames.FUEL_SUM_TONN: OutputVarSpec(
        name=StavangerOutputVarNames.FUEL_SUM_TONN,
        sheet_name=StavangerOutputVarNames.FUEL_SUM_TONN,
        processing_func=_process_fuel_sum_tonn
    ),
    StavangerOutputVarNames.FUEL_PER_GT_TONN: OutputVarSpec(
        name=StavangerOutputVarNames.FUEL_PER_GT_TONN,
        sheet_name=StavangerOutputVarNames.FUEL_PER_GT_TONN,
        processing_func=_process_fuel_per_gt_tonn
    ),
}
