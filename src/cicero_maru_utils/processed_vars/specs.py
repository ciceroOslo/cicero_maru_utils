"""Specifications for processed output variables.

This module contains specifications to use for output variables from processed
MarU data, such as produced by the script `maru_xlsx_to_parquet.py`.
"""
from collections.abc import Mapping
import dataclasses
import enum
import functools
import typing as tp

import polars as pl
import polars.selectors as cs

from cicero_maru_utils.labels.columns import MaruCol

from .types import (
    OutputVarSpec,
)



class GHG(enum.StrEnum):
    """Selects a greenhouse gas species."""
    CO2 = enum.auto()
    CH4 = enum.auto()
    N2O = enum.auto()


@dataclasses.dataclass(kw_only=True, slots=True)
class StavangerOutputVars[_T]:
    """Attributes for output variables for Stavanger."""
    ENERGY_SUM_KWH: _T
    ENERGY_PER_PHASE_KWH: _T
    ENERGY_PER_GT_KWH: _T
    ENERGY_PER_VOYAGE_TYPE_KWH: _T
    FUEL_SUM_TONN: _T
    FUEL_PER_GT_TONN: _T
    CO2_SUM_TONN: _T
    CO2_PER_PHASE_TONN: _T
    CO2_PER_GT_TONN: _T
    CH4_SUM_TONN: _T
    CH4_PER_PHASE_TONN: _T
    CH4_PER_GT_TONN: _T
    N2O_SUM_TONN: _T
    N2O_PER_PHASE_TONN: _T
    N2O_PER_GT_TONN: _T

@dataclasses.dataclass(kw_only=True, slots=True)
class StavangerOutputVarNames(StavangerOutputVars[str]):
    """Names of output variables for Stavanger."""
    ENERGY_SUM_KWH: str = 'maru_energibehov_sum_kwh'
    ENERGY_PER_PHASE_KWH: str = 'maru_energibehov_per_fase_kwh'
    ENERGY_PER_GT_KWH: str = 'maru_energibehov_per_gt_kwh'
    ENERGY_PER_VOYAGE_TYPE_KWH: str = 'maru_energibehov_per_voyage_type_kwh'
    FUEL_SUM_TONN: str = 'maru_fuel_sum_tonn'
    FUEL_PER_GT_TONN: str = 'maru_fuel_per_gt_tonn'
    CO2_SUM_TONN: str = 'maru_co2_sum_tonn'
    CO2_PER_PHASE_TONN: str = 'maru_co2_per_fase_tonn'
    CO2_PER_GT_TONN: str = 'maru_co2_per_gt_tonn'
    CH4_SUM_TONN: str = 'maru_ch4_sum_tonn'
    CH4_PER_PHASE_TONN: str = 'maru_ch4_per_fase_tonn'
    CH4_PER_GT_TONN: str = 'maru_ch4_per_gt_tonn'
    N2O_SUM_TONN: str = 'maru_n2o_sum_tonn'
    N2O_PER_PHASE_TONN: str = 'maru_n2o_per_fase_tonn'
    N2O_PER_GT_TONN: str = 'maru_n2o_per_gt_tonn'

VAR_NAMES: tp.Final[StavangerOutputVarNames] = StavangerOutputVarNames()

@dataclasses.dataclass(kw_only=True, slots=True)
class StavangerOutputSheetNames(StavangerOutputVarNames):
    """Names of output worksheets for Stavanger."""
    ENERGY_PER_VOYAGE_TYPE_KWH: str = 'maru_energ_per_voyage_type_kwh'

SHEET_NAMES: tp.Final[StavangerOutputSheetNames] = StavangerOutputSheetNames()

group_by_common: tp.Final[tp.Sequence[str]] = (
    MaruCol.municipality_name,
    MaruCol.vessel_type,
    MaruCol.municipality_voyage_type,
    MaruCol.year,
)


def _select_ghg_value_col(
        ghg: GHG,
        maru_cols: MaruCol,
) -> str:
    """Get the right MarU value column for a given GHG species."""
    ghg = GHG(ghg)  # Ensure that `ghg` is a valid enum
    data_value_col: str = (
        maru_cols.co2 if ghg == GHG.CO2 else
        maru_cols.ch4 if ghg == GHG.CH4 else
        maru_cols.n2o if ghg == GHG.N2O else
        ''
    )
    if data_value_col == '':
        raise ValueError(
            'Invalid GHG value passed. This should not really be possible '
            'at this stage, and suggests a bug in the code.'
        )
    return data_value_col


def _process_ghg_sum_tonn(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        ghg: GHG,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process GHG sum in tonnes."""
    data_value_col: str = _select_ghg_value_col(ghg, maru_cols)
    if output_value_col is None:
        output_value_col = data_value_col
    return (
        df
        .group_by(*group_by_common)
        .agg(
            pl.sum(data_value_col).alias(output_value_col)
        )
        .sort(cs.exclude(cs.by_name(output_value_col)))
    )


def _process_ghg_per_gt_tonn(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        ghg: GHG,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process GHG per GT tonn."""
    data_value_col: str = _select_ghg_value_col(ghg, maru_cols)
    if output_value_col is None:
        output_value_col = data_value_col
    return (
        df
        .group_by(*group_by_common, maru_cols.gt_group)
        .agg(
            pl.sum(data_value_col).alias(output_value_col)
        )
        .sort(cs.exclude(cs.by_name(output_value_col)))
    )


def _process_ghg_per_phase_tonn(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        ghg: GHG,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process GHG per phase in tonnes."""
    data_value_col: str = _select_ghg_value_col(ghg, maru_cols)
    if output_value_col is None:
        output_value_col = data_value_col
    return (
        df
        .group_by(*group_by_common, maru_cols.phase)
        .agg(
            pl.sum(data_value_col).alias(output_value_col)
        )
        .sort(cs.exclude(cs.by_name(output_value_col)))
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


def _process_energy_per_voyage_type_kwh(
        df: pl.LazyFrame,
        *,
        maru_cols: MaruCol,
        output_var_name: str,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process energy per voyage type in kWh."""
    if output_value_col is None:
        output_value_col = maru_cols.energy_kwh
    return (
        df
        .group_by(
            pl.col(*group_by_common, maru_cols.voyage_type)
            .exclude(maru_cols.municipality_voyage_type)
        )
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
    VAR_NAMES.ENERGY_SUM_KWH: OutputVarSpec(
        name=VAR_NAMES.ENERGY_SUM_KWH,
        sheet_name=SHEET_NAMES.ENERGY_SUM_KWH,
        processing_func=_process_energy_sum_kwh
    ),
    VAR_NAMES.ENERGY_PER_PHASE_KWH: OutputVarSpec(
        name=VAR_NAMES.ENERGY_PER_PHASE_KWH,
        sheet_name=SHEET_NAMES.ENERGY_PER_PHASE_KWH,
        processing_func=_process_energy_per_phase_kwh
    ),
    VAR_NAMES.ENERGY_PER_GT_KWH: OutputVarSpec(
        name=VAR_NAMES.ENERGY_PER_GT_KWH,
        sheet_name=SHEET_NAMES.ENERGY_PER_GT_KWH,
        processing_func=_process_energy_per_gt_kwh
    ),
    VAR_NAMES.ENERGY_PER_VOYAGE_TYPE_KWH: OutputVarSpec(
        name=VAR_NAMES.ENERGY_PER_VOYAGE_TYPE_KWH,
        sheet_name=SHEET_NAMES.ENERGY_PER_VOYAGE_TYPE_KWH,
        processing_func=_process_energy_per_voyage_type_kwh
    ),
    VAR_NAMES.FUEL_SUM_TONN: OutputVarSpec(
        name=VAR_NAMES.FUEL_SUM_TONN,
        sheet_name=SHEET_NAMES.FUEL_SUM_TONN,
        processing_func=_process_fuel_sum_tonn
    ),
    VAR_NAMES.FUEL_PER_GT_TONN: OutputVarSpec(
        name=VAR_NAMES.FUEL_PER_GT_TONN,
        sheet_name=SHEET_NAMES.FUEL_PER_GT_TONN,
        processing_func=_process_fuel_per_gt_tonn
    ),
    VAR_NAMES.CO2_SUM_TONN: OutputVarSpec(
        name=VAR_NAMES.CO2_SUM_TONN,
        sheet_name=SHEET_NAMES.CO2_SUM_TONN,
        processing_func=functools.partial(
            _process_ghg_sum_tonn,
            ghg=GHG.CO2,
        )
    ),
}
