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
    ENERGY_PER_PHASE_KWH = 'maru_energibehov_per_fase_kwh'

group_by_common: tp.Final[tp.Sequence[str]] = (
    MaruCol.municipality_name,
    MaruCol.vessel_type,
    MaruCol.municipality_voyage_type,
    MaruCol.year,
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


stavanger_output_specs_202508: tp.Final[Mapping[str, OutputVarSpec]] = {
    StavangerOutputVarNames.ENERGY_PER_PHASE_KWH: OutputVarSpec(
        name=StavangerOutputVarNames.ENERGY_PER_PHASE_KWH,
        sheet_name=StavangerOutputVarNames.ENERGY_PER_PHASE_KWH,
        processing_func=_process_energy_per_phase_kwh
    )
}
