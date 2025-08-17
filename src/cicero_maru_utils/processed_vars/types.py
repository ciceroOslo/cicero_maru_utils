"""Type definitions for processed variable specs"""
import dataclasses
import typing as tp

import polars as pl

from cicero_maru_utils.labels.columns import MaruCol



class ProcessingFunc(tp.Protocol):
    """Protocol for processing functions."""
    def __call__(
            self,
            df: pl.LazyFrame,
            *,
            maru_cols: MaruCol,
            output_var_name: str,
            output_value_col: str|None = None,
    ) -> pl.LazyFrame:
        ...

@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class OutputVarSpec:
    """Specification for an output variable."""
    name: str
    sheet_name: str
    output_value_col: str|None = None
    processing_func: ProcessingFunc

    def __post_init__(self) -> None:
        sheet_name: str = self.sheet_name
        max_sheet_name_len: int = 31
        if len(sheet_name) > max_sheet_name_len:
            raise ValueError(
                f'Sheet name "{sheet_name}" is too long. '
                'It must be {max_sheet_name_len} characters or less.'
            )
