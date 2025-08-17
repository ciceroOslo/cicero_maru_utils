#!python
"""
Utility to create MarU variables for use in LOKFRAM from a parquet file of
processed data from MarU reports.

The parquet file must have been produced from MarU Excel report files using
`maru_xlsx_to_parquet`.
"""
import argparse
import dataclasses
from pathlib import Path
import typing as tp
import sys

import polars as pl
import polars.selectors as cs
import xlsxwriter
from xlsxwriter import Workbook as XlsxWorkbook

from cicero_maru_utils.labels.columns import (
    MaruCol,
    get_maru_cols
)



OutputObj: tp.TypeAlias = XlsxWorkbook
InputObj: tp.TypeAlias = pl.LazyFrame

class ProcessingFunc(tp.Protocol):
    """Protocol for processing functions."""
    def __call__(
            self,
            df: pl.LazyFrame,
            *,
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


def get_input_data_obj(in_file: Path|tp.IO[bytes]) -> InputObj:
    """Get an input data object for the given input file object."""
    if isinstance(in_file, Path) and not in_file.exists():
        raise FileNotFoundError(f'Input file "{in_file}" does not exist.')
    return pl.scan_parquet(in_file)

def get_output_obj(out_file: Path|tp.IO[bytes]) -> OutputObj:
    """Get an output data object for the given output file object."""
    if isinstance(out_file, Path) and out_file.exists():
        raise FileExistsError(f'Output file "{out_file}" already exists.')
    return xlsxwriter.Workbook(out_file)


def process_and_write_output(
        input_obj: InputObj,
        output_obj: OutputObj,
        var_spec: OutputVarSpec,
) -> None:
    """Process input data into a single variable and write to output object."""



def main() -> None:
    """
    Main function to orchestrate the file processing and conversion.
    """
    parser = argparse.ArgumentParser(
        description=(
            'Create MarU variables for use in LOKFRAM from a parquet file of '
            'processed data from MarU Excel reports.'
        ),
        epilog=(
            'The following output variable IDs are currently provided in the '
            'output file: "' + '", "'.join(output_var_specs.keys()) + '"'
        ),
    )
    parser.add_argument(
        '--in-file',
        type=Path,
        required=True,
        help='Path for the input .parquet file.',
    )
    parser.add_argument(
        '--out-file',
        type=Path,
        required=True,
        help=(
            'Path for the output file. Only excel output is supported at the '
            'moment.'
        ),
    )

    args: argparse.Namespace = parser.parse_args()

    print(f'Opening input file "{args.in_file}"...')
    in_file: Path = args.in_file
    print(f'Obtaining output object for output file "{args.out_file}"...')
    out_file: Path = args.out_file

    input_obj: InputObj = get_input_data_obj(in_file)
    output_obj: OutputObj = get_output_obj(out_file)

    curr_var: str = '<no variables processed yet>'
    try:
        for _var_name, _var_spec in output_var_specs.items():
            curr_var = _var_name
            print(f'Processing variable "{_var_name}"...')
            process_and_write_output(
                input_obj=input_obj,
                output_obj=output_obj,
                var_spec=_var_spec,
            )
    except Exception as err:
        sys.stderr.write(
            f'An error occurred while processing the variable "{curr_var}". '
            f'Further processing was aborted.'
        )
        raise
    finally:
        print('Closing output file...')
        output_obj.close()

    print('Processing finished.')
