#!python
"""
Utility to create MarU variables for use in LOKFRAM from a parquet file of
processed data from MarU reports.

The parquet file must have been produced from MarU Excel report files using
`maru_xlsx_to_parquet`.
"""
import argparse
from collections.abc import Mapping
from pathlib import Path
import typing as tp
import sys

import polars as pl
import polars.selectors as cs
import xlsxwriter
from xlsxwriter import Workbook as XlsxWorkbook

from cicero_maru_utils.labels.columns import (
    MaruCol,
    MaruVersion,
    get_maru_cols,
)
from cicero_maru_utils.processed_vars.specs import stavanger_output_specs_202508
from cicero_maru_utils.processed_vars.types import OutputVarSpec



output_var_specs: tp.Final[Mapping[str, OutputVarSpec]] \
    = stavanger_output_specs_202508
OutputObj: tp.TypeAlias = XlsxWorkbook
InputObj: tp.TypeAlias = pl.LazyFrame


def get_input_data_obj(
        in_file: Path|tp.IO[bytes],
        maru_cols: MaruCol,
        municipality_name: str,
) -> InputObj:
    """Get an input data object for the given input file object.

    Note that this function currently materializes the input (filtered to the
    correct municipality) and then re-lazies it before returning, in order to
    avoid repeated materialization during processing.
    """
    if isinstance(in_file, Path) and not in_file.exists():
        raise FileNotFoundError(f'Input file "{in_file}" does not exist.')
    df: pl.DataFrame = (
        pl.scan_parquet(in_file)
        .filter(pl.col(maru_cols.municipality_name) == municipality_name)
    ).collect()
    if df.height == 0:
        raise ValueError(
            f'No data found for municipality "{municipality_name}".'
        )
    return df.lazy()


def get_output_obj(out_file: Path|tp.IO[bytes]) -> OutputObj:
    """Get an output data object for the given output file object."""
    if isinstance(out_file, Path) and out_file.exists():
        raise FileExistsError(f'Output file "{out_file}" already exists.')
    return xlsxwriter.Workbook(out_file)


def process_and_write_output(
        *,
        input_obj: InputObj,
        output_obj: OutputObj,
        var_spec: OutputVarSpec,
        maru_cols: MaruCol,
        output_value_col: str|None = None,
) -> pl.LazyFrame:
    """Process input data into a single variable and write to output object."""
    output_lf: pl.LazyFrame = var_spec.processing_func(
        df=input_obj,
        maru_cols=maru_cols,
        output_var_name=var_spec.name,
        output_value_col=output_value_col or var_spec.output_value_col,
    )
    output_lf.collect().write_excel(
        output_obj,
        worksheet=var_spec.sheet_name,
    )
    return output_lf


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
    parser.add_argument(
        '--version',
        type=MaruVersion,
        required=True,
        choices=[_member for _member in MaruVersion],
        help='MarU report release version of the data.',
    )
    parser.add_argument(
        '--municipality-name',
        type=str,
        required=True,
        help=(
            'Name of the municipality for which the output variables are '
            'generated (not including municipality number).'
        ),
    )
    parser.add_argument(
        '--output-value-col',
        type=str,
        required=False,
    )

    args: argparse.Namespace = parser.parse_args()

    in_file: Path = args.in_file
    out_file: Path = args.out_file
    municipality_name: str = args.municipality_name
    output_value_col: str|None = args.output_value_col

    maru_version: MaruVersion = args.version
    maru_cols: MaruCol = get_maru_cols(maru_version)

    print(f'Opening input file "{args.in_file}"...')
    input_obj: InputObj = get_input_data_obj(
        in_file=in_file,
        municipality_name=municipality_name,
        maru_cols=maru_cols,
    )
    print(f'Obtaining output object for output file "{args.out_file}"...')
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
                maru_cols=maru_cols,
                output_value_col=output_value_col,
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
