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

import polars as pl
import polars.selectors as cs
import xlsxwriter

from cicero_maru_utils.labels.columns import (
    MaruCol,
    get_maru_cols
)



def main() -> None:
    """
    Main function to orchestrate the file processing and conversion.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Create MarU variables for use in LOKFRAM from a parquet file of "
            "processed data from MarU Excel reports."
        ),
        epilog=(
            'The following output variable IDs are currently provided in the '
            'output file: "' + '", "'.join(output_var_specs.keys()) + '"'
        ),
    )
    parser.add_argument(
        "--in-file",
        type=Path,
        required=True,
        help="Path for the input .parquet file.",
    )
    parser.add_argument(
        "--out-file",
        type=Path,
        required=True,
        help=(
            "Path for the output file. Only excel output is supported at the "
            "moment."
        ),
    )


