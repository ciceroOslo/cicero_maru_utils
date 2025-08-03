"""Subpackage with runnable scripts (also importable as modules).

All modules in this subpackage can be run as command line scripts as such
(replace `xxx` with the script module name):
```
python -m cicero_maru_utils.scripts.xxx [options]
```
You can get an overview of options by replacing `[options]` with `--help`.
"""
from . import xlsx_to_parquet
