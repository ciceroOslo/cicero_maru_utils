"""Labels for columns used in MarU report files and processed data
structures.
"""
import dataclasses
import typing as tp

from cicero_maru_utils.versions import (
    MaruVersion,
)



@dataclasses.dataclass(kw_only=True)
class MaruColOrig:
    year: str = 'year'
    year_month: str = 'year_month'
    gt_group: str = 'gt_group'
    vessel_type: str = 'vessel_type'
    phase: str = 'phase'
    voyage_type: str = 'voyage_type'
    ez_area_name: str = 'maritime_borders_norwegian_economic_zone_area_name'
    mgmt_plan_area_name: str = 'management_plan_marine_areas_area_name_norwegian'
    municipality_name: str = 'municipality_name'
    county_name: str = 'county_name'
    municipality_voyage_type: str = 'municipality_voyage_type'
    time_sec: str = 'sum_seconds'
    energy_kwh: str = 'sum_kwh'
    kwh_battery: str|None = None
    kwh_shore_power: str|None = None
    fuel: str = 'sum_fuel'
    co2: str = 'sum_co2'
    nmvoc: str = 'sum_nmvoc'
    co: str = 'sum_co'
    ch4: str = 'sum_ch4'
    n2o: str = 'sum_n2o'
    sox: str = 'sum_sox'
    pm10: str = 'sum_pm10'
    pm2_5: str = 'sum_pm2_5'
    nox: str = 'sum_nox'
    bc: str = 'sum_bc'
    co2e: str = 'sum_co2e'
    distance_km: str = 'distance_kilometers'
    version: str = 'version'
    timestamp: str = 'timestamp_utc_generated'

@dataclasses.dataclass
class MaruCol(MaruColOrig):
    municipality_number: str = 'municipality_number'
    month: str = 'month'

def get_maru_cols_original(version: MaruVersion) \
        -> MaruColOrig:
    """Get a dataclass of column names for the MarU data

    Parameters
    ----------
    version : MaruVersion
        Which version to get column names for. Current versions offered are
        '20241128' and '20250304'. For report files released on or after
        2025-03-04, use '20250304'.

    Returns
    -------
    MaruCol
        MaruCol dataclass with column names. This includes columns that are
        added or modified in processing the original Excel files into parquet
        format.
    """
    valid_versions: tuple[str, ...] = tuple(
        str(_member) for _member in MaruVersion
    )
    version = MaruVersion(version)
    if version == MaruVersion.V20241128:
        return MaruColOrig()
    elif version == MaruVersion.V20250304:
        return MaruColOrig(
            kwh_battery='sum_kwh_battery',
            kwh_shore_power='sum_kwh_shore_power',
        )
    else:
        raise ValueError(
            f'Invalid value passed in `version`. Valid values are: '
            f'{"\", \"".join(valid_versions)}'
        )

def get_maru_cols(version: MaruVersion) -> MaruCol:
    """Get a dataclass of column names fo the MarU data, including
    added/modified columns.

    This function does the same as `get_maru_cols_original`, but also includes
    columns that were added or modified during processing from Excel to parquet.
    See the docstring of `get_maru_cols_original` for more details. It returns
    a dataclass of type `MaruCol` instead of `MaruColOrig`.
    """
    orig_cols: MaruColOrig = get_maru_cols_original(version)
    return MaruCol(**dataclasses.asdict(orig_cols))
