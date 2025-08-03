#!/usr/bin/env python3
"""
A command-line utility to find all Excel (.xlsx) files in a directory,
consolidate them, and convert them into a single Apache Parquet file.
"""

import argparse
import dataclasses
from pathlib import Path
import typing as tp

import polars as pl
import polars.selectors as cs

from cicero_maru_utils.labels.columns import (
    MaruCol,
    MaruColOrig,
    get_maru_cols,
    get_maru_cols_original,
)
from cicero_maru_utils.versions import MaruVersion



def get_source_schema(version: MaruVersion) \
        -> pl.Schema:
    """
    Defines and returns the expected Polars schema for the source Excel files.

    The schema uses Enum types for categorical data to ensure consistency and
    optimize memory usage. The categories have been predefined based on sample
    data.

    Returns:
        pl.Schema: The Polars schema for the source data.
    """
    cols: MaruColOrig = get_maru_cols_original(version)
    schema_base: pl.Schema = pl.Schema(
        {
            MaruColOrig.gt_group: pl.Enum(
                ['Unknown', 'gt1, 0-399', 'gt2, 400-999', 'gt3, 1000-2999',
                 'gt4, 3000-4999', 'gt5, 5000-9999', 'gt6, 10000-24999',
                 'gt7, 25000-49999', 'gt8, 50000-99999', 'gt9, >=100 000']
            ),
            MaruColOrig.vessel_type: pl.Enum(
                ['Andre aktiviteter', 'Andre offshorefartøy', 'Andre servicefartøy',
                 'Container/RoRo', 'Cruise', 'Fiskefartøy', 'Gasstankskip',
                 'Havbruk', 'Kjemikalie-/produkttanker', 'Offshore', 'Oljetanker',
                 'Passasjer', 'Stykkgods', 'Tørrbulk']
            ),
            MaruColOrig.phase: pl.Enum(
                ['Anchor', 'Aquacultur', 'Cruise', 'Dynamic positioning offshore',
                 'Electric shore power', 'Maneuver', 'Node (berth)']
            ),
            MaruColOrig.voyage_type: pl.Enum(
                ['Berthed', 'Domestic', 'International in', 'International out',
                 'Transitt']
            ),
            MaruColOrig.ez_area_name: pl.Enum(
                ['Fiskerisonen ved Jan Mayen', 'Fiskevernsonen ved Svalbard',
                 'Norges økonomiske sone', 'Territorialområde']
            ),
            MaruColOrig.mgmt_plan_area_name: pl.Enum(
                ['Barentshavet-Lofoten', 'Kystområde Barentshavet',
                 'Kystområde Nordsjøen', 'Kystområde Norskehavet', 'Nordsjøen-Skagerrak',
                 'Norskehavet']
            ),
            MaruColOrig.municipality_name: pl.Enum(
                [
                    'Alstahaug (1820)', 'Alta (5601)', 'Alver (4631)', 'Andøy (1871)',
                    'Arendal (4203)', 'Asker (3203)', 'Askvoll (4645)', 'Askøy (4627)',
                    'Aukra (1547)', 'Aure (1576)', 'Aurland (4641)', 'Aurskog-Høland (3226)',
                    'Austevoll (4625)', 'Austrheim (4632)', 'Averøy (1554)',
                    'Balsfjord (5532)', 'Bamble (4012)', 'Bardu (5520)', 'Beiarn (1839)',
                    'Bergen (4601)', 'Berlevåg (5630)', 'Bindal (1811)', 'Birkenes (4216)',
                    'Bjerkreim (1114)', 'Bjørnafjorden (4624)', 'Bodø (1804)',
                    'Bokn (1145)', 'Bremanger (4648)', 'Brønnøy (1813)', 'Bygland (4220)',
                    'Bykle (4222)', 'Båtsfjord (5632)', 'Bærum (3201)', 'Bø (1867)',
                    'Bømlo (4613)', 'Dovre (3431)', 'Drammen (3301)', 'Drangedal (4016)',
                    'Dyrøy (5528)', 'Dønna (1827)', 'Eidfjord (4619)', 'Eidskog (3416)',
                    'Eidsvoll (3240)', 'Eigersund (1101)', 'Enebakk (3220)',
                    'Engerdal (3425)', 'Etne (4611)', 'Etnedal (3450)', 'Evenes (1853)',
                    'Evje og Hornnes (4219)', 'Farsund (4206)', 'Fauske (1841)',
                    'Fedje (4633)', 'Fitjar (4615)', 'Fjaler (4646)', 'Fjord (1578)',
                    'Flakstad (1859)', 'Flatanger (5049)', 'Flekkefjord (4207)',
                    'Flå (3320)', 'Folldal (3429)', 'Fredrikstad (3107)', 'Frogn (3214)',
                    'Froland (4214)', 'Frosta (5036)', 'Frøya (5014)', 'Færder (3911)',
                    'Gamvik (5626)', 'Gildeskål (1838)', 'Giske (1532)', 'Gjemnes (1557)',
                    'Gjerdrum (3230)', 'Gjerstad (4211)', 'Gjesdal (1122)', 'Gjøvik (3407)',
                    'Gloppen (4650)', 'Gol (3324)', 'Gran (3446)', 'Grane (1825)',
                    'Gratangen (5516)', 'Grimstad (4202)', 'Gulen (4635)', 'Hadsel (1866)',
                    'Halden (3101)', 'Hamar (3403)', 'Hamarøy (1875)', 'Hammerfest (5603)',
                    'Haram (1580)', 'Hareid (1517)', 'Harstad (5503)', 'Hasvik (5616)',
                    'Hattfjelldal (1826)', 'Haugesund (1106)', 'Heim (5055)',
                    'Hemnes (1832)', 'Herøy (1515)', 'Herøy (1818)', 'Hitra (5056)',
                    'Hjelmeland (1133)', 'Hol (3330)', 'Hole (3310)', 'Holmestrand (3903)',
                    'Horten (3901)', 'Hurdal (3242)', 'Hustadvika (1579)', 'Hvaler (3110)',
                    'Hyllestad (4637)', 'Hå (1119)', 'Høyanger (4638)', 'Høylandet (5046)',
                    'Ibestad (5514)', 'Inderøy (5053)', 'Indre Fosen (5054)',
                    'Indre Østfold (3118)', 'Iveland (4218)', 'Jevnaker (3236)',
                    'Karasjok (5610)', 'Karlsøy (5534)', 'Karmøy (1149)',
                    'Kautokeino (5612)', 'Kinn (4602)', 'Klepp (1120)',
                    'Kongsvinger (3401)', 'Kragerø (4014)', 'Kristiansand (4204)',
                    'Kristiansund (1505)', 'Krødsherad (3318)', 'Kvam (4622)',
                    'Kvinesdal (4227)', 'Kvinnherad (4617)', 'Kviteseid (4028)',
                    'Kvitsøy (1144)', 'Kvæfjord (5510)', 'Kvænangen (5546)',
                    'Kåfjord (5540)', 'Larvik (3909)', 'Lavangen (5518)', 'Lebesby (5624)',
                    'Leirfjord (1822)', 'Leka (5052)', 'Levanger (5037)', 'Lier (3312)',
                    'Lierne (5042)', 'Lillehammer (3405)', 'Lillesand (4215)',
                    'Lillestrøm (3205)', 'Lindesnes (4205)', 'Lom (3434)', 'Loppa (5614)',
                    'Lund (1112)', 'Lunner (3234)', 'Lurøy (1834)', 'Luster (4644)',
                    'Lyngdal (4225)', 'Lyngen (5536)', 'Lærdal (4642)', 'Lødingen (1851)',
                    'Malvik (5031)', 'Masfjorden (4634)', 'Melhus (5028)', 'Meløy (1837)',
                    'Midt-Telemark (4020)', 'Midtre Gauldal (5027)', 'Modalen (4629)',
                    'Modum (3316)', 'Molde (1506)', 'Moskenes (1874)', 'Moss (3103)',
                    'Målselv (5524)', 'Måsøy (5618)', 'Namsos (5007)', 'Namsskogan (5044)',
                    'Narvik (1806)', 'Nes (3228)', 'Nesna (1828)', 'Nesodden (3212)',
                    'Nesseby (5636)', 'Nittedal (3232)', 'Nome (4018)', 'Nord-Odal (3414)',
                    'Nordkapp (5620)', 'Nordre Follo (3207)', 'Nordre Land (3448)',
                    'Nordreisa (5544)', 'Nore og Uvdal (3338)', 'Notodden (4005)',
                    'Nærøysund (5060)', 'Oppdal (5021)', 'Orkland (5059)', 'Osen (5020)',
                    'Oslo (0301)', 'Osterøy (4630)', 'Overhalla (5047)',
                    'Porsanger (5622)', 'Porsgrunn (4001)', 'Rakkestad (3120)',
                    'Rana (1833)', 'Randaberg (1127)', 'Rauma (1539)', 'Rendalen (3424)',
                    'Rindal (5061)', 'Ringerike (3305)', 'Ringsaker (3411)',
                    'Risør (4201)', 'Rollag (3336)', 'Røyrvik (5043)', 'Råde (3112)', 'Rødøy (1836)',
                    'Røst (1856)', 'Salangen (5522)', 'Saltdal (1840)', 'Samnanger (4623)',
                    'Sande (1514)', 'Sandefjord (3907)', 'Sandnes (1108)',
                    'Sarpsborg (3105)', 'Sauda (1135)', 'Sel (3437)', 'Selbu (5032)',
                    'Senja (5530)', 'Sigdal (3332)', 'Siljan (4010)', 'Sirdal (4228)',
                    'Skaun (5029)', 'Skien (4003)', 'Skjervøy (5542)', 'Skjåk (3433)',
                    'Smøla (1573)', 'Snåsa (5041)', 'Sogndal (4640)', 'Sokndal (1111)',
                    'Sola (1124)', 'Solund (4636)', 'Sortland (1870)', 'Stad (4649)',
                    'Stange (3413)', 'Stavanger (1103)', 'Steigen (1848)',
                    'Steinkjer (5006)', 'Stjørdal (5035)', 'Stord (4614)',
                    'Storfjord (5538)', 'Strand (1130)', 'Stranda (1525)', 'Stryn (4651)',
                    'Sula (1531)', 'Suldal (1134)', 'Sunndal (1563)', 'Sunnfjord (4647)',
                    'Surnadal (1566)', 'Svalbard', 'Sveio (4612)', 'Sykkylven (1528)',
                    'Sømna (1812)', 'Søndre Land (3447)', 'Sør-Aurdal (3449)',
                    'Sør-Odal (3415)', 'Sør-Varanger (5605)', 'Sørfold (1845)',
                    'Sørreisa (5526)', 'Tana (5628)', 'Time (1121)', 'Tingvoll (1560)', 'Tinn (4026)',
                    'Tjeldsund (5512)', 'Tokke (4034)', 'Tolga (3426)', 'Tromsø (5501)',
                    'Trondheim (5001)', 'Træna (1835)', 'Tvedestrand (4213)',
                    'Tynset (3427)', 'Tysnes (4616)', 'Tysvær (1146)', 'Tønsberg (3905)',
                    'Ullensaker (3209)', 'Ullensvang (4618)', 'Ulstein (1516)',
                    'Ulvik (4620)', 'Utsira (1151)', 'Vadsø (5607)', 'Vaksdal (4628)',
                    'Valle (4221)', 'Vanylven (1511)', 'Vardø (5634)', 'Vefsn (1824)',
                    'Vega (1815)', 'Vegårshei (4212)', 'Vennesla (4223)', 'Verdal (5038)',
                    'Vestby (3216)', 'Vestnes (1535)', 'Vestre Slidre (3452)',
                    'Vestvågøy (1860)', 'Vevelstad (1816)', 'Vik (4639)',
                    'Vindafjord (1160)', 'Vinje (4036)', 'Volda (1577)', 'Voss (4621)',
                    'Vågan (1865)', 'Våler (3419)', 'Værøy (1857)', 'Åfjord (5058)',
                    'Ålesund (1508)', 'Åmli (4217)', 'Åmot (3422)', 'Årdal (4643)',
                    'Ås (3218)', 'Åseral (4224)', 'Åsnes (3418)', 'Øksnes (1868)',
                    'Ørland (5057)', 'Ørsta (1520)', 'Østre Toten (3442)',
                    'Øvre Eiker (3314)', 'Øygarden (4626)',
                ] + [  # Added later
                    'Elverum (3420)', 'Flesberg (3334)', 'Fyresdal (4032)',
                    'Hemsedal (3326)', 'Hjartdal (4024)', 'Hægebostad (4226)',
                    'Kongsberg (3303)', 'Lesja (3432)', 'Løten (3412)',
                    'Marker (3122)', 'Meråker (5034)', 'Nannestad (3238)',
                    'Nesbyen (3322)', 'Nissedal (4030)', 'Nord-Aurdal (3451)',
                    'Nord-Fron (3436)', 'Os (3430)', 'Rennebu (5022)',
                    'Ringebu (3439)', 'Røros (5025)', 'Seljord (4022)',
                    'Stor-Elvdal (3423)', 'Trysil (3421)', 'Tydal (5033)',
                    'Vang (3454)', 'Vågå (3435)', 'Ål (3328)',
                    'Øystre Slidre (3453)',
                ]
            ),
            MaruColOrig.county_name: pl.Enum(
                ['Agder', 'Akershus', 'Buskerud', 'Finnmark', 'Innlandet',
                 'Møre og Romsdal', 'Nordland', 'Oslo', 'Rogaland', 'Svalbard',
                 'Telemark', 'Troms', 'Trøndelag', 'Vestfold', 'Vestland', 'Østfold']
            ),
            MaruColOrig.municipality_voyage_type: pl.Enum(
                ['Berthed', 'Departure or destination', 'Local', 'Transit']
            ),
            MaruColOrig.version: pl.Enum(
                ['v1.5.0']
            ),
            MaruColOrig.timestamp: pl.Datetime(time_unit='ms', time_zone='UTC'),
            MaruColOrig.year: pl.Int16(),
            MaruColOrig.year_month: pl.String(),
            MaruColOrig.energy_kwh: pl.Float64(),
            MaruColOrig.fuel: pl.Float64(),
            MaruColOrig.co2: pl.Float64(),
            MaruColOrig.nmvoc: pl.Float64(),
            MaruColOrig.co: pl.Float64(),
            MaruColOrig.ch4: pl.Float64(),
            MaruColOrig.n2o: pl.Float64(),
            MaruColOrig.sox: pl.Float64(),
            MaruColOrig.pm10: pl.Float64(),
            MaruColOrig.pm2_5: pl.Float64(),
            MaruColOrig.nox: pl.Float64(),
            MaruColOrig.bc: pl.Float64(),
            MaruColOrig.co2e: pl.Float64(),
            MaruColOrig.distance_km: pl.Float64(),
        },
    )
    if version == '20241128':
        schema: pl.Schema = schema_base
    else:
        schema = schema_base | pl.Schema(
            {
                _col: pl.Float64()
                for _col in (
                    MaruCol.kwh_battery, MaruCol.kwh_shore_power
                ) if _col is not None
            } | {
                MaruColOrig.version: pl.Enum(['v1.5.0', 'v1.6.0', 'v1.7.0']),
                MaruColOrig.phase: pl.Enum(
                    schema_base[MaruColOrig.phase].categories.to_list() \
                        + ['Fishing']
                ),
                MaruColOrig.municipality_name: pl.Enum(
                    # ###
                    # Adding Våler with new municipality number instead of
                    # replacing the old value. It appears that it is still
                    # present with the old number in some rows in some of the
                    # newer files as well.
                    # ###
                    # schema_base[MaruColOrig.municipality_name].categories.replace(
                    #     {
                    #         'Våler (3419)': 'Våler (3114)',
                    #     }
                    # )
                    schema_base[MaruColOrig.municipality_name].categories.to_list() \
                        + ['Våler (3114)']
                ),
                MaruColOrig.voyage_type: pl.Enum(
                    schema_base[MaruColOrig.voyage_type].categories.to_list() \
                        + ['NCS_Facility_Proximate']
                )
            },
        )
    return schema


def process_municipality_data(schema: pl.Schema) \
        -> tuple[pl.DataFrame, pl.Schema]:
    """
    Processes the municipality names from the schema.

    It extracts the municipality name and number from the combined string
    (e.g., "Oslo (0301)") into separate columns. This is used to create new
    Enum types for casting the columns after reading the Excel files.

    Args:
        schema: The source Polars schema containing the municipality names.

    Returns:
        A DataFrame with processed 'municipality_name' and 'municipality_number'
        columns, and a Schema for the new columns.
    """
    # Regex to extract name and number. The number part is optional to handle
    # 'Svalbard', which has no number.
    municipality_regex = r'^(?P<municipality_name>[^(]*)(?: \((?P<municipality_number>\d{4})\))?$'

    # The schema object itself doesn't hold the categories directly in a way
    # that can be turned into a DataFrame. We access them through the dtype.
    municipality_enum_type = schema['municipality_name']
    if not isinstance(municipality_enum_type, pl.Enum):
         raise TypeError("Schema's 'municipality_name' is not an Enum type.")
    municipality_df: pl.DataFrame \
        = pl.DataFrame({'municipality_name': municipality_enum_type.categories})\
        .select(
            pl.col('municipality_name').str.extract_groups(
                municipality_regex
            ).struct.unnest()
        ).with_columns(
            # Disambiguate the two 'Herøy' municipalities by including their number
            pl.when(pl.col('municipality_name') == 'Herøy')
            .then('Herøy (' + pl.col('municipality_number') + ')')
            .otherwise(pl.col('municipality_name'))
            .alias('municipality_name')
        ).with_columns(
            # Fill null for Svalbard's number to avoid issues with Enum creation
            pl.col('municipality_number').fill_null('')
        )
    municipality_schema: pl.Schema = pl.Schema(
        {
            'municipality_name': \
                pl.Enum(
                    municipality_df['municipality_name']\
                        .unique(maintain_order=True)
                ),
            'municipality_number': \
                pl.Enum(
                    municipality_df['municipality_number']\
                        .unique(maintain_order=True)
                ),
        }
    )
    return municipality_df, municipality_schema


def process_excel_file(
    file_path: Path,
    base_schema: pl.Schema,
    municipality_info_schema: pl.Schema,
) -> pl.DataFrame:
    """
    Reads a single Excel file, applies transformations, and returns a DataFrame.

    Args:
        file_path: The path to the Excel file.
        base_schema: The base schema for reading the file.
        municipality_info_df: DataFrame with processed municipality info for casting.

    Returns:
        A processed Polars DataFrame from the Excel file.
    """
    print(f"Processing file: {file_path.name}...")
    # Regex to extract municipality name and number from the data rows.
    municipality_regex = r'^(?P<municipality_name>[^(]*)(?: \((?P<municipality_number>\d{4})\))?$'

    # Define the final schema overrides for reading the excel file
    schema_overrides = pl.Schema({'year': pl.Int16(), **base_schema})

    lazy_df = pl.read_excel(
        file_path,
        schema_overrides=schema_overrides
    ).lazy()

    return (
        lazy_df
        .with_columns(
            pl.col('year_month').str.slice(-2).cast(pl.Int8())
        )
        .rename({'year_month': 'month'})
        .with_columns(
            pl.col('municipality_name').cast(str).str.extract_groups(
                municipality_regex
            ).struct.unnest()
        )
        .with_columns(
            # Disambiguate 'Herøy'
            pl.when(pl.col('municipality_name') == 'Herøy')
            .then('Herøy (' + pl.col('municipality_number') + ')')
            .otherwise(pl.col('municipality_name'))
            .alias('municipality_name')
        )
        .cast(municipality_info_schema)
        .collect()
    )


def main() -> None:
    """
    Main function to orchestrate the file processing and conversion.
    """
    parser = argparse.ArgumentParser(
        description="Consolidate MarU XLSX reports into a single Parquet file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--search-dir",
        type=Path,
        required=True,
        help="Directory to search recursively for .xlsx files.",
    )
    parser.add_argument(
        "--out-file",
        type=Path,
        required=True,
        help="Path for the output .parquet file.",
    )
    parser.add_argument(
        "--version",
        type=str,
        required=True,
        choices=[str(_member) for _member in MaruVersion],
        help="MarU report release version of the data.",
    )
    args = parser.parse_args()

    search_dir: Path = args.search_dir
    out_file: Path = args.out_file
    version: MaruVersion = MaruVersion(args.version)

    # --- Pre-flight checks ---
    if not search_dir.is_dir():
        print(f"Error: The search directory '{search_dir}' does not exist.")
        return

    if out_file.exists():
        print(
            f"Error: The output file '{out_file}' already exists. "
            "Please remove or move it and try again."
        )
        return

    # --- Find Files ---
    print(f"Searching for .xlsx files in '{search_dir}'...")
    data_files = list(search_dir.glob('**/*.xlsx'))

    if not data_files:
        print("No .xlsx files found in the specified directory.")
        return

    print(f"Found {len(data_files)} files to process.")

    # --- Processing ---
    try:
        base_schema = get_source_schema(version=version)
        municipality_info_df: pl.DataFrame
        municipality_info_schema: pl.Schema
        municipality_info_df, municipality_info_schema = \
            process_municipality_data(base_schema)

        all_dfs: list[pl.DataFrame] = [
            process_excel_file(file, base_schema, municipality_info_schema)
            for file in data_files
        ]

        print("\nConcatenating all DataFrames...")
        maru_df = pl.concat(all_dfs, how='vertical')

        print(f"Writing consolidated data to '{out_file}'...")
        maru_df.write_parquet(out_file)

        print("\nSuccessfully created the Parquet file!")
        print(f"Final DataFrame shape: {maru_df.shape}")

    except Exception as e:
        print(f"\nAn error occurred during processing: {e}")
        import traceback
        traceback.print_exc()


# --- Utility function for schema maintenance ---
def str_cols_to_enum_schema(df: pl.DataFrame) -> pl.Schema:
    """
    Generates a Polars Enum schema from string/categorical columns.

    This is a helper function for maintenance. If the source data changes,
    you can load a sample file with string types, then run this function on
    the resulting DataFrame to get an updated Enum schema definition.

    Args:
        df: A DataFrame with columns to be converted to Enums.

    Returns:
        A Polars Schema with Enum definitions.
    """
    str_columns = df.select(cs.string(), cs.categorical()).columns
    return pl.Schema({
        col_name: pl.Enum(df[col_name].unique().sort())
        for col_name in str_columns
    })


if __name__ == "__main__":
    main()
