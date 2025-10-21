import argparse
import time
from typing import Iterable, Tuple, List
import geopandas as gpd
import os
import pandas as pd
import pathlib
import shutil
import subprocess
import tempfile
import warnings
import pyarrow.lib
from gedidb.database.column_to_field import FIELD_TO_COLUMN
from gedidb.granule import granule_parser
from gedidb.database import gedidb_common
from gedidb.common import gedi_cmr_query as cmr, shape_parser
from gedidb.granule import granule_name
from gedidb.pipeline import spark_postgis
from gedidb import constants, environment
from gedidb.common import earthdata
from gedidb.constants import GediProduct
import hashlib


def hash_string_list(string_list: list) -> str:
    joined = ",".join([f"{len(item)}:{item}" for item in string_list])
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


PRODUCTS = [
    GediProduct.L2A,
    GediProduct.L2B,
    GediProduct.L4A,
]


def _get_granule_key_for_filename(filename: str) -> str:
    parsed = granule_name.parse_granule_filename(filename)
    return f"{parsed.orbit}_{parsed.sub_orbit_granule}"


def _get_saved_md_test():
    # TODO: This makes no sense -- the shape and version could have changed.
    # But, it's helpful for faster development.
    md_file = environment.GEDI_PATH / "metadata.pickle"
    if md_file.exists():
        print("Using existing metadata: ", md_file)
        import pickle

        with open(md_file, "rb") as f:
            return pickle.load(f)


def _get_granule_metadata(
    shape: gpd.GeoSeries, products: List[str]
) -> gpd.GeoDataFrame:
    # if _get_saved_md_test() is not None:
    #     return _get_saved_md_test()

    md_list = []
    for product in products:
        print("Querying NASA metadata API for product: ", product.value)
        df = cmr.query(product, spatial=shape)
        df.rename({"granule_name": "granule_file"}, axis=1, inplace=True)
        df["granule_key"] = df.granule_file.map(_get_granule_key_for_filename)
        df["product"] = product.value
        md_list.append(df)
    md = gpd.GeoDataFrame(
        pd.concat(md_list), geometry="granule_poly"
    ).reset_index()

    # Filter out granules with incomplete product sets
    # i.e. granules that do not have exactly one L2A, L2B, and L4A file
    nprod = (
        md[["product", "granule_key"]]
        .groupby("granule_key")["product"]
        .nunique()
    )
    omit = nprod[nprod != len(products)].index
    print(
        f"Omitting {len(omit)} (orbit, suborbit) pairs with incomplete product sets:"
    )
    print(omit)
    md = md[~md.granule_key.isin(omit)].reset_index()
    print("Total granules found: ", md.granule_key.nunique())
    print("Total files found: ", len(md.index) - 1)
    print("Total file size (MB): ", md["granule_size"].sum())
    return md


def _download_url(
    input: Tuple[str, str, str, str]
) -> Tuple[str, Tuple[GediProduct, pathlib.Path]]:
    granule_key, granule_file, url, product = input
    product = GediProduct(product)
    outfile_path = environment.gedi_product_path(product) / granule_file
    return_value = (granule_key, (product, outfile_path))
    if outfile_path.exists():
        return return_value

    os.makedirs(outfile_path.parent, exist_ok=True)
    try:
        temp = tempfile.NamedTemporaryFile(
            dir=environment.gedi_product_path(product),
        )
        subprocess.run(
            [
                "wget",
                "--load-cookies",
                environment.EARTH_DATA_COOKIE_FILE,
                "--save-cookies",
                environment.EARTH_DATA_COOKIE_FILE,
                "--auth-no-challenge=on",
                "--keep-session-cookies",
                "--content-disposition",
                "-O",
                temp.name,
                url,
            ],
            check=True,
        )
        # Sometimes the LP DAAC serves an empty L2A file even though the file exists.
        # This ... is very annoying. It does not produce a wget error,
        #           so we have to check for it manually.
        # Wait 5 seconds and try downloading again. If that doesn't work,
        # we have no choice but to raise.
        # For this reason, run spark with some failure tolerance
        # for large downloads so that the script doesn't crash all the time.
        if os.path.getsize(temp.name) == 0:
            time.sleep(5)
            subprocess.run(
                [
                    "wget",
                    "--load-cookies",
                    environment.EARTH_DATA_COOKIE_FILE,
                    "--save-cookies",
                    environment.EARTH_DATA_COOKIE_FILE,
                    "--auth-no-challenge=on",
                    "--keep-session-cookies",
                    "--content-disposition",
                    "-O",
                    temp.name,
                    url,
                ],
                check=True,
            )
            if os.path.getsize(temp.name) == 0:
                raise ValueError(f"Empty file: {url}")
        shutil.move(temp.name, outfile_path)
    finally:
        # Attempt to clean up the temp file. This will usually fail since
        # we've moved the file.
        try:
            temp.close()
        except:
            pass
    return return_value


def _process_granule(
    row: Tuple[str, Iterable[Tuple[GediProduct, pathlib.Path]]]
):
    granule_key, granules = row
    included_files = sorted([fname[1].name for fname in granules])
    outfile_path = (
        environment.GEDI_PATH
        / "Granules"
        / f"filtered_l2ab_l4a_{granule_key}_{hash_string_list(included_files)}.parquet"
    )
    return_value = (granule_key, outfile_path, included_files)
    if os.path.exists(outfile_path):
        return return_value

    gdfs = {}
    # 1. Parse each file and run per-product filtering.
    for product, file in granules:
        try:
            gdfs[product] = (
                granule_parser.parse_file(product, file, quality_filter=True)
                .rename(lambda x: f"{x}_{product.value}", axis=1)
                .rename({f"shot_number_{product.value}": "shot_number"}, axis=1)
            )
            if gdfs[product].empty:
                # Write an empty file as a placeholder
                # TODO(amelia): Can we do something better here?
                # we could at least use the columns in the schema
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    gdfs[product].to_parquet(outfile_path)
                return return_value

        except:
            # TODO: Better error recovery for failed granules.
            with open(
                "/home/ah2174/gedi-database/logs/failed_granules.txt",
                "a+",
            ) as f:
                f.write(f"{granule_key}\n")
                f.write(f"{file}\n")
                f.write(f"{included_files}\n")
                raise

    # 2. Join all products on shot_number.
    # Shots must be present in ALL THREE products to be included.
    # Might also consider allowing shots that are missing L4A data,
    # but for now we do not do this.
    # Additional filtering (not done here):
    #  - L4A quality_flag == 1
    #  - L4A algorithm_run_flag == 1
    #  - L4A predictor_limit_flag == 0
    #  - L4A response_limit_flag == 0
    #  - L2B stale_return_flag == 0
    #  - UMD outliers: need EASE72 grid info

    gdf = (
        gdfs[GediProduct.L2A]
        .join(
            gdfs[GediProduct.L2B].set_index("shot_number"),
            on="shot_number",
            how="inner",
        )
        .join(
            gdfs[GediProduct.L4A].set_index("shot_number"),
            on="shot_number",
            how="inner",
        )
        .drop(["geometry_level2B", "geometry_level4A"], axis=1)
        .set_geometry("geometry_level2A")
        .rename_geometry("geometry")
    )

    gdf["granule"] = granule_key

    # We could drop redundant columns here ...
    # or we can do that on import by selecting only the columns
    # in the PostGIS schema.

    # 4. Write to parquet
    os.makedirs(outfile_path.parent, exist_ok=True)
    gdf.to_parquet(
        outfile_path, allow_truncated_timestamps=True, coerce_timestamps="us"
    )
    return return_value


def _write_db(input):
    granule_key, outfile_path, included_files = input

    # Write all shots in the granule dataframe in a transaction while inserting
    # the granule name into the granule table.
    # If the transaction fails, we then know which granules need to be re-inserted
    # and can re-insert the entire granule at once without checking whether
    # individual shots are already in the database.
    # This is important because it allows us to drop indexes and key constraints
    # on the table while inserting, which increases performance considerably.
    try:
        gedi_data = gpd.read_parquet(outfile_path)
    except pyarrow.lib.ArrowInvalid as e:
        print(f"WARNING: Corrupted file {outfile_path}")
        print(e)
        os.remove(outfile_path)
        return

    if gedi_data.empty:
        return
    gedi_data = gedi_data[list(FIELD_TO_COLUMN.keys())]
    gedi_data = gedi_data.rename(columns=FIELD_TO_COLUMN)
    gedi_data = gedi_data.astype({"shot_number": "int64"})
    print(f"Writing granule: {granule_key}")

    with gedidb_common.get_engine().begin() as conn:
        granule_entry = pd.DataFrame(
            data={
                "granule_name": [granule_key],
                "granule_hash": [hash_string_list(included_files)],
                "granule_file": [outfile_path.name],
                "l2a_file": [included_files[0]],
                "l2b_file": [included_files[1]],
                "l4a_file": [included_files[2]],
                "created_date": [pd.Timestamp.utcnow()],
            }
        )
        granule_entry.to_sql(
            name="gedi_granules",
            con=conn,
            index=False,
            if_exists="append",
        )

        gedi_data.to_postgis(
            name="filtered_l2ab_l4a_shots",
            con=conn,
            index=False,
            if_exists="append",
        )
        conn.commit()
        del gedi_data
    return granule_entry


def exec_spark(
    shape: gpd.GeoSeries,
    confirm: bool = True,
    download_only: bool = False,
    dry_run: bool = False,
):
    earthdata.authenticate()
    # 1. Construct table of file metadata from CMR API
    required_granules = _get_granule_metadata(shape, PRODUCTS)
    with gedidb_common.get_engine().connect() as conn:
        # TODO(amelia): Also deal with the hash correctly
        existing_granules = pd.read_sql_query(
            "SELECT granule_name FROM gedi_granules", conn
        )
        required_granules = required_granules[
            ~required_granules.granule_key.isin(existing_granules.granule_name)
        ]

    if dry_run:
        return
    if confirm:
        input("To proceed, press ENTER >>> ")

    ## SPARK STARTS HERE ##
    # 2. Download granule files to shared location
    spark = spark_postgis.get_spark()
    name_url = required_granules[
        [
            "granule_key",
            "granule_file",
            "granule_url",
            "product",
        ]
    ].to_records(index=False)
    # TODO(amelia):
    # Partition by granule because the L2A files are huge compared to the other
    # products. Try to evenly distribute these files across workers.
    urls = spark.sparkContext.parallelize(name_url)
    files_by_granule = urls.map(_download_url).groupByKey()

    # 3. Parse HDF5 files into per-granule filtered parquet files
    # It would be nice if we could use Sedona for this,
    # but the python API does not yet support HDF5 files.
    # https://hdfeos.org/examples/sedona.php
    processed_granules = files_by_granule.map(_process_granule)

    # # 4. Ingest granule parquet files into PostGIS database
    gedidb_common.maybe_create_tables()
    # TODO granule_poly
    # limit to 8 concurrent connections to avoid overwhelming the DB
    # this number was chosen somewhat arbitrarily
    out = processed_granules.coalesce(8).map(_write_db)
    out.count()

    spark.stop()
    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download, filter, and ingest L2A, L2B, and L4A GEDI data"
    )
    parser.add_argument(
        "--shapefile",
        help="Shapefile (zip) containing the world region to download.",
        type=str,
    )
    parser.add_argument(
        "--confirm",
        help="Ask for confirmation of download plan before starting.",
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(confirm=True)
    parser.add_argument(
        "--dry_run",
        help=("Dry run only: save all found granules to temporary file."),
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "--download_only",
        help=(
            "Only download the raw granule files to shared location."
            "Do not also filter and ingest the data into PostGIS database."
        ),
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(download_only=False)
    args = parser.parse_args()

    shapefile = pathlib.Path(args.shapefile)
    if not shapefile.exists():
        print("Unable to locate file {}".format(shapefile))
        exit(1)
    shp = gpd.read_file(shapefile)
    try:
        try:
            shp = shape_parser.check_and_format_shape(shp)
        except shape_parser.DetailError as exc:
            input(
                (
                    "The NASA API can only accept up to 5000 vertices in a single shape,\n"
                    "but the shape you supplied has {} vertices.\n"
                    "If you would like to automatically simplify this shape to its\n"
                    "convex hull, press ENTER, otherwise Ctrl-C to quit."
                ).format(exc.n_coords)
            )
            shp = shape_parser.check_and_format_shape(shp, simplify=True)
    except ValueError:
        print("This script only accepts one (multi)polygon at a time.")
        print("Please split up each row of your shapefile into its own file.")
        exit(1)

    exec_spark(
        shp,
        confirm=args.confirm,
        download_only=args.download_only,
        dry_run=args.dry_run,
    )
