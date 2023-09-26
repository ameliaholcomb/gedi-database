import argparse
import geopandas as gpd
import os
import pandas as pd
import pathlib
import pyspark.sql.types as T
from pyspark.sql.functions import collect_list
import shutil
import sqlalchemy
import subprocess
import tempfile

from gedidb.database import gedidb_loader
from gedidb.granule import gedi_cmr_query as cmr
from gedidb.granule import granule_name
from gedidb.pipeline import shape_parser, spark_postgis
from gedidb import constants, environment

PRODUCTS = [
    constants.GediProduct.L2A,
    constants.GediProduct.L2B,
    constants.GediProduct.L4A,
]


def _get_engine():
    # Since spark runs workers in their own process, we cannot share database
    # connections between workers. So we create a new connection for each query.
    # This is reasonable because most of our queries involve
    # inserting a large amount of data into the database.
    return sqlalchemy.create_engine(environment.DB_CONFIG, echo=False)


def _fetch_cookies():
    print("No authentication cookies found, fetching earthdata cookies ...")
    netrc_file = environment.USER_PATH / ".netrc"
    add_login = True
    if netrc_file.exists():
        with open(netrc_file, "r") as f:
            if "urs.earthdata.nasa.gov" in f.read():
                add_login = False

    if add_login:
        with open(environment.USER_PATH / ".netrc", "a+") as f:
            f.write(
                "\nmachine urs.earthdata.nasa.gov login {} password {}".format(
                    environment.EARTHDATA_USER, environment.EARTHDATA_PASSWORD
                )
            )
            os.fchmod(f.fileno(), 0o600)

    environment.EARTH_DATA_COOKIE_FILE.touch()
    subprocess.run(
        [
            "wget",
            "--load-cookies",
            constants.EARTH_DATA_COOKIE_FILE,
            "--save-cookies",
            constants.EARTH_DATA_COOKIE_FILE,
            "--keep-session-cookies",
            "https://urs.earthdata.nasa.gov",
        ],
        check=True,
    )


def _get_orbit_metadata(shape: gpd.GeoSeries) -> gpd.GeoDataFrame:
    metadata_list = []
    for product in PRODUCTS:
        df = cmr.query(product, spatial=shape)
        # Note that orbits are NOT unique across product files.
        # {orbit}_{ground_track}_{granule} is a unique file.
        # However, these keys do not match across products.
        df["orbit"] = df.granule_name.map(
            granule_name.parse_granule_filename.orbit
        )
        df["product"] = product.value
        metadata_list.append(df)
    metadata = gpd.GeoDataFrame(pd.concat(metadata_list))

    # Filter out orbits with incomplete product sets
    # i.e. orbits that do not have at least one L2A, L2B, and L4A file
    nprod = metadata[["product", "orbit"]].groupby("orbit")["product"].nunique()
    omit = nprod[nprod < len(PRODUCTS)].index
    print(f"Omitting {len(omit)} orbits with incomplete product sets:")
    print(omit)
    return metadata[~metadata.orbit.isin(omit)]


def _query_downloaded():
    return pd.read_sql_table(
        table_name="orbit_table", columns=["granule_name"], con=_get_engine()
    )


def _download_url(inp):
    name, url, product, orbit = inp
    outfile_path = environment.gedi_product_path(product) / name
    if os.path.exists(outfile_path):
        return outfile_path
    with tempfile.NamedTemporaryFile(
        dir=environment.gedi_product_path(product)
    ) as temp:
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
        shutil.move(temp.name, outfile_path)
    return outfile_path, product, orbit


def _process_orbit(input):
    # Row(orbit='x', collect_list(files)=['y', 'y', 'y'])
    # 1. Parse each file and do basic filtering, then close the file.
    # TODO: where would we find out that files didn't download?
    # Are undownloaded files still going to be in the list?
    # 2. Join all products on shot_number.
    # 3. Perform better filtering with shared data
    # 4. Drop unneeded columns
    # 5. Write to parquet
    # 6. Return path to parquet file
    return


def exec_spark(
    shape: gpd.GeoSeries,
    download_only: bool = False,
    dry_run: bool = False,
):
    if not os.path.exists(environment.EARTH_DATA_COOKIE_FILE):
        _fetch_cookies()
    # 1. Construct table of file metadata from CMR API
    metadata = _get_orbit_metadata(shape)
    print("Total files found: ", len(metadata.index) - 1)
    print("Total file size (MB): ", metadata["granule_size"].sum())

    if download_only:
        # TODO: Check the list of filtered orbit files
        #      against the orbit metadata table
        required_orbits = metadata
    else:
        stored_orbits = _query_downloaded()
        required_orbits = metadata.loc[
            ~metadata["orbit"].isin(stored_orbits["orbit"])
        ]

    if required_orbits.empty:
        if download_only:
            print("All orbits for this region already downloaded")
        else:
            print("All granules for this region already in the database")
        return

    if dry_run:
        return
    if download_only:
        input("To proceed to download this data, press ENTER >>> ")
    else:
        input("To proceed to download AND INGEST this data, press ENTER >>> ")
    for path in [environment.gedi_product_path(p) for p in PRODUCTS]:
        if not os.path.exists(path):
            print(
                "Creating directory {}".format(
                    environment.gedi_product_path(path)
                )
            )
            os.mkdir(path)

    ## SPARK STARTS HERE ##
    # 2. Download granule files to shared location
    #    >> Ensure success for all granules in metadata table
    #    >> Warn about and omit granules if any product failed to download
    #    >> Later improvement: Okay if L2 exists even if L4 doesn't?
    #    >> Missed granules will be picked up in the next run.
    spark = spark_postgis.get_spark()
    name_url = required_orbits[
        ["granule_name", "granule_url", "product", "orbit"]
    ].to_records(index=False)
    urls = spark.sparkContext.parallelize(name_url)
    out_schema = T.StructType(
        [
            T.StructField("file", T.StringType(), True),
            T.StructField("product", T.StringType(), True),
            T.StructField("orbit", T.StringType(), True),
        ]
    )
    files = spark.createDataFrame(urls.map(_download_url), out_schema)

    if download_only:
        files.count()
    else:
        # 3. Parse HDF5 files into per-granule filtered parquet files
        orbit_files = files.groupby("orbit").agg(collect_list("file"))
        # Ideally, some of this could be done with sedona directly,
        # but the python API does not yet support HDF5 files.
        # https://hdfeos.org/examples/sedona.php
        processed_orbits = orbit_files.rdd.map(_process_orbit)

        # 4. Ingest granule parquet files into PostGIS database
        # out = processed_orbits.coalesce(10).map(_write_db)
        # out.count()

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
                    "bounding box, press ENTER, otherwise Ctrl-C to quit."
                ).format(exc.n_coords)
            )
            shp = shape_parser.check_and_format_shape(shp, simplify=True)
    except ValueError:
        print("This script only accepts one (multi)polygon at a time.")
        print("Please split up each row of your shapefile into its own file.")
        exit(1)

    exec_spark(
        [shp],
        download_only=args.download_only,
        dry_run=args.dry_run,
    )
