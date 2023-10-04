import unittest
import unittest.mock
import pathlib
import geopandas as gpd
from sqlalchemy import text
import pandas as pd

from gedidb.pipeline.data_setup import _write_db
from gedidb.database import gedidb_common

GRANULE_FNAME = pathlib.Path(
    "./data/filtered_l2ab_l4a_O02806_01_52534f927d60fc7dca2accaca7357d91.parquet"
)
# these need to really match the hash in the granule filename
GRANULE_L2A_FILE = "GEDI02_A_2019162130932_O02806_01_T04237_02_003_01_V002.h5"
GRANULE_L2B_FILE = "GEDI02_B_2019162130932_O02806_01_T04237_02_003_01_V002.h5"
GRANULE_L4A_FILE = "GEDI04_A_2019162130932_O02806_01_T04237_02_002_02_V002.h5"
INCLUDED_FILES = [GRANULE_L2A_FILE, GRANULE_L2B_FILE, GRANULE_L4A_FILE]

GRANULE_NAME = "O02806_01"


def _array_from_dictstring(dictstring):
    dictstring = dictstring.replace("{", "").replace("}", "")
    return [float(x) for x in dictstring.split(",")]


class TestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.engine = gedidb_common.get_test_engine()
        gedidb_common.Base.metadata.create_all(cls.engine)

    @classmethod
    def tearDownClass(cls) -> None:
        gedidb_common.Base.metadata.drop_all(cls.engine)

    def test_write_granule(self):
        with unittest.mock.patch(
            "gedidb.database.gedidb_common.get_engine", return_value=self.engine
        ):
            _write_db((GRANULE_NAME, GRANULE_FNAME, INCLUDED_FILES))

        with self.engine.connect() as conn:
            granule_df = pd.read_sql(text("SELECT * FROM gedi_granules"), conn)
            self.assertEqual(len(granule_df), 1)
            self.assertEqual(granule_df.iloc[0]["granule_name"], GRANULE_NAME)
            self.assertEqual(
                granule_df.iloc[0]["granule_file"], GRANULE_FNAME.name
            )
            self.assertEqual(granule_df.iloc[0]["l2a_file"], GRANULE_L2A_FILE)
            self.assertEqual(granule_df.iloc[0]["l2b_file"], GRANULE_L2B_FILE)
            self.assertEqual(granule_df.iloc[0]["l4a_file"], GRANULE_L4A_FILE)
            self.assertEqual(
                granule_df.iloc[0]["granule_hash"],
                "52534f927d60fc7dca2accaca7357d91",
            )

            shots_gdf_orig = gpd.read_parquet(GRANULE_FNAME)
            shots_gdf_new = gpd.read_postgis(
                text("SELECT * FROM filtered_l2ab_l4a_shots"),
                conn,
                geom_col="geometry",
            )

            self.assertEqual(len(shots_gdf_orig), len(shots_gdf_new))

            # pick a random shot and compare some of the data
            # including the geometry, a Float column, a SmallInt column, and an Array column
            shot_idx = 100
            shot_orig = shots_gdf_orig.iloc[shot_idx]
            shot_number = shot_orig["shot_number"].item()
            shot_new = shots_gdf_new[
                shots_gdf_new["shot_number"] == shot_number
            ]
            self.assertEqual(
                shot_orig.lon_lowestmode_level2A.item(),
                shot_new.lon_lowestmode.item(),
            )
            self.assertEqual(
                shot_orig.degrade_flag_level2A.item(),
                shot_new.degrade_flag.item(),
            )
            self.assertEqual(
                shot_orig.sensitivity_a0_level2A.item(),
                shot_new.sensitivity_a0.item(),
            )
            self.assertEqual(
                shot_orig.agbd_level4A.item(), shot_new.agbd.item()
            )

            self.assertEqual(
                _array_from_dictstring(shot_orig.pai_z_level2B),
                shot_new.pai_z.item(),
            )
            self.assertEqual(shot_orig.geometry.x, shot_new.geometry.item().x)
            self.assertEqual(shot_orig.geometry.y, shot_new.geometry.item().y)


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
