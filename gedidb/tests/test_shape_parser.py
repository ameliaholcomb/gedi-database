import unittest
import geopandas as gpd
import pathlib
from shapely import orient_polygons

from gedidb.common.shape_parser import (
    get_n_coords,
    get_covering_region_for_shape,
    orient_shape,
    check_and_format_shape,
    DetailError,
)


THIS_DIR = pathlib.Path(__file__).parent


class TestShapeParser(unittest.TestCase):
    # The test shape has:
    # - over 4999 coordinates
    # - multiple features (rows)
    # - multipolygons among the features
    # - polygons with holes
    # - geometries that span the antimeridian (180º)
    TEST_SHAPE = (
        THIS_DIR
        / "data"
        / "southeast_asia_oceania_evergreen_moist_dry_forest_v2017"
    )

    def test_get_n_coords(self):
        shp = gpd.GeoDataFrame.from_file(self.TEST_SHAPE)
        n_coords = get_n_coords(shp)
        self.assertEqual(n_coords, 346278)

    def test_orient_shape(self):
        shp = gpd.GeoDataFrame.from_file(self.TEST_SHAPE)
        disoriented = orient_shape(shp, exterior_cw=False)
        oriented = orient_shape(disoriented, exterior_cw=True)
        for poly in oriented:
            if poly.geom_type.startswith("Multi"):
                for part in poly.geoms:
                    self.assertFalse(part.exterior.is_ccw)
                    if len(part.interiors) > 0:
                        for interior in part.interiors:
                            self.assertTrue(interior.is_ccw)
            else:
                self.assertFalse(poly.exterior.is_ccw)
                if len(poly.interiors) > 0:
                    for interior in poly.interiors:
                        self.assertTrue(interior.is_ccw)

    def test_get_covering_region_for_shape(self):
        shp = gpd.GeoDataFrame.from_file(self.TEST_SHAPE)
        covering = get_covering_region_for_shape(shp)
        self.assertEqual(len(covering.geometry), 1)
        self.assertTrue(covering.contains(shp.union_all()).all())

    def test_detail_error(self):
        shp = gpd.GeoDataFrame.from_file(self.TEST_SHAPE)
        max_coords = 5
        with self.assertRaises(DetailError):
            check_and_format_shape(shp, simplify=False, max_coords=max_coords)

    def test_check_and_format_shape(self):
        shp = gpd.GeoDataFrame.from_file(self.TEST_SHAPE)
        # Test with simplification
        formatted = check_and_format_shape(shp, simplify=True, max_coords=4999)
        n_coords = get_n_coords(formatted)
        self.assertLessEqual(n_coords, 4999)
        self.assertTrue(formatted.contains(shp.union_all()).all())

        # Test without simplification should raise error
        with self.assertRaises(DetailError):
            check_and_format_shape(shp, simplify=False, max_coords=4999)

        # Test with a very low max_coords should raise ValueError
        # even after simplification
        with self.assertRaises(ValueError):
            check_and_format_shape(shp, simplify=True, max_coords=3)
