from geopandas import gpd
from shapely import geometry


class DetailError(Exception):
    """Used when too many points in a shape for NASA's API"""

    def __init__(self, n_coords: int):
        self.n_coords = n_coords


def get_covering_region_for_shape(shp: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """The NASA CMR API can only handle shapes with less than 5000 points.
    To simplify shapes without adding lots of extra area
    (as a bounding box or convex hull would), we instead tile the region into
    covering 1x1 degree boxes, and return the union of those boxes.
    """
    # generate all the tiles in the world
    minx = -180
    maxx = 180
    miny = -90
    maxy = 90
    tiles = []
    for i in range(minx, maxx):
        for j in range(maxy, miny, -1):
            tile = geometry.box(i, j - 1, i + 1, j)
            tiles.append(tile)
    tile_df = gpd.GeoDataFrame(geometry=tiles, crs="EPSG:4326")

    covering_tiles = tile_df.sjoin(shp, how="inner", predicate="intersects")
    covering = gpd.GeoSeries(covering_tiles.union_all(), crs="EPSG:4326")
    return covering


def get_n_coords(shp: gpd.GeoDataFrame) -> int:
    """Returns the number of coordinates in a shape"""
    n_coords = 0
    for row in shp.geometry:
        if row.geom_type.startswith("Multi"):
            n_coords += sum([len(part.exterior.coords) for part in row.geoms])
        else:
            n_coords += len(row.exterior.coords)
    return n_coords


def orient_shape(shp: gpd.GeoDataFrame) -> gpd.GeoSeries:
    """Orients the shape(s) in a GeoDataFrame to be clockwise"""
    oriented = []
    for row in shp.geometry:
        if row.geom_type.startswith("Multi"):
            oriented.append(
                geometry.MultiPolygon(
                    [geometry.polygon.orient(s) for s in row.geoms]
                )
            )
        else:
            oriented.append(geometry.polygon.orient(row))
    return gpd.GeoSeries(oriented)


def check_and_format_shape(
    shp: gpd.GeoDataFrame, simplify: bool = False, max_coords: int = 4999
) -> gpd.GeoSeries:
    """
    Checks a shape for compatibility with NASA's API.

    Args:
        shp (gpd.GeoDataFrame): The shape to check and format.
        simplify (bool): Whether to simplify the shape if it doesn't meet the max_coords threshold.
        max_coords (int): Threshold for simplifying, must be less than 5000 (NASA's upper-bound).

    Raises:
        ValueError: If max_coords is not less than 5000 or more than one polygon is supplied.
        DetailError: If simplify is not true and the shape does not have less than max_coord points.

    Returns:
        GeoSeries: The possibly simplified shape.
    """
    if max_coords > 4999:
        raise ValueError("NASA's API can only cope with less than 5000 points")

    n_coords = get_n_coords(shp)
    if n_coords > max_coords:
        if not simplify:
            raise DetailError(n_coords)
        shp = get_covering_region_for_shape(shp)
        n_coords = get_n_coords(shp)
        if n_coords > max_coords:
            raise ValueError(
                f"""Covering region still has {n_coords},
                but max_coords is {max_coords}"""
            )

    return orient_shape(shp)
