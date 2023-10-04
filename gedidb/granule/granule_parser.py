from typing import Optional
from tqdm.auto import tqdm
import geopandas as gpd
import pandas as pd
from pathlib import Path
import warnings

from gedidb.granule.gedi_granule import GediGranule  # for typing only
from gedidb.granule.gedi_l4a import L4AGranule
from gedidb.granule.gedi_l2b import L2BGranule
from gedidb.granule.gedi_l2a import L2AGranule
from gedidb.constants import WGS84, GediProduct

from typing import Union


def parse_file(
    product: Union[str, GediProduct], file: Path, quality_filter=True
) -> gpd.GeoDataFrame:
    product = GediProduct(product)
    if product == GediProduct.L4A:
        return parse_file_l4a(file, quality_filter)
    elif product == GediProduct.L2B:
        return parse_file_l2b(file, quality_filter)
    elif product == GediProduct.L2A:
        return parse_file_l2a(file, quality_filter)
    else:
        raise ValueError(f"Product {product} not supported")


def parse_file_l4a(file: Path, quality_filter=True) -> gpd.GeoDataFrame:
    granule = L4AGranule(file)
    return _parse(granule, quality_filter)


def parse_file_l2b(file: Path, quality_filter=True) -> gpd.GeoDataFrame:
    granule = L2BGranule(file)
    return _parse(granule, quality_filter)


def parse_file_l2a(file: Path, quality_filter=True) -> gpd.GeoDataFrame:
    granule = L2AGranule(file)
    return _parse(granule, quality_filter)


def _parse(granule: GediGranule, quality_filter=True) -> gpd.GeoDataFrame:
    granule_data = []
    for beam in tqdm(granule.iter_beams(), total=granule.n_beams):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                if quality_filter:
                    beam.quality_filter()
                beam.sql_format_arrays()
                granule_data.append(beam.main_data)
        except KeyError as e:
            continue
    df = pd.concat(granule_data, ignore_index=True)
    gdf = gpd.GeoDataFrame(df, crs=WGS84)
    granule.close()
    return gdf


def spatial_filter_granules(
    gdf: gpd.GeoDataFrame, roi: Optional[gpd.GeoDataFrame]
) -> gpd.GeoDataFrame:
    if roi is None:
        return gdf

    # Filter for shots that fall within the ROI
    gdf_filtered = gpd.sjoin(gdf, roi, predicate="within", how="inner")
    gdf_filtered = gdf_filtered.drop("index_right", axis=1)
    return gdf_filtered
