import geopandas as gpd
import pandas as pd
import pathlib

from gedidb.granule.gedi_granule import GediGranule, GediBeam
from gedidb.granule import granule_name
from gedidb.constants import WGS84


class L4ABeam(GediBeam):
    def __init__(self, granule: GediGranule, beam_name: str):
        super().__init__(granule=granule, beam_name=beam_name)

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations == None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["lon_lowestmode"],
                y=self["lat_lowestmode"],
                crs=WGS84,
            )
        return self._shot_geolocations

    def _get_main_data_dict(self) -> dict:
        """
        Return the main data for all shots in a GEDI L4A product beam as a dictionary.
        Download the L4A data dictionary from
        https://daac.ornl.gov/GEDI/guides/GEDI_L4A_AGB_Density.html for details
        of all the available variables.

        Returns: A dictionary containing the main data for all shots in the given
            beam of the granule.
        """
        gedi_l4a_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
        data = {
            # General identifiable data
            "granule_name": [self.parent_granule.filename] * self.n_shots,
            "shot_number": self["shot_number"][:],
            "beam_type": [self.beam_type] * self.n_shots,
            "beam_name": [self.name] * self.n_shots,
            # Temporal data
            "delta_time": self["delta_time"][:],
            "absolute_time": (
                gedi_l4a_count_start
                + pd.to_timedelta(self["delta_time"], unit="seconds")
            ),
            # Quality data
            "sensitivity": self["sensitivity"][:],
            "algorithm_run_flag": self["algorithm_run_flag"][:],
            "degrade_flag": self["degrade_flag"][:],
            "l2_quality_flag": self["l2_quality_flag"][:],
            "l4_quality_flag": self["l4_quality_flag"][:],
            "predictor_limit_flag": self["predictor_limit_flag"][:],
            "response_limit_flag": self["response_limit_flag"][:],
            "surface_flag": self["surface_flag"][:],
            # Processing data
            "selected_algorithm": self["selected_algorithm"][:],
            "selected_mode": self["selected_mode"][:],
            # Geolocation data
            "elev_lowestmode": self["elev_lowestmode"][:],
            "lat_lowestmode": self["lat_lowestmode"][:],
            "lon_lowestmode": self["lon_lowestmode"][:],
            # ABGD data
            "agbd": self["agbd"][:],
            "agbd_pi_lower": self["agbd_pi_lower"][:],
            "agbd_pi_upper": self["agbd_pi_upper"][:],
            "agbd_se": self["agbd_se"][:],
            "agbd_t": self["agbd_t"][:],
            "agbd_t_se": self["agbd_t_se"][:],
            # Land cover data
            "pft_class": self["land_cover_data/pft_class"][:],
            "region_class": self["land_cover_data/region_class"][:],
        }
        return data


class L4AGranule(GediGranule):
    def __init__(self, file_path: pathlib.Path):
        super().__init__(file_path)

    @property
    def filename_metadata(self) -> granule_name.GediNameMetadata:
        if self._parsed_filename_metadata is None:
            self._parsed_filename_metadata = (
                granule_name._parse_ornl_granule_filename(self.filename)
            )
        return self._parsed_filename_metadata

    def _beam_from_name(self, beam_name: str) -> GediBeam:
        if not beam_name in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L4ABeam(granule=self, beam_name=beam_name)
