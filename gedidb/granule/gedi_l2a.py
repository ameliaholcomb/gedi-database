import geopandas as gpd
import pandas as pd
import pathlib

from gedidb.granule.gedi_granule import GediGranule, GediBeam
from gedidb.granule import granule_name


class L2ABeam(GediBeam):
    def __init__(self, granule: GediGranule, beam_name: str):
        super().__init__(granule=granule, beam_name=beam_name)

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations == None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["lon_lowestmode"],
                y=self["lat_lowestmode"],
                crs="EPSG:4326",
            )
        return self._shot_geolocations

    def _get_main_data_dict(self) -> dict:
        """
        Return the main data for all shots in a GEDI L2A product beam as dictionary.

        Returns:
            dict: A dictionary containing the main data for all shots in the given
                beam of the granule.
        """
        gedi_l2a_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
        data = {
            # General identifiable data
            "granule_name": [self.parent_granule.filename] * self.n_shots,
            "shot_number": self["shot_number"][:],
            "beam_type": [self.beam_type] * self.n_shots,
            "beam_name": [self.name] * self.n_shots,
            # Temporal data
            "delta_time": self["delta_time"][:],
            "absolute_time": (
                gedi_l2a_count_start
                + pd.to_timedelta(self["delta_time"], unit="seconds")
            ),
            # Quality data
            "sensitivity": self["sensitivity"][:],
            "quality_flag": self["quality_flag"][:],
            "solar_elevation": self["solar_elevation"][:],
            "solar_azimuth": self["solar_elevation"][:],
            "energy_total": self["energy_total"][:],
            # DEM
            "dem_tandemx": self["digital_elevation_model"][:],
            "dem_srtm": self["digital_elevation_model_srtm"][:],
            # Processing data
            "selected_algorithm": self["selected_algorithm"][:],
            "selected_mode": self["selected_mode"][:],
            # Geolocation data
            "lon_lowestmode": self["lon_lowestmode"][:],
            "longitude_bin0_error": self["longitude_bin0_error"][:],
            "lat_lowestmode": self["lat_lowestmode"][:],
            "latitude_bin0_error": self["latitude_bin0_error"][:],
            "elev_lowestmode": self["elev_lowestmode"][:],
            "elevation_bin0_error": self["elevation_bin0_error"][:],
            "lon_highestreturn": self["lon_highestreturn"][:],
            "lat_highestreturn": self["lat_highestreturn"][:],
            "elev_highestreturn": self["elev_highestreturn"][:],
        } | {f"rh{i}": self["rh"][:, i] for i in range(101)}
        return data


class L2AGranule(GediGranule):
    def __init__(self, file_path: pathlib.Path):
        super().__init__(file_path)

    @property
    def filename_metadata(self) -> granule_name.GediNameMetadata:
        if self._parsed_filename_metadata is None:
            self._parsed_filename_metadata = (
                granule_name._parse_lp_granule_filename(self.filename)
            )
        return self._parsed_filename_metadata

    def _beam_from_name(self, beam_name: str) -> GediBeam:
        if not beam_name in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L2ABeam(granule=self, beam_name=beam_name)
