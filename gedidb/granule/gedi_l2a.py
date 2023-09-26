import geopandas as gpd
import pandas as pd
import pathlib

from gedidb.granule import gedi_granule
from gedidb.granule import granule_name


class L2ABeam(gedi_granule.GediBeam):
    def __init__(self, granule: gedi_granule.GediGranule, beam_name: str):
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
            "sensitivity_a0": self["sensitivity"][:],
            "sensitivity_a2": self["geolocation/sensitivity_a2"][:],
            "quality_flag": self["quality_flag"][:],
            "degrade_flag": self["degrade_flag"][:],
            "solar_elevation": self["solar_elevation"][:],
            "solar_azimuth": self["solar_elevation"][:],
            "energy_total": self["energy_total"][:],
            "surface_flag": self["surface_flag"][:],
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
        } | {f"rh_{i}": self["rh"][:, i] for i in range(101)}
        return data

    def quality_filter(self):
        """Perform quality-filtering on main data.

        Until this function is called, all granule shots will be included in
         main_data. This filtering can be undone by resetting the cache.
        """
        filtered = self.main_data
        filtered["elevation_difference_tdx"] = (
            filtered["elev_lowestmode"] - filtered["dem_tandemx"]
        )
        filtered = filtered[
            (filtered["quality_flag"] == 1)
            & (filtered["sensitivity_a0"] >= 0.9)
            & (filtered["sensitivity_a0"] <= 1.0)
            & (filtered["sensitivity_a2"] > 0.95)
            & (filtered["sensitivity_a2"] <= 1.0)
            & (filtered["degrade_flag"].isin(gedi_granule.QDEGRADE))
            & (filtered["rh_100"] >= 0)
            & (filtered["rh_100"] < 120)
            & (filtered["surface_flag"] == 1)
            & (filtered["elevation_difference_tdx"] > -150)
            & (filtered["elevation_difference_tdx"] < 150)
        ]
        filtered = filtered.drop(
            ["elevation_difference_tdx", "quality_flag", "surface_flag"], axis=1
        )
        self._cached_data = filtered


class L2AGranule(gedi_granule.GediGranule):
    def __init__(self, file_path: pathlib.Path):
        super().__init__(file_path)

    def _beam_from_name(self, beam_name: str) -> gedi_granule.GediBeam:
        if not beam_name in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L2ABeam(granule=self, beam_name=beam_name)
