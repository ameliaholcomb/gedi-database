import geopandas as gpd
import pandas as pd
import pathlib

from gedidb.granule.gedi_granule import GediGranule, GediBeam
from gedidb.granule import granule_name


class L2BBeam(GediBeam):
    def __init__(self, granule: GediGranule, beam_name: str):
        super().__init__(granule=granule, beam_name=beam_name)

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations == None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["geolocation/lon_lowestmode"],
                y=self["geolocation/lat_lowestmode"],
                crs="EPSG:4326",
            )
        return self._shot_geolocations

    def _get_main_data_dict(self) -> dict:
        """
        Return the main data for all shots in a GEDI L2B product beam as dictionary.

        Returns:
            dict: A dictionary containing the main data for all shots in the given
                beam of the granule.
        """
        gedi_l2b_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
        data = {
            # General identifiable data
            "granule_name": [self.parent_granule.filename] * self.n_shots,
            "shot_number": self["shot_number"][:],
            "beam_type": [self.beam_type] * self.n_shots,
            "beam_name": [self.name] * self.n_shots,
            # Temporal data
            "delta_time": self["geolocation/delta_time"][:],
            "absolute_time": (
                gedi_l2b_count_start
                + pd.to_timedelta(self["delta_time"], unit="seconds")
            ),
            # Quality data
            "algorithm_run_flag": self["algorithmrun_flag"][:],
            "l2a_quality_flag": self["l2a_quality_flag"][:],
            "l2b_quality_flag": self["l2b_quality_flag"][:],
            "sensitivity": self["sensitivity"][:],
            "degrade_flag": self["geolocation/degrade_flag"][:],
            "stale_return_flag": self["stale_return_flag"][:],
            "surface_flag": self["surface_flag"][:],
            "solar_elevation": self["geolocation/solar_elevation"][:],
            "solar_azimuth": self["geolocation/solar_azimuth"][:],
            # Scientific data
            "cover": self["cover"][:],
            "cover_z": list(self["cover_z"][:]),
            "fhd_normal": self["fhd_normal"][:],
            "num_detectedmodes": self["num_detectedmodes"][:],
            "omega": self["omega"][:],
            "pai": self["pai"][:],
            "pai_z": list(self["pai_z"][:]),
            "pavd_z": list(self["pavd_z"][:].tolist()),
            "pgap_theta": self["pgap_theta"][:],
            "pgap_theta_error": self["pgap_theta_error"][:],
            "rg": self["rg"][:],
            "rh100": self["rh100"][:],
            "rhog": self["rhog"][:],
            "rhog_error": self["rhog_error"][:],
            "rhov": self["rhov"][:],
            "rhov_error": self["rhov_error"][:],
            "rossg": self["rossg"][:],
            "rv": self["rv"][:],
            "rx_range_highestreturn": self["rx_range_highestreturn"][:],
            # DEM
            "dem_tandemx": self["geolocation/digital_elevation_model"][:],
            # Land cover data: NOTE this is gridded and/or derived data
            "gridded_leaf_off_flag": self["land_cover_data/leaf_off_flag"][:],
            "gridded_leaf_on_doy": self["land_cover_data/leaf_on_doy"][:],
            "gridded_leaf_on_cycle": self["land_cover_data/leaf_on_cycle"][:],
            "interpolated_modis_nonvegetated": self[
                "land_cover_data/modis_nonvegetated"
            ][:],
            "interpolated_modis_treecover": self[
                "land_cover_data/modis_treecover"
            ][:],
            "gridded_pft_class": self["land_cover_data/pft_class"][:],
            "gridded_region_class": self["land_cover_data/region_class"][:],
            # Processing data
            "selected_l2a_algorithm": self["selected_l2a_algorithm"][:],
            "selected_rg_algorithm": self["selected_rg_algorithm"][:],
            # Geolocation data
            "lon_highestreturn": self["geolocation/lon_highestreturn"][:],
            "lon_lowestmode": self["geolocation/lon_lowestmode"][:],
            "longitude_bin0": self["geolocation/longitude_bin0"][:],
            "longitude_bin0_error": self["geolocation/longitude_bin0_error"][:],
            "lat_highestreturn": self["geolocation/lat_highestreturn"][:],
            "lat_lowestmode": self["geolocation/lat_lowestmode"][:],
            "latitude_bin0": self["geolocation/latitude_bin0"][:],
            "latitude_bin0_error": self["geolocation/latitude_bin0_error"][:],
            "elev_highestreturn": self["geolocation/elev_highestreturn"][:],
            "elev_lowestmode": self["geolocation/elev_lowestmode"][:],
            "elevation_bin0": self["geolocation/elevation_bin0"][:],
            "elevation_bin0_error": self["geolocation/elevation_bin0_error"][:],
            # waveform data
            "waveform_count": self["rx_sample_count"][:],
            "waveform_start": self["rx_sample_start_index"][:] - 1,
        }

        # handle array data
        ## could delete waveform start/count after storing waveform chunks
        start = data["waveform_start"]
        end = start + data["waveform_count"]
        data["pgap_theta_z"] = self._accumulate_waveform_data(
            "pgap_theta_z", start, end
        )
        return data


class L2BGranule(GediGranule):
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
        return L2BBeam(granule=self, beam_name=beam_name)
