import datetime
import geopandas as gpd
import pandas as pd
import pathlib
import xarray

from gedidb.granule.gedi_granule import GediGranule, GediBeam
from gedidb.granule import granule_name


class L1BBeam(GediBeam):
    def __init__(self, granule: GediGranule, beam_name: str):
        super().__init__(granule=granule, beam_name=beam_name)

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations == None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["geolocation/longitude_lastbin"],
                y=self["geolocation/latitude_lastbin"],
                crs="EPSG:4326",
            )
        return self._shot_geolocations

    def _get_main_data_dict(self) -> dict:
        """
        Return the main data for all shots in a GEDI L1B product beam as dictionary.

        Returns:
            dict: A dictionary containing the main data for all shots in the given
                beam of the granule.
        """
        data = {
            # General identifiable data
            "granule_name": [self.parent_granule.filename] * self.n_shots,
            "shot_number": self["shot_number"][:],
            "beam_type": [self.beam_type] * self.n_shots,
            "beam_name": [self.name] * self.n_shots,
            # Temporal data
            "delta_time": self["delta_time"][:],
            # Quality data
            "degrade": self["geolocation/degrade"][:],
            "stale_return_flag": self["stale_return_flag"][:],
            "solar_elevation": self["geolocation/solar_elevation"][:],
            "solar_azimuth": self["geolocation/solar_elevation"][:],
            "rx_energy": self["rx_energy"][:],
            # DEM
            "dem_tandemx": self["geolocation/digital_elevation_model"][:],
            "dem_srtm": self["geolocation/digital_elevation_model_srtm"][:],
            # geolocation bin0
            "latitude_bin0": self["geolocation/latitude_bin0"][:],
            "latitude_bin0_error": self["geolocation/latitude_bin0_error"][:],
            "longitude_bin0": self["geolocation/longitude_bin0"][:],
            "longitude_bin0_error": self["geolocation/longitude_bin0_error"][:],
            "elevation_bin0": self["geolocation/elevation_bin0"][:],
            "elevation_bin0_error": self["geolocation/elevation_bin0_error"][:],
            # geolocation lastbin
            "latitude_lastbin": self["geolocation/latitude_lastbin"][:],
            "latitude_lastbin_error": self[
                "geolocation/latitude_lastbin_error"
            ][:],
            "longitude_lastbin": self["geolocation/longitude_lastbin"][:],
            "longitude_lastbin_error": self[
                "geolocation/longitude_lastbin_error"
            ][:],
            "elevation_lastbin": self["geolocation/elevation_lastbin"][:],
            "elevation_lastbin_error": self[
                "geolocation/elevation_lastbin_error"
            ][:],
            # relative waveform position info in beam and ssub-granule
            "waveform_start": self["rx_sample_start_index"][:] - 1,
            "waveform_count": self["rx_sample_count"][:],
        }
        return data

    @property
    def waveform(self):
        return xarray.DataArray(
            self["rxwaveform"][:],
            dims=["sample_points"],
            name=f"{self.parent_granule.filename[:-3]}_{self.name}",
            attrs={
                "granule": self.parent_granule.filename,
                "beam": self.name,
                "type": self.beam_type,
                "creation_timestamp_utc": str(datetime.datetime.utcnow()),
            },
        )

    def save_waveform(
        self, save_dir: pathlib.Path, overwrite: bool = False
    ) -> None:
        waveform = self.waveform
        save_name = f"{self.parent_granule.filename[:-3]}_{self.name}.nc"
        save_path = pathlib.Path(save_dir) / save_name
        if overwrite or not save_path.exists():
            waveform.to_netcdf(save_path)


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
        return L1BBeam(granule=self, beam_name=beam_name)
