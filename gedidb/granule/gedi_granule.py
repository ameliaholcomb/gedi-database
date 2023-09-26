"""Module for convenient objects to deal with GEDI data products"""
from __future__ import annotations

import geopandas as gpd
import h5py
import numpy as np
import pandas as pd
import pathlib
from shapely.geometry import box

from typing import Iterable, Union, List
from gedidb.constants import WGS84
from gedidb.granule import granule_name

QDEGRADE = [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]


class GediGranule(h5py.File):  # TODO  pylint: disable=missing-class-docstring
    def __init__(self, file_path: pathlib.Path):
        super().__init__(file_path, "r")
        self.file_path = file_path
        self.beam_names = [
            name for name in self.keys() if name.startswith("BEAM")
        ]
        self._parsed_filename_metadata = None

    @property
    def filename_metadata(self) -> granule_name.GediNameMetadata:
        if self._parsed_filename_metadata is None:
            self._parsed_filename_metadata = (
                granule_name.parse_granule_filename(self.filename)
            )
        return self._parsed_filename_metadata

    @property
    def version(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["VersionID"]

    def filename_metadata(self):
        # Subclass must implement this
        raise NotImplementedError

    @property
    def start_datetime(self) -> pd.Timestamp:
        return pd.to_datetime(
            (
                f"{self.filename_metadata.year}"
                f".{self.filename_metadata.julian_day}"
                f".{self.filename_metadata.hour}"
                f":{self.filename_metadata.minute}"
                f":{self.filename_metadata.second}"
            ),
            format="%Y.%j.%H:%M:%S",
        )

    @property
    def product(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["shortName"]

    @property
    def uuid(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["uuid"]

    @property
    def filename(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["fileName"]

    @property
    def abstract(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["abstract"]

    @property
    def n_beams(self) -> int:
        return len(self.beam_names)

    def beam(self, identifier: Union[str, int]) -> GediBeam:
        if isinstance(identifier, int):
            return self._beam_from_index(identifier)
        elif isinstance(identifier, str):
            return self._beam_from_name(identifier)
        else:
            raise ValueError(
                "identifier must either be the beam index or beam name"
            )

    def _beam_from_index(self, beam_index: int) -> GediBeam:
        if not 0 <= beam_index < self.n_beams:
            raise ValueError(
                f"Beam index must be between 0 and {self.n_beams-1}"
            )

        beam_name = self.beam_names[beam_index]
        return self._beam_from_name(beam_name)

    def _beam_from_name(self, beam_name: str) -> GediBeam:
        # Subclass must implement this
        raise NotImplementedError

    def iter_beams(self) -> Iterable[GediBeam]:
        for beam_index in range(self.n_beams):
            yield self._beam_from_index(beam_index)

    def list_beams(self) -> list[GediBeam]:
        return list(self.iter_beams())

    def close(self) -> None:
        super().close()

    def __repr__(self) -> str:
        try:
            description = (
                "GEDI Granule:\n"
                f" Granule name: {self.filename}\n"
                f" Sub-granule:  {self.filename_metadata.sub_orbit_granule}\n"
                f" Product:      {self.product}\n"
                f" Release:      {self.filename_metadata.release_number}\n"
                f" No. beams:    {self.n_beams}\n"
                f" Start date:   {self.start_datetime.date()}\n"
                f" Start time:   {self.start_datetime.time()}\n"
                f" HDF object:   {super().__repr__()}"
            )
        except AttributeError:
            description = (
                "GEDI Granule:\n"
                f" Granule name: {self.filename}\n"
                f" Product:      {self.product}\n"
                f" No. beams:    {self.n_beams}\n"
                f" Start date:   {self.start_datetime.date()}\n"
                f" Start time:   {self.start_datetime.time()}\n"
                f" HDF object:   {super().__repr__()}"
            )
        return description


class GediBeam(h5py.Group):
    """
    Class containing GEDI data for a single beam of a granule.

    Args:
        granule: The parent granule for this beam
        beam name: The name of this beam, e.g. BEAM0000
    """

    def __init__(self, granule: GediGranule, beam_name: str, roi: box = None):
        super().__init__(granule[beam_name].id)
        self.parent_granule = granule  # Reference to parent granule
        self._cached_data = None
        self._shot_geolocations = None

    def list_datasets(self, top_level_only: bool = True) -> list[str]:
        if top_level_only:
            return list(self)
        else:
            # TODO
            raise NotImplementedError

    @property
    def name(self) -> str:
        return super().name[1:]

    @property
    def beam_type(self) -> str:
        return self.attrs["description"].split(" ")[0].lower()

    @property
    def quality(self) -> h5py.Dataset:
        return self["quality_flag"]

    @property
    def sensitivity(self) -> h5py.Dataset:
        return self["sensitivity"]

    @property
    def geolocation(self) -> h5py.Dataset:
        return self["geolocation"]

    @property
    def n_shots(self) -> int:
        return len(self["beam"])

    @property
    def main_data(self) -> gpd.GeoDataFrame:
        """
        Return the main data for all shots in beam as geopandas DataFrame.

        Returns:
            gpd.GeoDataFrame: A geopandas DataFrame containing the main data for the given beam object.
        """
        if self._cached_data is None:
            data = self._get_main_data_dict()
            geometry = self.shot_geolocations
            self._cached_data = gpd.GeoDataFrame(
                data, geometry=geometry, crs=WGS84
            )

        return self._cached_data

    def sql_format_arrays(self) -> None:
        """Forces array-type fields to be sql-formatted (text strings).

        Until this function is called, array-type fields will be np.array() objects. This formatting can be undone by resetting the cache.
        """
        array_cols = [c for c in self.main_data.columns if c.endswith("_z")]
        for c in array_cols:
            self._cached_data[c] = self.main_data[c].map(self._arr_to_str)

    def reset_cache(self):
        self._cached_data = None

    def _accumulate_waveform_data(
        self, name: str, start: int, end: int
    ) -> np.array:
        waveform_data_all = np.array(self[name][:])
        data = []
        for i in range(len(start)):
            dz = waveform_data_all[
                start[i] : end[i]
            ]  # this is a view, not a copy
            data.append(dz)
        return data

    def _arr_to_str(self, arr: Union[List[float], np.array]) -> str:
        """Converts array type data to SQL-friendly string."""
        return "{" + ", ".join(map(str, arr)) + "}"

    def __repr__(self) -> str:
        description = (
            "GEDI Beam object:\n"
            f" Beam name:  {self.name}\n"
            f" Beam type:  {self.attrs['description']}\n"
            f" Shots:      {self.n_shots}\n"
            f" HDF object: {super().__repr__()}"
        )
        return description
