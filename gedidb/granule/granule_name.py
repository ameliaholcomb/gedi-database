from dataclasses import dataclass
import re

@dataclass
class GediNameMetadata:
    """Data class container for metadata derived from GEDI file name conventions."""

    product: str
    year: str
    julian_day: str
    hour: str
    minute: str
    second: str
    orbit: str
    ground_track: str
    positioning: str
    granule_production_version: str
    release_number: str


@dataclass
class GediLPNameMetadata(GediNameMetadata):
    """Data class container for metadata derived from GEDI file names
    released by the LP DAAC"""

    sub_orbit_granule: str
    pge_version_number: str


@dataclass
class GediORNLNameMetadata(GediNameMetadata):
    """Data class container for metadata derived from GEDI file names
    released by the ORNL DAAC"""


GEDI_SUBPATTERN_LP = GediLPNameMetadata(
    product=r"\w+_\w",
    year=r"\d{4}",
    julian_day=r"\d{3}",
    hour=r"\d{2}",
    minute=r"\d{2}",
    second=r"\d{2}",
    orbit=r"O\d+",
    sub_orbit_granule=r"\d{2}",
    ground_track=r"T\d+",
    positioning=r"\d{2}",
    pge_version_number=r"\d{3}",
    granule_production_version=r"\d{2}",
    release_number=r"V\d+",
)

GEDI_SUBPATTERN_ORNL = GediORNLNameMetadata(
    product=r"\w+_\w",
    year=r"\d{4}",
    julian_day=r"\d{3}",
    hour=r"\d{2}",
    minute=r"\d{2}",
    second=r"\d{2}",
    orbit=r"O\d+",
    ground_track=r"T\d+",
    positioning=r"\d{2}",
    release_number=r"\d{3}",
    granule_production_version=r"\d{2}",
)


def parse_lp_granule_filename(gedi_filename: str) -> GediLPNameMetadata:
    GEDI_SUBPATTERN = GEDI_SUBPATTERN_LP
    gedi_naming_pattern = re.compile(
        (
            f"({GEDI_SUBPATTERN.product})"
            f"_({GEDI_SUBPATTERN.year})"
            f"({GEDI_SUBPATTERN.julian_day})"
            f"({GEDI_SUBPATTERN.hour})"
            f"({GEDI_SUBPATTERN.minute})"
            f"({GEDI_SUBPATTERN.second})"
            f"_({GEDI_SUBPATTERN.orbit})"
            f"_({GEDI_SUBPATTERN.sub_orbit_granule})"
            f"_({GEDI_SUBPATTERN.ground_track})"
            f"_({GEDI_SUBPATTERN.positioning})"
            f"_({GEDI_SUBPATTERN.pge_version_number})"
            f"_({GEDI_SUBPATTERN.granule_production_version})"
            f"_({GEDI_SUBPATTERN.release_number})"
        )
    )
    parse_result = re.search(gedi_naming_pattern, gedi_filename)
    if parse_result is None:
        raise ValueError(
            f"Filename {gedi_filename} does not conform the the GEDI naming pattern."
        )
    return GediLPNameMetadata(*parse_result.groups())


def parse_ornl_granule_filename(gedi_filename: str) -> GediORNLNameMetadata:
    GEDI_SUBPATTERN = GEDI_SUBPATTERN_ORNL
    gedi_naming_pattern = re.compile(
        (
            f"({GEDI_SUBPATTERN.product})"
            f"_({GEDI_SUBPATTERN.year})"
            f"({GEDI_SUBPATTERN.julian_day})"
            f"({GEDI_SUBPATTERN.hour})"
            f"({GEDI_SUBPATTERN.minute})"
            f"({GEDI_SUBPATTERN.second})"
            f"_({GEDI_SUBPATTERN.orbit})"
            f"_({GEDI_SUBPATTERN.ground_track})"
            f"_({GEDI_SUBPATTERN.positioning})"
            f"_({GEDI_SUBPATTERN.release_number})"
            f"_({GEDI_SUBPATTERN.granule_production_version})"
        )
    )
    parse_result = re.search(gedi_naming_pattern, gedi_filename)
    if parse_result is None:
        raise ValueError(
            f"Filename {gedi_filename} does not conform the the GEDI naming pattern."
        )
    return GediORNLNameMetadata(*parse_result.groups())

    