import logging
import os
from pathlib import Path

import dotenv

from .constants import GediProduct

dotenv.load_dotenv()

# ---------------- PATH CONSTANTS -------------------
#  Source folder path
constants_path = Path(__file__)
SRC_PATH = constants_path.parent
PROJECT_PATH = SRC_PATH.parent
CONDA_ENV = os.getenv("CONDA_DEFAULT_ENV")

#  Data related paths
DATA_PATH = Path(os.environ["DATA_PATH"])
USER_PATH = Path(os.environ["USER_PATH"])
EARTHDATA_USER = os.getenv("EARTHDATA_USER")
EARTHDATA_PASSWORD = os.getenv("EARTHDATA_PASSWORD")
EARTH_DATA_COOKIE_FILE = Path(os.environ["EARTH_DATA_COOKIE_FILE"])

GEDI_PATH = DATA_PATH / "GEDI"


def gedi_product_path(product: GediProduct) -> Path:
    return GEDI_PATH / product.value


GEDI_L1B_PATH = gedi_product_path(GediProduct.L1B)
GEDI_L2A_PATH = gedi_product_path(GediProduct.L2A)
GEDI_L4A_PATH = gedi_product_path(GediProduct.L4A)

# ---------------- DATABASE CONSTANTS ----------------
DB_HOST = os.getenv("DB_HOST")  # JASMIN database server
DB_PORT = "5432"
DB_NAME = os.getenv("DB_NAME")  # Database for GEDI shots
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_POSTGRES = "postgresql"
DB_CONFIG = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

DB_TEST_NAME = "test"
DB_TEST_CONFIG = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_TEST_NAME}"
)
