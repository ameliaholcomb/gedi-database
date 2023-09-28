import os
import subprocess
from gedidb import constants, environment


def authenticate():
    if os.path.exists(environment.EARTH_DATA_COOKIE_FILE):
        return
    print("No authentication cookies found, fetching earthdata cookies ...")
    netrc_file = environment.USER_PATH / ".netrc"
    add_login = True
    if netrc_file.exists():
        with open(netrc_file, "r") as f:
            if "urs.earthdata.nasa.gov" in f.read():
                add_login = False

    if add_login:
        with open(environment.USER_PATH / ".netrc", "a+") as f:
            f.write(
                "\nmachine urs.earthdata.nasa.gov login {} password {}".format(
                    environment.EARTHDATA_USER, environment.EARTHDATA_PASSWORD
                )
            )
            os.fchmod(f.fileno(), 0o600)

    environment.EARTH_DATA_COOKIE_FILE.touch()
    subprocess.run(
        [
            "wget",
            "--load-cookies",
            environment.EARTH_DATA_COOKIE_FILE,
            "--save-cookies",
            environment.EARTH_DATA_COOKIE_FILE,
            "--keep-session-cookies",
            "https://urs.earthdata.nasa.gov",
        ],
        check=True,
    )
