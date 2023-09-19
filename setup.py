from setuptools import find_packages, setup

setup(
    name="gedidb",
    version="0.0.1",
    author="Amelia Holcomb",
    author_email="ah2174@cl.cam.ac.uk",
    description="This package is designed to set up a database of GEDI data.",
    packages=find_packages(),
    test_suite="src.tests.test_all.suite",
)
