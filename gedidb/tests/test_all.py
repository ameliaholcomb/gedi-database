import unittest

from gedidb.tests import test_granule_name
from gedidb.tests import test_gedi_granules

suites = []
suites.append(test_gedi_granules.suite)
suites.append(test_granule_name.suite)

suite = unittest.TestSuite(suites)

if __name__ == "__main__":  # pragma: no cover
    unittest.TextTestRunner(verbosity=2).run(suite)