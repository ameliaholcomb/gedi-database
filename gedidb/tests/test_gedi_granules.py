import unittest
from gedidb.granule import gedi_l4a, gedi_l2b, gedi_l2a, gedi_l1b

L4A_NAME = "./data/GEDI04_A_2019110062417_O01994_04_T02062_02_002_01_V002.h5"
L2B_NAME = "./data/GEDI02_B_2019171194823_O02950_04_T00250_02_003_01_V002.h5"
L2A_NAME = "./data/GEDI02_A_2019162222610_O02812_04_T01244_02_003_01_V002.h5"


class TestCase(unittest.TestCase):
    def test_parse_granule_l4a(self):
        granule = gedi_l4a.L4AGranule(L4A_NAME)
        self.assertEqual(granule.n_beams, 8)
        for beam in granule.iter_beams():
            beam.quality_filter()
            _ = beam.main_data

    def test_parse_granule_l2b(self):
        granule = gedi_l2b.L2BGranule(L2B_NAME)
        self.assertEqual(granule.n_beams, 8)
        for beam in granule.iter_beams():
            beam.quality_filter()
            _ = beam.main_data

    def test_parse_granule_l2a(self):
        granule = gedi_l2a.L2AGranule(L2A_NAME)
        self.assertEqual(granule.n_beams, 8)
        for beam in granule.iter_beams():
            beam.quality_filter()
            _ = beam.main_data


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
