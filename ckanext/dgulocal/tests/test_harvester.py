import os

from nose.tools import assert_equal

from ckanext.dgulocal.harvester import LGAHarvester

class TestHarvester:

    def test_find_package(self):
        h = LGAHarvester()
        #pkg = h._find_dataset_by_identifier("non-existent", "non-existent")