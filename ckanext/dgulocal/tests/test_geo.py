from nose.tools import assert_equal

import ckanext.dgulocal.lib.geo as geo


class TestGeo:

    def test_lookup(self):
        import json

        # Peterborough
        b = geo.get_boundary("http://statistics.data.gov.uk/doc/statistical-geography/E06000031")
        boundary = json.loads(b)
        assert_equal(len(boundary), 2)
        assert_equal(len(boundary['coordinates']), 432)

