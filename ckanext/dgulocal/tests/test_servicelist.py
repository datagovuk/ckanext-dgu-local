from nose.tools import assert_equal

import ckanext.dgulocal.lib.services as services


class TestServiceList:

    def test_lookup(self):
        data = services.load_services()

        assert_equal(len(data), 2)
        assert_equal(len(data.get('functions',{})), 113)
        assert_equal(len(data.get('services',{})), 1126)
