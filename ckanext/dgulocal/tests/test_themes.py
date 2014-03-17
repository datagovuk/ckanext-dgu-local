from nose.tools import assert_equal

import ckanext.dgulocal.lib.themes as themes

class TestThemes:

    def test_function_lookup(self):
        # 2
        assert_equal(themes.theme_lookup('', 'http://id.esd.org.uk/function/2'), ["Society"])

    def test_service_lookup(self):
        # 311/2
        assert_equal(themes.theme_lookup('http://id.esd.org.uk/service/311', ''), ["Society"])

    def test_both(self):
        assert_equal(themes.theme_lookup('http://id.esd.org.uk/service/311', 'http://id.esd.org.uk/function/2'), ["Society"])
