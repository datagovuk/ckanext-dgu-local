import json
from pprint import pprint

from nose.tools import assert_equal

from ckanext.harvest.harvesters.base import PackageDictDefaults
from ckanext.dgulocal.harvester import LGAHarvester

class MockObject(dict):
    def __getattr__(self, name):
        return self[name]

class TestGetPackageDict:

    def _get_test_harvest_object(self):
        content = {
                'title': 'Test dataset',
                'description': 'Test description',
                'active': True,
                'rights': 'http://www.nationalarchives.gov.uk/doc/open-government-licence/',
                'services': '',
                'functions': '',
                'resources': [
                    {'active': True,
                     'url': 'http://test.com/file.xls',
                     'mimetype': 'text/csv',
                     'title': 'Some file',
                     'resource_type': ''  # i.e. data
                     }
                    ]
                }
        harvest_source = MockObject()
        harvest_job = MockObject(harvest_source=harvest_source)
        harvest_object = MockObject(
                job=harvest_job,
                content=json.dumps(content),
                )
        return harvest_object

    def test_get_package_dict(self):
        h = LGAHarvester()
        harvest_object = self._get_test_harvest_object()
        package_dict_defaults = PackageDictDefaults()
        source_config = {}
        existing_pkg = MockObject(resources=[])
        pkg_dict = h.get_package_dict(harvest_object, package_dict_defaults,
                                      source_config, existing_pkg)
        pprint(pkg_dict)
        assert_equal(pkg_dict, {
            'name': 'test-dataset',
            'title': u'Test dataset',
            'notes': u'Test description',
            'license_id': 'uk-ogl',
            'state': 'active',
            'tags': [],
            'resources': [],
            'extras': {'lga_functions': '', 'lga_services': '', 'local': True},
            })
