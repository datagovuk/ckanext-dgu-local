import os

from nose.tools import assert_equal

from ckanext.dgulocal.lib.inventory import InventoryDocument

class TestInventory:

    def test_parse_metadata(self):

        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "test_inventory.xml")

        with InventoryDocument(open(f, 'r').read()) as doc:
            valid, err = doc.validate()
            metadata = doc.prepare_metadata()

            assert_equal(len(metadata), 4)
            assert_equal(metadata['publisher'],
                'http://opendatacommunities.org/doc/unitary-authority/peterborough')
            assert_equal(metadata['modified'], '2013-12-01')
            assert_equal(metadata['title'], 'Peterborough  datasets')

    def test_parse_datasets(self):

        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "test_inventory.xml")

        with InventoryDocument(open(f, 'r').read()) as doc:
            valid, err = doc.validate()

            dataset = doc.datasets().next()
            assert_equal(dataset['modified'], '2013-12-01')
            assert_equal(dataset['active'], True)
            assert_equal(len(dataset['resources']), 3)
