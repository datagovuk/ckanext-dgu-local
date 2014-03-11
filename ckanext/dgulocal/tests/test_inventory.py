import os

from nose.tools import assert_equal

from ckanext.dgulocal.lib.inventory import InventoryDocument

class TestInventory:

    def test_parse_metadata(self):

        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "test_inventory.xml")

        with InventoryDocument(open(f, 'r').read()) as doc:
            metadata = doc.prepare_metadata()
            assert_equal(len(metadata), 5)
            assert_equal(metadata['publisher'],
                'http://opendatacommunities.org/doc/unitary-authority/peterborough')
            assert_equal(metadata['modified'], '2013-12-01')
            assert_equal(metadata['title'], 'Peterborough  datasets')

    def test_parse_datasets(self):

        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "test_inventory.xml")

        with InventoryDocument(open(f, 'r').read()) as doc:
            dataset = doc.datasets().next()
            assert_equal(dataset['modified'], '2013-12-01')
            assert_equal(dataset['active'], True)
            assert_equal(len(dataset['resources']), 3)

    def test_validate(self):

        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "test_inventory.xml")

        with InventoryDocument(open(f, 'r').read()) as doc:
            valid, err = doc.validate()
            print err
            #assert_equal(valid, True)

    def test_parse_metadata_large(self):

        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "esdInventory_live.xml")

        with InventoryDocument(open(f, 'r').read()) as doc:
            metadata = doc.prepare_metadata()
            assert_equal(len(metadata), 5)
            assert_equal(metadata['publisher'],
                'http://opendatacommunities.org/doc/london-borough-council/redbridge')
            assert_equal(metadata['modified'], '2014-03-10')
            assert_equal(metadata['title'], 'Inventory covering a selection of London Borough of Redbridge datasets')

    def test_parse_datasets_large(self):

        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "esdInventory_live.xml")

        with InventoryDocument(open(f, 'r').read()) as doc:
            dataset = doc.datasets().next()
            assert_equal(dataset['modified'], '2013-12-19')
            assert_equal(dataset['active'], True)
            assert_equal(len(dataset['resources']), 89)

    def test_validate_large(self):

        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "esdInventory_live.xml")

        with InventoryDocument(open(f, 'r').read()) as doc:
            valid, err = doc.validate()
            print err
            #assert_equal(valid, True)
