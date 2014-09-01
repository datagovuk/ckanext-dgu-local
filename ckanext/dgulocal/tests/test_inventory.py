import os

from nose.tools import assert_equal, assert_raises

from ckanext.dgulocal.lib.inventory import InventoryDocument, InventoryXmlError


class TestInventory:

    @staticmethod
    def _get_inventory_doc(inventory_xml_filename):
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))
        filepath = os.path.join(path, inventory_xml_filename)
        return InventoryDocument(open(filepath, 'r').read())

    def test_parse_error(self):
        assert_raises(InventoryXmlError, InventoryDocument, '<tag></wrongtag>')

    def test_validation_error(self):
        assert_raises(InventoryXmlError, InventoryDocument, '<tag></tag>')

    def test_parse_metadata(self):
        doc = self._get_inventory_doc('test_inventory.xml')
        metadata = doc.top_level_metadata()
        assert_equal(len(metadata), 5)
        assert_equal(metadata['publisher'],
            'http://opendatacommunities.org/doc/unitary-authority/peterborough')
        assert_equal(metadata['modified'], '2013-12-01')
        assert_equal(metadata['title'], 'Peterborough  datasets')

    def test_parse_datasets(self):
        doc = self._get_inventory_doc('test_inventory.xml')
        dataset = doc.datasets().next()
        assert_equal(dataset['modified'], '2013-12-01')
        assert_equal(dataset['active'], True)
        assert_equal(len(dataset['resources']), 3)

    def test_parse_metadata_large(self):
        doc = self._get_inventory_doc('esdInventory_live.xml')
        metadata = doc.top_level_metadata()
        assert_equal(len(metadata), 5)
        assert_equal(metadata['publisher'],
            'http://opendatacommunities.org/doc/london-borough-council/redbridge')
        assert_equal(metadata['modified'], '2014-03-10')
        assert_equal(metadata['title'], 'Inventory covering a selection of London Borough of Redbridge datasets')

    def test_parse_datasets_large(self):
        doc = self._get_inventory_doc('esdInventory_live.xml')
        dataset = doc.datasets().next()
        assert_equal(dataset['modified'], '2013-12-19')
        assert_equal(dataset['active'], True)
        assert_equal(len(dataset['resources']), 89)

    def test_validate_large(self):
        self._get_inventory_doc('esdInventory_live.xml')
