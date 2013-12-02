import logging
import json
import cStringIO

import lxml.etree

log = logging.getLogger(__name__)


class InventoryDocument(object):
    """
    Controls a downloaded inventory document for a local authority and
    allows for validation and retrieval of the content in more useful
    forms.
    """


    def __init__(self, content):
        """
        Creates the XML document from the provided content. This may
        break and so handling of errors is expected by the caller.
        """
        self.data =  cStringIO.StringIO(content)
        self.doc = lxml.etree.parse(self.data)

    def validate(self):
        """
        Returns true (and empty string) if this document validates according
        to the XSD it claims to follow. If it fails to validate, False and
        an error string will be returned.
        """
        return True, ""

    def prepare_metadata(self):
        """
        Retrieves the metadata from the document, and returns it in a
        dictionary.
        """
        md = {}

        root = self.doc.getroot()
        md['modified'] = root.get('Modified')
        md['title'] = self._get_node_text(root.xpath('Title'))
        md['publisher'] = self._get_node_text(root.xpath('Publisher'))
        md['description'] = self._get_node_text(root.xpath('Description'))

        return md


    def datasets(self):
        """
        Yields all of the datasets within the document as dictionaries (each
        of which contain the attached resources)
        """
        for node in self.doc.xpath('/Inventory/Datasets/Dataset'):
            yield self._dataset_to_dict(node)

    def _get_node_text(self, node, default=''):
        """
        Retrieves the text from the result of an xpath query, or
        the default value.
        """
        if node:
            return node[0].text
        return default

    def _dataset_to_dict(self, node):
        """
        Converts a Dataset node to a dictionary, complete with the resources
        """
        d = {}
        d['identifier'] = node.get('Identifier') # TODO: This will become child
        d['modified'] = node.get('Modified')
        d['active'] = node.get('Active') == 'Yes'
        d['description'] = self._get_node_text(node.xpath('Description'))
        d['rights'] = self._get_node_text(node.xpath('Rights'))
        d['resources'] = []
        for resnode in node.xpath('Resources/Resource'):
            d['resources'].extend(self._resource_to_dict(resnode))
        return d

    def _resource_to_dict(self, node):
        """
        When passed a Resource node this method will flatten down all of the
        renditions into CKAN resources.
        """
        d = {}
        # TODO: Add resource_type to each rendition based on the containing
        # resource
        for n in node.xpath('Renditions/Rendition'):
            d['url'] = n.get('Identifier')
            d['active'] = n.get('Active') == 'Yes'
            d['title'] = self._get_node_text(n.xpath('Title'))
            d['description'] = self._get_node_text(n.xpath('Description'))
            d['mimetype'] = self._get_node_text(n.xpath('Format')) # Will become mimetype.
            yield d


    def __enter__(self):
        """
        Helpers to allow for using this class as a context manager
        """
        return self

    def __exit__(self, type, value, traceback):
        """
        Cleanup when the context manager closes.
        """
        self.data.close()
        del self.doc

