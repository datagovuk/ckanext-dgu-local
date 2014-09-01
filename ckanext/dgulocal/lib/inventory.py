import logging
import cStringIO
import os
import HTMLParser

import lxml.etree

from ckanext.dgulocal.lib.geo import get_boundary

log = logging.getLogger(__name__)

NSMAP = {'inv': 'http://schemas.esd.org.uk/inventory'}


class InventoryXmlError(Exception):
    pass


class InventoryDocument(object):
    """
    Represents an Inventory XML document. It can be validated and parsed to
    extract its content.
    """

    def __init__(self, inventory_xml_string):
        """
        Initialize with an Inventory XML string.
        It validates it against the schema and therefore may raise
        InventoryXmlError
        """
        # Load the XSD and make sure we use it to validate the incoming
        # XML
        schema_content = self._load_schema()
        schema = lxml.etree.XMLSchema(schema_content)
        parser = lxml.etree.XMLParser(schema=schema)

        # Load and parse the Inventory XML
        xml_file = cStringIO.StringIO(inventory_xml_string)
        try:
            self.doc = lxml.etree.parse(xml_file, parser=parser)
        except lxml.etree.XMLSyntaxError, e:
            raise InventoryXmlError(unicode(e))
        finally:
            xml_file.close()

    def _load_schema(self):
        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        f = os.path.join(d, "inventory.xsd")
        return lxml.etree.parse(f)

    def top_level_metadata(self):
        """
        Extracts the top-level inv:Metadata from the XML document, and returns
        it in a dictionary.
        """
        md = {}

        root = self.doc.getroot()
        md['modified'] = root.get('Modified')
        md['identifier'] = self._get_node_text(root.xpath('inv:Identifier', namespaces=NSMAP))
        md['title'] = self._get_node_text(root.xpath('inv:Metadata/inv:Title', namespaces=NSMAP))
        md['publisher'] = self._get_node_text(root.xpath('inv:Metadata/inv:Publisher', namespaces=NSMAP))
        md['description'] = self._get_node_text(root.xpath('inv:Metadata/inv:Description', namespaces=NSMAP))
        md['spatial-coverage-url'] = spatial_coverage_url = self._get_node_text(root.xpath('inv:Metadata/inv:Coverage/inv:Spatial', namespaces=NSMAP))

        return md

    def get_spatial_coverage_boundary(self):
        if spatial_coverage_url:
            # Get the geo coverage for this publisher, and update in our db if necessary
            return get_boundary(spatial_coverage_url)

    def datasets(self):
        """
        Yields each inv:Dataset within the XML document as a dict (complete
        with resources)
        """
        for node in self.doc.xpath('/inv:Inventory/inv:Datasets/inv:Dataset', namespaces=NSMAP):
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
        d['identifier'] = self._get_node_text(node.xpath('inv:Identifier',namespaces=NSMAP))
        d['title'] = self._get_node_text(node.xpath('inv:Title',namespaces=NSMAP))
        d['modified'] = node.get('Modified')
        d['active'] = node.get('Active') in ['True', 'Yes']
        d['description'] = self._get_node_text(node.xpath('inv:Description',namespaces=NSMAP))
        d['rights'] = self._get_node_text(node.xpath('inv:Rights',namespaces=NSMAP))
        if d['rights'] == 'http://www.nationalarchives.gov.uk/doc/open-government-licence':
            d['rights'] = 'http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/'

        # Clean description to decode any encoded HTML
        h = HTMLParser.HTMLParser()
        d['description'] = h.unescape(d.get('description', ''))

        services = []
        functions = []
        svc = self._get_node_text(node.xpath('inv:Subjects/inv:Subject/inv:Service', namespaces=NSMAP))
        fn =  self._get_node_text(node.xpath('inv:Subjects/inv:Subject/inv:Function', namespaces=NSMAP))
        if svc:
            services.append(svc)
        if fn:
            functions.append(fn)

        d['services'] = services
        d['functions'] = functions
        d['resources'] = []
        for resnode in node.xpath('inv:Resources/inv:Resource', namespaces=NSMAP):
            d['resources'].extend(self._resource_to_dict(resnode))
        return d

    def _resource_to_dict(self, node):
        """
        When passed an inv:Resource node this method will flatten down all of the
        inv:Renditions into CKAN resources.
        """
        d = {}
        d['resource_type'] = node.get('Type')  # 'Document' or 'Data'

        for n in node.xpath('inv:Renditions/inv:Rendition', namespaces=NSMAP):
            d['url'] = self._get_node_text(n.xpath('inv:Identifier', namespaces=NSMAP))
            # If no active flag, default to active.
            d['active'] = n.get('Active') in ['Yes', 'True', '', None]
            d['title'] = self._get_node_text(n.xpath('inv:Title', namespaces=NSMAP))
            d['description'] = self._get_node_text(n.xpath('inv:Description', namespaces=NSMAP))
            d['mimetype'] = self._get_node_text(n.xpath('inv:MimeType', namespaces=NSMAP)) # Will become mimetype.
            yield d

