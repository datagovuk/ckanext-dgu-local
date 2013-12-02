import logging
import uuid

import requests

from ckanext.dgulocal.lib.inventory import InventoryDocument
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)

class LGAHarvester(HarvesterBase):
    '''
    Harvesting of LGA Inventories from a single XML document provided at a
    single URL.
    '''

    def info(self):
        '''
        Returns a descriptor with information about the harvester.
        '''
        return {
                "name": "lga",
                "title": "LGA Inventory Server",
                "description": "A Local Government Authority Inventory Server"
            }


    def gather_stage(self, harvest_job):
        '''
        Fetches the single inventory document containing all of the
        datasets to be created/modified.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids

            # Create a new HarvestObject for this identifier
            #harvest_job.source.url

                    ids.append(obj.id)
        '''
        try:
            req = requests.get(harvest_job.source.url)
            e = req.raise_for_status()
            if e:
                raise e
        except Exception, e:
            self._save_gather_error(e, harvest_job)
            log.exception(e)
            return None

        # Validate XML here before we waste time later on.
        try:
            doc = InventoryDocument(req.content)
            doc.validate()
        except Exception, e:
            self._save_gather_error("Failed to load document: %s" % e, harvest_job)
            log.exception(e)
            return None

        metadata = doc.prepare_metadata()

        obj = HarvestObject(guid=unicode(uuid.uuid4()),
                           job=harvest_job,
                           content=req.content,
                           harvest_source_reference=metadata['identifier'])
        obj.save()

        return [obj.id]

    def fetch_stage(self, harvest_object):
        '''
        Check that we have content from the gather stage and just return
        success
        :returns: True if everything went right, False if errors were found
        '''
        return bool(obj.content)

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object with the inventory
        XML document and will:

          - Create/Modify a CKAN package
          - Creating the HarvestObject - Package relation (if necessary)
          - Creating and storing any suitable HarvestObjectErrors that may
            occur.
          - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''

