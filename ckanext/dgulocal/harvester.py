import logging
import uuid

import requests

from ckanext.dgulocal.lib.inventory import InventoryDocument
from ckanext.harvest.model import HarvestGatherError

log = logging.getLogger(__name__)

class LGAHarvester(object):
    '''
    Harvesting of LGA Inventories from a single XML document provided at a
    single URL.
    '''

    IDENTIFIER_KEY = 'lga_identifier'

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
            return None

        # Validate XML here before we waste time later on.
        try:
            doc = InventoryDocument(req.content)
            ok, err = doc.validate()
            if not ok:
                raise Exception(err)
        except Exception, e:
            self._save_gather_error("Failed to load document: %s" % e, harvest_job)
            return None

        metadata = doc.prepare_metadata()

        # TODO: Check modified field.  If not modified since last run, no point
        # continuing.

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
        try:
            doc = InventoryDocument(harvest_object.content)
            # Don't validate a second time. We did this in gather.
        except Exception, e:
            self._save_import_error("Failed to load document: %s" % e, harvest_job)
            log.exception(e)
            return False

        owner_org = harvest_object.harvest_source.publisher_id
        for dataset in doc.datasets():
            package = self._find_dataset_by_identifier(dataset['identifier'], owner_org)

            # TODO: Check Modified field on dataset
            package.title = dataset['title']
            package.notes = dataset['description']

            if not dataset['active']:
                pkg.state = 'deleted'

            # 2. Either create or modify based on 'Active'
            # 3. Create/Modify resources based on 'Active'
            # 4. Save and update harvestobj


    def _save_gather_error(self, message, job):
        '''
        Helper function to create an error during the gather stage.
        '''
        err = HarvestGatherError(message=message,job=job)
        err.save()
        log.error(message)


    def _find_dataset_by_identifier(self, identifier, owner_org):
        """
        Find a package using the lga:identifier passed in, or creates a new
        package and pre-sets the identifier.
        """
        import ckan.model as model

        pkg = model.Session.query(model.Package)\
            .join(model.PackageExtra)\
            .filter(model.PackageExtra.key==LGAHarvester.IDENTIFIER_KEY)\
            .filter(model.PackageExtra.value==identifier)\
            .filter(model.Package.owner_org==owner_org)\
            .first()
        if not pkg:
            pkg = model.Package(owner_org=owner_org)
            pkg.extras[LGAHarvester.IDENTIFIER_KEY] = identifier

        return pkg
