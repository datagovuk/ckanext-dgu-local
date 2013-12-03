import json
import logging
import uuid

import requests

from ckanext.dgulocal.lib.inventory import InventoryDocument
from ckanext.harvest.model import HarvestGatherError, HarvestJob

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
        '''
        import ckan.model as model

        try:
            req = requests.get(harvest_job.source.url)
            e = req.raise_for_status()
            if e:
                raise e
        except Exception, e:
            self._save_error(e, harvest_job)
            return None

        # Validate XML here before we waste time later on.
        try:
            doc = InventoryDocument(req.content)
            ok, err = doc.validate()
            if not ok:
                raise Exception(err)
        except Exception, e:
            self._save_error("Failed to load document: %s" % e, harvest_job)
            return None

        metadata = doc.prepare_metadata()

        # TODO: Check modified field.  If not modified since last run, no point
        # continuing.

        # Find any previous harvests and store. If modified since then continue
        # otherwise bail. Store the last process date so we can check the
        # datasets

        previous = model.Session.query(HarvestJob)\
            .filter(HarvestJob.source_id==harvest_job.source_id)\
            .filter(HarvestJob.status!='New')\
            .order_by("gather_finished desc").first()
        if previous:
            # Check if inventory job has been modified since previous
            # processing (if the processing was succesful).
            pass


        # We create a new entry for each /Inventory/Dataset within this
        # document
        ids = []
        for dataset in doc.datasets():
            obj = HarvestObject(guid=unicode(uuid.uuid4()),
                                job=harvest_job,
                                content=json.dumps(dataset),
                                harvest_source_reference=metadata['identifier'])
            obj.save()
            ids.append(obj.id)

        return ids

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
          - Creating and storing any suitable HarvestObjectErrors that may
            occur.
          - returning True if everything went as expected, False otherwise.

          The following isn't done as we are processing the inventory as a
          single document.
          --Creating the HarvestObject - Package relation (if necessary)--


        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        owner_org = harvest_object.harvest_source.publisher_id
        if not owner_orgL
            self._save_error("Unable to import without publisher", harvest_job)
            log.error(e)
            return False

        dataset = json.loads(harvest_object.content)
        package = self._find_dataset_by_identifier(dataset['identifier'], owner_org)

        # TODO: Check Modified field on dataset. Need to check against our last
        # run really to see if it was changed since then.

        #d['modified'] = node.get('Modified')

        package.title = dataset['title']
        package.notes = dataset['description']

        #d['rights'] = self._get_node_text(node.xpath('Rights'))

        # 2. Either create or modify based on 'Active'
        if not dataset['active']:
            pkg.state = 'deleted'
        else:
            pkg.state = 'active'

        # 3. Create/Modify resources based on 'Active'
        #d['resources'] = []

        # 4. Save and update harvestobj
        #model.Session.add(pkg)
        #model.Session.commit()

        return True


    def _save_error(self, message, job):
        '''
        Helper function to create an error during the gather stage.
        '''
        log.error(message)
        HarvestGatherError(message=message,job=job).save()


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
