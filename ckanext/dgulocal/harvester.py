import collections
import datetime
import json
import logging
import uuid

import requests
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.sql import update, bindparam

from ckan.plugins.core import SingletonPlugin, implements
import ckanext.dgu.lib.theme as dgutheme
from ckanext.dgulocal.lib.inventory import InventoryDocument
from ckanext.harvest.model import HarvestGatherError, HarvestJob, HarvestObject, \
                                  HarvestObjectError
from ckanext.harvest.interfaces import IHarvester

from ckan.lib.munge import munge_title_to_name,substitute_ascii_equivalents

log = logging.getLogger(__name__)

db_srid = 4326

def set_organization_polygon(orgid, geojson):
    import ckan.model as model
    from geoalchemy import WKTSpatialElement
    from shapely.geometry import asShape
    from ckanext.dgulocal.model import OrganizationExtent

    if not orgid:
        log.debug("No organization provided")
        return

    shape = asShape(geojson)
    extent = model.Session.query(OrganizationExtent)\
        .filter(OrganizationExtent.organization_id == orgid).first()
    if not extent:
        extent = OrganizationExtent(organization_id=orgid)
    extent.the_geom=WKTSpatialElement(shape.wkt, db_srid)
    extent.save()


class LGAHarvester(SingletonPlugin):
    '''
    Harvesting of LGA Inventories from a single XML document provided at a
    URL.
    '''
    implements(IHarvester)

    IDENTIFIER_KEY = 'lga_identifier'

    def _gen_new_name(self,title):
        '''
        Creates a URL friendly name from a title
        '''
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        return name

    def _check_name(self,name):
        '''
        Checks if a package name already exists in the database, and adds
        a counter at the end if it does exist.
        '''
        import ckan.model as model

        like_q = u'%s%%' % name
        pkg_query = model.Session.query(model.Package).filter(model.Package.name.ilike(like_q)).limit(100)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 101:
                if name+str(counter) not in taken:
                    return name+str(counter)
                counter = counter + 1
            return None

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

        self.last_run = None

        log.debug('Resolving source: %s', harvest_job.source.url)
        try:
            req = requests.get(harvest_job.source.url)
            e = req.raise_for_status()
            if e:
                raise e
        except Exception, e:
            # e.g. requests.exceptions.ConnectionError
            self._save_gather_error('Failed to get content from URL: %s Error:%s %s' %
                             (harvest_job.source.url, e.__class__.__name__, e),
                             harvest_job)
            return None

        try:
            doc = InventoryDocument(req.content)
            ok, err = doc.validate()
            if not ok:
                raise Exception(err)
        except Exception, e:
            self._save_gather_error("Failed to load document: %s %s" %
                             (e.__class__.__name__, e), harvest_job)
            return None

        metadata = doc.prepare_metadata()

        # TODO: Somehow update the publisher details with the geo boundary
        if metadata.get('spatial-coverage'):
            try:
                set_organization_polygon(harvest_job.source.publisher_id, metadata['spatial-coverage'])
            except Exception, e:
                log.exception(e)

        # Find any previous harvests and store. If modified since then continue
        # otherwise bail. Store the last process date so we can check the
        # datasets
        previous = model.Session.query(HarvestJob)\
            .filter(HarvestJob.source_id==harvest_job.source_id)\
            .filter(HarvestJob.status!='New')\
            .order_by("gather_finished desc").first()
        if previous:
            # Check if inventory job has been modified since previous
            # processing (if the processing was succesful). We only want to
            # compare the dates
            self.last_run = previous.gather_finished.date()
            last_modified = datetime.datetime.strptime(metadata['modified'], '%Y-%m-%d').date()
            if last_modified <= self.last_run:
                log.info("Not modified {0} since last run on {1}".format(last_modified, self.last_run))
                return None

        # We create a new entry for each /Inventory/Dataset within this
        # document
        ids = []
        for dataset in doc.datasets():
            obj = HarvestObject(guid=unicode(uuid.uuid4()),
                                job=harvest_job,
                                content=json.dumps(dataset),
                                harvest_source_reference=dataset['identifier'],
                                metadata_modified_date=last_modified)
            obj.save()
            ids.append(obj.id)

        return ids

    def fetch_stage(self, harvest_object):
        '''
        Check that we have content from the gather stage and just return
        success
        :returns: True if everything went right, False if errors were found
        '''
        return bool(harvest_object.content)

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
        import ckan.model as model

        owner_org = harvest_object.source.publisher_id
        if not owner_org:
            self._save_object_error('Unable to import without publisher (object %s)' % harvest_object.id, harvest_object, 'Import')
            log.error(e)
            return False

        dataset = json.loads(harvest_object.content)
        package,found = self._find_dataset_by_identifier(dataset['identifier'], owner_org)

        # Check Modified field on dataset. Need to check against our last
        # run really to see if it was changed since then.
        #if self.last_run:
        #    last_modified = datetime.datetime.strptime(dataset['modified'], '%Y-%m-%d').date()
        #    if last_modified <= self.last_run:
        #        log.info("Dataset not modified since last run on {0}".format(self.last_run))
        #        return False

        package.owner_org = owner_org
        package.title = dataset['title']
        package.notes = dataset['description']

        if not found:
            # If it was found, we already have a name
            package.name = self._check_name(self._gen_new_name(package.title))

        # Set the state based on what the inventory claims
        log.debug('Received data: %r', dataset)
        if not dataset['active']:
            package.state = 'deleted'
        else:
            package.state = 'active'

        # License
        register = model.Package.get_license_register()
        for l in register.values():
            if l.url == dataset.get('rights'):
                package.license_id = l.id
                break

        # 3. Create/Modify resources based on 'Active'
        resources = [r for r in dataset['resources'] if r['active']]
        resource_urls = [r['url'] for r in dataset['resources'] if r['active']]
        for resource in package.resources:
            resource.state = 'deleted'

        for resource in resources:
            # if it is temporal, it should be a timeseries,
            # if it is not data, it should be an additional resource
            # otherwise it is data
            package.add_resource(resource['url'], format=resource['mimetype'],
                description=resource['name'], resource_type=resource['resource_type'])

        # Add LGA Services and Functions
        # Set themes based on services/functions
        if dataset['services']:
            log.info("LGA Services: %r", dataset['services'])
            # e.g. {http://id.esd.org.uk/service/190}
            package.extras['lga_services'] = ' '.join(dataset['services'])
        else:
            package.extras['lga_services'] = ''
        if dataset['functions']:
            log.info("LGA Functions %r", dataset['functions'])
            package.extras['lga_functions'] = ' '.join(dataset['functions'])
        else:
            package.extras['lga_functions'] = ''

        # Boilerplate for harvesters
        extras = {
            'import_source': 'harvest',
            'harvest_object_id': harvest_object.id,
            'harvest_source_reference': harvest_object.harvest_source_reference,
            'metadata-date': harvest_object.metadata_modified_date.strftime('%Y-%m-%d'),
        }
        for key, value in extras.items():
            package.extras[key] = value

        # Package will be categorised by the presence of functions or services
        # and will default to keyword matching in an attempt to properly
        # categorise into a theme.
        themes = dgutheme.categorize_package(package)
        log.debug('%s given themes: %r', package.name, themes)
        if themes:
            package.extras[dgutheme.PRIMARY_THEME] = themes[0]
            if len(themes) == 2:
                package.extras[dgutheme.SECONDARY_THEMES] = '["%s"]' % themes[1]

        package.extras['local'] = True

        # 4. Save and update harvestobj, we need a pkg id though
        # harvest_object.package_id = pkg.id
        log.info("Creating package: %s" % package.name)
        model.repo.new_revision()
        model.Session.add(harvest_object)
        model.Session.add(package)
        model.Session.commit()

        # Flag the other objects of this source as not current anymore
        from ckanext.harvest.model import harvest_object_table
        u = update(harvest_object_table) \
                .where(harvest_object_table.c.package_id==bindparam('b_package_id')) \
                .values(current=False)
        model.Session.execute(u, params={'b_package_id':package.id})
        model.Session.commit()

        # Refresh current object from session, otherwise the
        # import paster command fails
        model.Session.remove()
        model.Session.add(harvest_object)
        model.Session.refresh(harvest_object)

        # Set reference to package in the HarvestObject and flag it as
        # the current one
        if not harvest_object.package_id:
            harvest_object.package_id = package.id

        harvest_object.current = True
        harvest_object.save()

        return True


    def _save_gather_error(self, message, job):
        '''
        Helper function to create an error during the gather stage.
        '''
        log.error(message)
        HarvestGatherError(message=message,job=job).save()


    def _save_object_error(self, message, obj, stage=u'Fetch'):
        import ckan.model as model
        err = HarvestObjectError(message=message, object=obj, stage=stage)
        try:
            err.save()
        except InvalidRequestError:
            model.Session.rollback()
            err.save()
        finally:
            # No need to alert administrator so don't log as an error, just info
            log.info(message)


    def _find_dataset_by_identifier(self, identifier, owner_org):
        """
        Find a package using the lga:identifier passed in, or creates a new
        package and pre-sets the identifier.
        """
        import ckan.model as model
        found = True
        pkg = model.Session.query(model.Package)\
            .join(model.PackageExtra)\
            .filter(model.PackageExtra.key==LGAHarvester.IDENTIFIER_KEY)\
            .filter(model.PackageExtra.value==identifier)\
            .filter(model.Package.owner_org==owner_org)\
            .first()
        if not pkg:
            pkg = model.Package(owner_org=owner_org)
            pkg.extras[LGAHarvester.IDENTIFIER_KEY] = identifier
            found = False

        return pkg, found
