import datetime
import json
import logging
import re

import requests
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.sql import update, bindparam

from ckan.plugins.core import implements
from ckan import model
import ckanext.dgu.lib.theme as dgutheme
from ckanext.dgulocal.lib.inventory import InventoryDocument, InventoryXmlError
from ckanext.harvest.model import HarvestGatherError, HarvestJob, HarvestObject, \
                                  HarvestObjectError, HarvestObjectExtra as HOExtra
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.dgulocal.lib.geo import get_boundary

log = logging.getLogger(__name__)


class LGAHarvester(HarvesterBase):
    '''
    Harvesting of LGA Inventories from a single XML document provided at a
    URL.
    '''
    implements(IHarvester)

    IDENTIFIER_KEY = 'lga_identifier'

    def info(self):
        '''
        Returns a descriptor with information about the harvester.
        '''
        return {
                "name": "lga",
                "title": "LGA Inventory XML",
                "description": "Dataset metadata published according to the Local Government Association's Inventory XML format."
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
        except requests.exceptions.RequestException, e:
            # e.g. requests.exceptions.ConnectionError
            self._save_gather_error('Failed to get content from URL: %s Error:%s %s' %
                             (harvest_job.source.url, e.__class__.__name__, e),
                             harvest_job)
            return None

        try:
            doc = InventoryDocument(req.content)
        except InventoryXmlError, e:
            self._save_gather_error("Failed to parse or validate the XML document: %s %s" %
                             (e.__class__.__name__, e), harvest_job)
            return None

        doc_metadata = doc.top_level_metadata()

        # TODO: Somehow update the publisher details with the geo boundary
        spatial_coverage_url = doc_metadata.get('spatial-coverage-url')
        if spatial_coverage_url:
            boundary = get_boundary(spatial_coverage_url)
            if boundary:
                # don't import dgulocal_model until here, to allow tests that
                # don't need postgis to run under sqlite
                from ckanext.dgulocal import model as dgulocal_model
                try:
                    dgulocal_model.set_organization_polygon(
                            harvest_job.source.publisher_id,
                            boundary)
                except Exception, e:
                    log.exception(e)
                    # but carry on anyway?

        # Find any previous harvests and store. If modified since then continue
        # otherwise bail. Store the last process date so we can check the
        # datasets
        last_modified = datetime.datetime.strptime(doc_metadata['modified'], '%Y-%m-%d').date()
        previous = model.Session.query(HarvestJob)\
            .filter(HarvestJob.source_id==harvest_job.source_id)\
            .filter(HarvestJob.status!='New')\
            .order_by("gather_finished desc").first()
        if previous:
            # Check if inventory job has been modified since previous
            # processing (if the processing was successful). We only want to
            # compare the dates
            self.last_run = previous.gather_finished.date()
            if last_modified <= self.last_run:
                log.info("Not modified {0} since last run on {1}".format(last_modified, self.last_run))
                return None

        # We create a new HarvestObject for each inv:Dataset within the
        # Inventory document
        ids = []
        for dataset in doc.datasets():
            guid = self.build_guid(doc_metadata['identifier'], dataset['identifier'])
            if previous:
                import pdb; pdb.set_trace()
            else:
                status = 'new'
            obj = HarvestObject(guid=guid,
                                job=harvest_job,
                                content=json.dumps(dataset),
                                harvest_source_reference=guid,
                                metadata_modified_date=last_modified,
                                extras=[HOExtra(key='status', value=status)],
                                )
            obj.save()
            ids.append(obj.id)

        return ids

    def fetch_stage(self, harvest_object):
        '''
        Check that we have content from the gather stage and just return
        success
        :returns: True if everything went right, False if errors were found
        '''
        # There is no fetching because all the content for the objects were got
        # in one request during the gather stage.
        return bool(harvest_object.content)

    @classmethod
    def build_guid(cls, doc_identifier, dataset_identifier):
        assert doc_identifier  # e.g. http://redbridge.gov.uk/
        assert dataset_identifier # e.g. river-levels
        return '%s/%s' % (doc_identifier, dataset_identifier)

    def get_package_dict(self, harvest_object, package_dict_defaults,
                         source_config, existing_dataset):
        '''
        Constructs a package_dict suitable to be passed to package_create or
        package_update. See documentation on
        ckan.logic.action.create.package_create for more details

        * name - a new package must have a unique name; if it had a name in the
          previous harvest, that will be in the package_dict_defaults.
        * resource.id - should be the same as the old object if updating a
          package
        * errors - call self._save_object_error() and return False
        * default values for name, owner_org, tags etc can be merged in using:
            package_dict = package_dict_defaults.merge(package_dict_harvested)
        '''
        inv_dataset = json.loads(harvest_object.content)

        pkg = dict(
            title=inv_dataset['title'],
            notes=inv_dataset['description'],
            state='active' if inv_dataset['active'] else 'deleted',
            resources=[],
            extras={'local': True},
            )
        # License
        rights = inv_dataset.get('rights')
        if rights:
            register = model.Package.get_license_register()
            if rights == 'http://www.nationalarchives.gov.uk/doc/open-government-licence/':
                pkg['license_id'] = 'uk-ogl'
            else:
                for l in register.values():
                    if l.url == rights:
                        pkg['license_id'] = l.id
                        break
                else:
                    # just save it as it is
                    pkg['license_id'] = register
                    log.info('Did not recognize license %r', register)
        else:
            pkg['license_id'] = None

        # Resources
        inv_resources = [r for r in inv_dataset['resources'] if r['active']]
        existing_resource_urls = dict((r.url, r.id)
                                      for r in existing_dataset.resources) \
                                 if existing_dataset else {}
        pkg['resources'] = []
        for inv_resource in inv_resources:
            # if it is temporal, it should be a timeseries,
            # if it is not data, it should be an additional resource
            resource_type = 'file' if inv_resource['resource_type'] == 'Data' \
                else 'documentation'
            res = {'url': inv_resource['url'],
                   'format': inv_resource['mimetype'],
                   'description': inv_resource['title'],
                   'resource_type': resource_type}
            if res['url'] in existing_resource_urls:
                res['id'] = existing_resource_urls[res['url']]

        # LGA Services and Functions
        if inv_dataset['services']:
            log.info('LGA Services: %r', inv_dataset['services'])
            # e.g. {http://id.esd.org.uk/service/190}
            pkg['extras']['lga_services'] = ' '.join(inv_dataset['services'])
        else:
            pkg['extras']['lga_services'] = ''
        if inv_dataset['functions']:
            log.info('LGA Functions %r', inv_dataset['functions'])
            pkg['extras']['lga_functions'] = ' '.join(inv_dataset['functions'])
        else:
            pkg['extras']['lga_functions'] = ''

        pkg = package_dict_defaults.merge(pkg)
        if not pkg.get('name'):
            # append the publisher name to differentiate similar titles better
            # than just a numbers suffix
            publisher = model.Group.get(harvest_object.job.source.publisher_id)
            publisher_abbrev = self._get_publisher_abbreviation(publisher)
            pkg['name'] = self.check_name(self.munge_title_to_name(
                '%s %s' % (pkg['title'], publisher_abbrev)))

        # Themes based on services/functions
        if 'tags' not in pkg:
            pkg['tags'] = []
        themes = dgutheme.categorize_package(pkg)
        log.debug('%s given themes: %r', pkg['name'], themes)
        if themes:
            pkg['extras'][dgutheme.PRIMARY_THEME] = themes[0]
            if len(themes) == 2:
                pkg['extras'][dgutheme.SECONDARY_THEMES] = '["%s"]' % themes[1]

        pkg['extras'] = self.extras_from_dict(pkg['extras'])
        return pkg

    @staticmethod
    def _get_publisher_abbreviation(publisher):
        abbrev = publisher.extras.get('abbreviation')
        if not abbrev:
            # Just look for capital letters
            abbrev = re.sub('[^A-Z]', '', publisher.title)
        return abbrev

    def _import_stage(self, harvest_object):
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

        self.create_or_update_package(package_dict, harvest_object)

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
