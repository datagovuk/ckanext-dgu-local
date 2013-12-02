"""
This plugin provides an entrypoint for all of the functionality in the
DGU Local Authority functionality which includes:

    * Harvesting datasets from local authority hosted servers.
    * Navigation to LA publishers via map/postcode lookup.
    * Custom schemas for the various schema defined by esd.

"""

import os
from logging import getLogger

from pylons import config

from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IRoutes
from ckan.plugins import IConfigurer
from ckan.plugins import ITemplateHelpers
from ckan.plugins import IAuthFunctions
from ckan.plugins import IActions
from ckan.plugins import IActions
import ckan.plugins.toolkit as toolkit
from ckan.config.routing import SubMapper

log = getLogger(__name__)


class LocalPlugin(SingletonPlugin):
    implements(IRoutes, inherit=True)
    implements(IActions)
    implements(IAuthFunctions, inherit=True)
    implements(IConfigurer)

    def after_map(self, map):
        return map

    def update_config(self, config):
        pass

    def before_map(self, map):
        return map

    def get_auth_functions(self):
        return {
        }

    def get_actions(self):
        return {}
