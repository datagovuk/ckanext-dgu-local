"""
Functions to retrieve the boundaries for spatial coverage when provided
with a local authority url on statistics.data.gov.uk

e.g.
    http://statistics.data.gov.uk/doc/statistical-geography/E06000031
"""
import logging
import json

import requests
from shapely.geometry import Polygon


log = logging.getLogger(__name__)

def get_boundary(url):
    """
    Gets the geographic boundary from the specified URL which is described
    for a given authority (who each have their own URL). This data *will*
    change, but should be stored against the publisher when we harvest a
    specific inventory.

    We've some inconsistency about whether this will be the publisher URI
    or the GSS uri, so we'll support both.
    """
    actual_url = None

    if not 'statistical-geography' in url:
        log.debug("Looking up publisher", url + '.json')
        req = requests.get(url + ".json")
        if not req.ok:
            log.error("Failed to lookup publisher")
            return None

        blob = json.loads(req.content)
        actual_url = blob[0]['http://opendatacommunities.org/def/local-government/governsGSS'][0]['@id'] + ".json"
    else:
        actual_url = url + ".json"

    log.debug("Fetching Geo boundary for authority: %s", actual_url)

    req = requests.get(actual_url)
    if not req.ok:
        log.error("Failed to retrieve boundary")
        return None

    blob = json.loads(req.content)
    boundary = blob['result']['primaryTopic']['hasExteriorLatLongPolygon']

    def chunk(l):
        for i in xrange(0, len(l), 2):
            lat = l[i:i+1][0]
            lng = l[i+1:i+2][0]
            yield (float(lat), float(lng))

    poly = Polygon([c for c in chunk(boundary.strip().split(' '))])
    return poly
