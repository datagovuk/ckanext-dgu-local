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
    """
    actual = url + ".json"

    log.debug("Fetching Geo boundary for authority")

    req = requests.get(actual)
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

    x = [c for c in chunk(boundary.strip().split(' '))]
    poly = Polygon([c for c in chunk(boundary.strip().split(' '))])
    return poly
