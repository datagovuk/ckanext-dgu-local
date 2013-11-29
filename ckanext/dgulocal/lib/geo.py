"""
Functions to retrieve the boundaries for spatial coverage when provided
with a local authority url on statistics.data.gov.uk

e.g.
    http://statistics.data.gov.uk/doc/statistical-geography/E06000031
"""
import logging
import json

import requests
import geojson


log = logging.getLogger(__name__)

def get_boundary(url):
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

    poly = geojson.Polygon([c for c in chunk(boundary.strip().split(' '))])
    return geojson.dumps(poly, sort_keys=True)
