"""
Uses the service and function URLs to determine a theme for a
dataset by stripping off the ID and looking it up in the JSON
file we generated from functions_services_themes.csv and stored
in service_function_themes.json
"""
import collections
import json
import os

function_dict = None
service_dict = None


def _strip_id(url):
    # id.esd.org.uk/function/2
    return url.split('/')[-1]

def theme_lookup(service_url, function_url):
    themes = []

    global function_dict
    global service_dict

    if not function_dict:
        # Loads the mapping if it hasn't yet been loaded
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        path = os.path.join(path, "service_function_themes.json")
        theme_dict = json.load(open(path, 'r'))
        function_dict = theme_dict.get('functions', {})
        service_dict =  theme_dict.get('services', {})

    if function_url:
        function_id = _strip_id(function_url)
        themes.extend(function_dict.get(function_id, ''))

    if service_url:
        service_id = _strip_id(service_url)
        themes.extend(service_dict.get(service_id, ''))

    # We should order by the number of times the string appears in the list
    d = collections.Counter([t for t in themes if t])
    return [t[0] for t in d.most_common(2)]
