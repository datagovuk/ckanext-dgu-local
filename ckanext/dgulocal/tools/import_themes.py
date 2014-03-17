"""
Converts the functions_services.xml file into something usable for determining
themes per function/service

The identifier specified in the first column is the ID of the function and is part
of the URI (http://id.esd.org.uk/function/XXXX) that will appear in the inventory. We
will use the functions/services to determine a collection of themes that *could* apply
to the dataset being imported, but we will need to make a decision on which should take
priority (functions or services). If we end up with two themes, they will be primary
and secondary.
"""
import collections
import csv
import json
import os
import pprint
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
INPUT = os.path.abspath(os.path.join(BASE, '..', 'data/functions_services_themes.csv'))

theme_dict = collections.defaultdict(dict)

def pretty_dict(d, indent=0):
   for key, value in d.iteritems():
      print '\t' * indent + str(key)
      if isinstance(value, dict):
         pretty_dict(value, indent+1)
      else:
         print '\t' * (indent+1) + str(value)

if __name__ == "__main__":
    reader = csv.DictReader(open(INPUT, 'r'))

    for row in reader:
        # Pull the data out of this row.  The service_id is the most specific so we will
        # use that to look up the theme if we can, using the function_id if not (or if there
        # is no service_id in the metadata)
        function_id = row.get('Identifier', '')
        service_id = row.get('Mapped identifier', '')
        themes = row.get('Theme','').split(',')

        l = theme_dict["functions"].get(function_id, [])
        s = set(l)
        for t in themes:
          s.add(t)
        theme_dict["functions"][function_id] = list(s)

        if service_id:
            l = theme_dict["services"].get(service_id, [])
            s = set(l)
            for t in themes:
              s.add(t)
            theme_dict["services"][service_id] = list(s)

    # Dump data to json file for reading at runtime.
    json.dump(theme_dict, sys.stdout)
