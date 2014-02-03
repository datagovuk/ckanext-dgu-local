# ckanext-dgu-local


This extension provides support for harvesting from Local Authorities who follow the spec [TBC] describing their inventory data. This currently has dependencies on UK specific data, but the extension should work if the inventory document doesn't reference the UK specific publisher and temporal URLs.


## Installation

For development you should install ckanext-dgu-local as follows.

```
1. Ensure virtualenv is activated, and go to src folder which holds your CKAN folder
2. git clone https://github.com/datagovuk/ckanext-dgu-local.git
3. cd ckanext-dgu-local
4. python setup.py develop
5. Add lga_harvester to the CKAN config ckan.plugins:
    ckan.plugins = ...other_plugins... lga_harvester
```

## Running tests

The tests for ckanext-dgu-local can be run using:

```
nosetests
```

from the cknext-dgu-local folder.


## Metadata

This extension relies on PackageExtras being added to packages that are created, or updated.  The Extras used the identifier field for a given publisher which is documented as being unique for that publisher (but no necessarily globally unique).

### Package Extras

|Key|Value|
|--|--|
|lga_identifier|The publisher-wide unique identifier for this dataset|
|lga_services|A JSON list of service URL/Label pairs|
|lga_functions|A JSON list of function URL/Label pairs|

### Group (publisher) Extras

|Key|Value|
|--|--|
|geo_boundary|The GEOJson describing the polygon within which this authority lives - not used in this release|




