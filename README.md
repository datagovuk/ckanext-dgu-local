# ckanext-dgu-local


This extension provides support for harvesting from Local Authorities who follow the spec [TBC] describing their inventory data. This currently has dependencies on UK specific data, but the extension should work if the inventory document doesn't reference the UK specific publisher and temporal URLs.


## Installation

For development you should install ckanext-dgu-local as follows.

1. Install this extension as normal using pip in your activated environment:

    (pyenv) $ pip install -e "git+https://github.com/datagovuk/ckanext-dgu-local.git#egg=ckanext-dgu-local"

2. Activate the plugins by adding them to the CKAN config and then restarting CKAN:

    ckan.plugins = ...other_plugins... dgu_local lga_harvester

3. Setup the database tables:

    paster --plugin=ckanext-dgulocal dgulocal init --config=ckan_default.ini


## Plugins

`dgu_local` will provide any UI/search enhancements

`lga_harvester` is used for harvesting from the LGA defined Inventory format.


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




