
import os, sys, textwrap, ckanapi
import fire

from datetime import datetime, timedelta
from pprint import pprint
from json import loads, dumps
import json
from collections import OrderedDict
from parameters.local_parameters import SETTINGS_FILE, PATH


def print_table(rs):
    template = "{{:<6.6}}... {{:<15.15}}  {{:<10.10}}  {}  {{:<12}}"
    fmt = template.format("{:>7.9}")
    print(fmt.format("", "", "", "", ""))
    print(fmt.format("id","resource","package", "publisher","rows"))
    print("=========================================================================")
    #fmt = template.format("{:>7.1f}")
    for r in rs:
        print(fmt.format(r['resource_id'],r['resource_name'],
            r['package_name'], r['organization'],
            r['rows']))
    print("=========================================================================\n")

def load_resources_from_file():
    resources_filepath = PATH+"/resources.json"
    if os.path.exists(resources_filepath):
        with open(resources_filepath,'r') as f:
        #    pprint(f.read())
            resources = loads(f.read())
        return resources
    else:
        return []

def load_packages_from_file():
    packages_filepath = PATH+"/packages.json"
    if os.path.exists(packages_filepath):
        with open(packages_filepath,'r') as f:
        #    pprint(f.read())
            packages = loads(f.read())
        return packages
    else:
        return []

def store_resources_as_file(xs):
    with open(PATH+"/resources.json",'w') as f:
        f.write(dumps(xs, indent=4))

def store(packages):
    with open(PATH+"/packages.json",'w') as f:
        f.write(dumps(packages, indent=4))

def query_resource(site,query,API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data

def load_resource(site,resource_id,API_key):
    data = query_resource(site,  'SELECT * FROM "{}"'.format(resource_id), API_key)
    return data

def load_resource_archive(site,API_key):
    rarid = resource_archive_resource_id = "fill-this-in"
    data = query_resource(site,  'SELECT * FROM "{}"'.format(rarid), API_key)
    return data

def get_number_of_rows(site,resource_id,API_key=None):
# On other/later versions of CKAN it would make sense to use
# the datastore_info API endpoint here, but that endpoint is
# broken on WPRDC.org.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=1) # The limit
        # must be greater than zero for this query to get the 'total' field to appear in
        # the API response.
        count = results_dict['total']
    except:
        return None

    return count

def get_schema(site,resource_id,API_key=None):
    # In principle, it should be possible to do this using the datastore_info
    # endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
    except:
        return None

    return schema

def extract_features(package,resource):
    if resource['format'] in ['CSV','csv','.csv']: #'XLSX','XLS']:
        rows = get_number_of_rows(site,resource['id'],API_key)
        schema = get_schema(site,resource['id'],API_key)
        if schema is None:
            columns = None
        else:
            columns = len(schema)
    else:
        rows = columns = None
    if 'name' not in resource:
        resource_name = "Unnamed resource" # This is how CKAN labels such resources.
    else:
        resource_name = resource['name']
    r_tuples = [('resource_name',resource_name),
        ('resource_id',resource['id']),
        ('package_name',package['title']),
        ('package_id',resource['package_id']),
        ('organization',package['organization']['title']),
        ('first_published',None),
        ('first_seen',datetime.now().isoformat()),
        ('last_seen',datetime.now().isoformat()),
        ('total_days_seen',1),
        ('rows',rows),
        ('columns',columns),
        ('size',resource['size']),
        ('format',resource['format'])]

    return OrderedDict(r_tuples)

def update(record,x):
    assert record['resource_id'] == x['resource_id']
    assert record['package_id'] == x['package_id']
    modified_record = OrderedDict(record)
    last_seen = datetime.strptime(record['last_seen'],"%Y-%m-%dT%H:%M:%S.%f")
    now = datetime.now()
    modified_record['last_seen'] = now.isoformat()
    if last_seen.date() != now.date:
        modified_record['total_days_seen'] += 1

    # Update row counts, column counts, etc.
    return modified_record

def inventory(packages=False):
    ckan = ckanapi.RemoteCKAN(site) # Without specifying the apikey field value,
    # the next line will only return non-private packages.
    packages = ckan.action.current_package_list_with_resources(limit=999999) 
    # This is a list of all the packages with all the resources nested inside and all the current information.
   
#    old_data = load_resource_archive(site,API_key)
    old_data = load_resources_from_file()
    old_resource_ids = [r['resource_id'] for r in old_data]
    resources = []
    list_of_odicts = []
    new_rows = []
    print("=== Printing resources with non-standard formats ===")
    for p in packages:
        resources += p['resources']
        for r in p['resources']:
            new_row = extract_features(p,r)
            list_of_odicts.append(new_row)
            if new_row['format'] in ['.csv','csv','',' ','.html','html','.xlsx','.zip','.xls',None,'None','pdf','.pdf']:
                print("{}: {}".format(new_row['resource_name'],new_row['format']))
            #if new_row['resource_id'] not in old_resource_ids:
            #    old_data.append(new_row)
            #else:
   
    merged = [] 
    processed_new_ids = []
    new_rows = list_of_odicts
    print("len(new_rows) = {}".format(len(new_rows)))
    new_resource_ids = [r['resource_id'] for r in new_rows]
    for datum in old_data:
        old_id = datum['resource_id']
        if old_id not in new_resource_ids:
            print("New resource: {} | {} | {}".format(old_id,datum['resource_name'],datum['organization']))
            merged.append(datum)
        else: # A case where an existing record needs to be 
        # updated has been found.
            x = new_rows[new_resource_ids.index(old_id)]
            print("old_id = {}, x['resource_id'] = {}".format(old_id, x['resource_id']))
            modified_datum = update(datum,x)
            merged.append(modified_datum)
            processed_new_ids.append(old_id)

    print("len(processed_new_ids) = {}".format(len(processed_new_ids)))
    for new_row in new_rows:
        if new_row['resource_id'] not in processed_new_ids:
            # These are new resources that haven't ever been added or tracked.
            merged.append(new_row)
                
    store_resources_as_file(merged)
    print("{} currently has {} datasets and {} resources.".format(site,len(packages),len(resources)))


    #wobbly_ps_sorted = sorted(wobbly_plates, 
    #                        key=lambda u: -u['cycles_late'])
    #print("\nPlates by Wobbliness: ")
    #print_table(wobbly_ps_sorted)

server = "test-production"
with open(SETTINGS_FILE) as f:
    settings = json.load(f)
    API_key = settings["loader"][server]["ckan_api_key"]
    site = settings["loader"][server]["ckan_root_url"]
    package_id = settings["loader"][server]["package_id"]

if __name__ == '__main__':
    if len(sys.argv) == 1:
        inventory() # Make this the default.
    else:
        fire.Fire()
