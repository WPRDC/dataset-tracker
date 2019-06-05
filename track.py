import os, sys, re, csv, time, itertools, textwrap, ckanapi, random, math
import requests, time, traceback
import shutil
import fire

from datetime import datetime, timedelta
from dateutil import parser
from json import loads, dumps
import json
from collections import OrderedDict, defaultdict
from parameters.local_parameters import SETTINGS_FILE, PATH, BACKUP_DATA
from notify import send_to_slack
from backup_util import backup_to_disk

#abspath = os.path.abspath(__file__)
#dname = os.path.dirname(abspath)
#os.chdir(dname)
#local_path = dname+"/latest_pull"
## If this path doesn't exist, create it.
#if not os.path.exists(local_path):
#    os.makedirs(local_path)
sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl

from marshmallow import fields, pre_load, post_load

class ResourceTrackingSchema(pl.BaseSchema):
    resource_id = fields.String(allow_none=False)
    resource_name = fields.String(allow_none=False)
    package_id = fields.String(allow_none=False)
    package_name = fields.String(allow_none=False)
    linking_code = fields.String(allow_none=False)
    organization =  fields.String(allow_none=False)
    resource_url = fields.String(allow_none=False)
    package_url = fields.String(allow_none=False)
    download_url = fields.String(allow_none=True) # 'url' parameter of the resource.
    download_link_status = fields.String(allow_none=True)
    created = fields.DateTime(allow_none=True)
    first_published = fields.DateTime(allow_none=True)
    first_seen = fields.DateTime(default=datetime.now().isoformat(),allow_none=True)
    last_seen = fields.DateTime(dump_only=True,dump_to='last_seen',default=datetime.now().isoformat())
    total_days_seen = fields.Integer(allow_none=True)
    last_modified = fields.DateTime(allow_none=True)
    time_of_last_size_change = fields.DateTime(allow_none=True)
    active = fields.Boolean(allow_none=True)
    rows = fields.Integer(allow_none=True)
    columns = fields.Integer(allow_none=True)
    size = fields.Integer(allow_none=True)
    last_sized = fields.DateTime(allow_none=True)
    _format = fields.String(dump_to='format',allow_none=False)
    loading_method = fields.String(allow_none=True)
    tags = fields.String(allow_none=True)
    groups = fields.String(allow_none=True)


    # Never let any of the key fields have None values. It's just asking for
    # multiplicity problems on upsert.
    #as_of = fields.DateTime(dump_only=True,dump_to='as_of',default=datetime.datetime.now().isoformat())

    # [Note that since this script is taking data from CSV files, there should be no
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as as a zero-length string, which this script
    # is then responsible for converting into something more appropriate.

    class Meta:
        ordered = True

    # From the Marshmallow documentation:
    #   Warning: The invocation order of decorated methods of the same
    #   type is not guaranteed. If you need to guarantee order of different
    #   processing steps, you should put them in the same processing method.
    #@pre_load
    #def fix_date(self, data):
    #    data['first_published'] = datetime.strptime(data['first_published'], "%Y-%m-%dT%H:%M:%S.%f").isoformat()
    #    data['first_seen'] = datetime.strptime(data['first_seen'], "%Y-%m-%dT%H:%M:%S.%f").isoformat()
    #    data['last_seen'] = datetime.strptime(data['last_seen'], "%Y-%m-%dT%H:%M:%S.%f").isoformat()
    @pre_load
    def fix_name(self, data):
        if data['resource_name'] is None:
            data['resource_name'] = "Unnamed resource"

class PackageTrackingSchema(pl.BaseSchema):
    package_id = fields.String(allow_none=False)
    package_name = fields.String(allow_none=False)
    organization =  fields.String(allow_none=False)
    package_url = fields.String(allow_none=False)
    active = fields.Boolean(allow_none=True)
    tags = fields.String(allow_none=True)
    groups = fields.String(allow_none=True)
    resources_last_modified = fields.String(allow_none=True)
    # Never let any of the key fields have None values. It's just asking for
    # multiplicity problems on upsert.
    #as_of = fields.DateTime(dump_only=True,dump_to='as_of',default=datetime.datetime.now().isoformat())

    # [Note that since this script is taking data from CSV files, there should be no
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as as a zero-length string, which this script
    # is then responsible for converting into something more appropriate.

    class Meta:
        ordered = True

    # From the Marshmallow documentation:
    #   Warning: The invocation order of decorated methods of the same
    #   type is not guaranteed. If you need to guarantee order of different
    #   processing steps, you should put them in the same processing method.
    #@pre_load
    #def fix_date(self, data):
    #    data['first_published'] = datetime.strptime(data['first_published'], "%Y-%m-%dT%H:%M:%S.%f").isoformat()
    #    data['first_seen'] = datetime.strptime(data['first_seen'], "%Y-%m-%dT%H:%M:%S.%f").isoformat()
    #    data['last_seen'] = datetime.strptime(data['last_seen'], "%Y-%m-%dT%H:%M:%S.%f").isoformat()
    @pre_load
    def fix_name(self, data):
        if data['resource_name'] is None:
            data['resource_name'] = "Unnamed resource"


period = {'Annually': timedelta(days = 366),
        'Bi-Annually': timedelta(days = 183),
        'Quarterly': timedelta(days = 31+30+31),
        'Monthly': timedelta(days = 31),
        'Bi-Monthly': timedelta(days = 16),
        'Weekly': timedelta(days = 7),
        'Bi-Weekly': timedelta(days = 4),
        'Daily': timedelta(days = 1),
        'Hourly': timedelta(hours = 1),
        'Multiple Times per Hour': timedelta(minutes=30)}
nonperiods = ['', 'As Needed', 'Not Updated (Historical Only)']
# Some datasets are showing up as stale for one day because
# (for instance) the County doesn't post jail census data
# on a given day to their FTP server; our ETL script runs
# but it doesn't update the metadata_modified.

# One better solution to this would be to create a package-
# (and maybe also resource-) level metadata field called
# etl_job_last_ran.

# For now, I'm hard-coding in a few exceptions.
extensions = {'d15ca172-66df-4508-8562-5ec54498cfd4': {'title': 'Allegheny County Jail Daily Census',
                'extra_time': timedelta(days=1),
                'actual_data_source_reserve': timedelta(days=15)},
              '046e5b6a-0f90-4f8e-8c16-14057fd8872e': {'title': 'Police Incident Blotter (30 Day)',
                'extra_time': timedelta(days=1)}
            }

def pause(delay=None):
    if delay is None:
        time.sleep(0.5)
    else:
        time.sleep(delay)

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def smart_pprint(x):
    from pprint import pprint
    try:
        pprint(x)
    except UnicodeEncodeError:
        # There's some annoying non-ASCII character in whatever we're trying to print.
        string_representation = x.__str__
        ascii_string = ''
        ascii_set = '~1234567890-=qwertyuiop[]\\asdfghjkl;zxcvbnm,./~!@$%^&*()_+QWERTYUIOP{}|ASDFGHJKL:"ZXCVBNM<>?'
        ascii_set += '\#'
        ascii_set += "'"
        for c in string_representation:
            if c in ascii_set:
                ascii_string += c
            else:
                ascii_string += '?'
        print(ascii_string)

        # We could also try print(x)

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

def pluralize(word,xs,count=None):
    if xs is not None:
        count = len(xs)
    return "{} {}{}".format(count,word,'' if count == 1 else 's')

def get_resources_filepath(server):
    return "{}/resources-{}.json".format(PATH,server)

def get_packages_filepath(server):
    return "{}/packages-{}.json".format(PATH,server)

def load_xs_from_file(server,filepath):
    if os.path.exists(filepath):
        with open(filepath,'r',encoding='utf-8') as f:
            xs = loads(f.read())
        # Also back up this file, so that any changes can be easily undone.
        backup_filepath = '/'.join(filepath.split('/')[:-1] + ['backup.json'])
        shutil.copy(filepath, backup_filepath)

        return xs
    else:
        return []

def load_resources_from_file(server):
    resources_filepath = get_resources_filepath(server)
    return load_xs_from_file(server,resources_filepath)

def load_packages_from_file(server):
    packages_filepath = get_packages_filepath(server)
    return load_xs_from_file(server,packages_filepath)

def store_xs_as_file(xs,designation,filepath,Schema,field_names_seed = None,filename_override=None):
    with open(filepath,'w',encoding='utf-8') as f:
        f.write(dumps(xs, indent=4))

    if field_names_seed is not None:
        field_names = field_names_seed
    else:
        # Make sure to get every possible field name (since some
        # JSON rows lack some fields).
        ckan_fields = Schema().serialize_to_ckan_fields()
        ordered_fields = [cf['id'] for cf in ckan_fields]
        # Get order of field names from schema.
        field_names = ordered_fields

        #print(set(extracted_field_names) - set(ordered_fields)) # Currently 'comments' is the only
        # field that exists in the JSON file but is not present in the schema.
        # [ ] We could take these leftover field names and tack them on to the end of
        # the field_names list in alphabetical order.
    if filename_override is None:
        target = "{}/{}.csv".format(PATH,designation)
    else:
        target = "{}/{}.csv".format(PATH,filename_override)
    write_to_csv(target,xs,field_names)
    print("Just wrote {} rows to {} using these field names: {}".format(len(xs),target,field_names))

def store_packages_as_file(ps,server,field_names_seed=None,filename_override=None):
    if filename_override is None:
        packages_filepath = get_packages_filepath(server)
    else:
        packages_filepath = "{}/{}.json".format(PATH,filename_override)

    store_xs_as_file(ps,'packages',packages_filepath,
            PackageTrackingSchema,field_names_seed,filename_override)

def store_resources_as_file(rs,server,field_names_seed=None):
    resources_filepath = get_resources_filepath(server)
    store_xs_as_file(rs,'resources',resources_filepath,
            ResourceTrackingSchema)

    tracked_packages_dict = {}

    last_modified_by_package = defaultdict(list)
    for r in rs:
        package_id = r['package_id']
        fields_to_extract = ['package_id', 'package_name',
                'organization', 'tags', 'groups', 'active',
                'package_url']
        # loading_method will take some more work to convert.
        tp = {}

        if 'last_modified' in r:
            r_last_modified = r['last_modified']
            print("r_last_modified = {}".format(r_last_modified))
            if r_last_modified is not None:
                try:
                    r_last_modified_date = parser.parse(r_last_modified)
                    last_modified_by_package[package_id].append(r_last_modified_date)
                    print("added r_last_modified_date: {}".format(r_last_modified_date))
                except AttributeError:
                    print("Unable to parse {} as a date/datetime.".format(r_last_modified))

        for f in fields_to_extract:
            if f in r:
                tp[f] = r[f]

        if package_id not in tracked_packages_dict.keys():
            tracked_packages_dict[package_id] = tp
        else:
            for f in fields_to_extract:
                if f in r:
                    if type(r[f]) == bool:
                        if f in tracked_packages_dict[package_id]:
                            tracked_packages_dict[package_id][f] = r[f] or tracked_packages_dict[package_id][f]
                        else:
                            tracked_packages_dict[package_id][f] = r[f]
                    elif f not in tracked_packages_dict[package_id]:
                        tracked_packages_dict[package_id][f] = r[f]
                    elif 'active' in r and r['active']: # Overwrite package values when looking at an active resource.
                        tracked_packages_dict[package_id][f] = r[f]

    tracked_packages = [tp for tp in tracked_packages_dict.values()]
    for tp in tracked_packages:
        p_id = tp['package_id']
        if p_id in last_modified_by_package:
            most_recent_date = max(last_modified_by_package[p_id])
            tp['resources_last_modified'] = most_recent_date.isoformat()
            print("tp['resources_last_modified'] = {}".format(tp['resources_last_modified']))
        else:
            tp['resources_last_modified'] = None

    store_packages_as_file(tracked_packages,server)

def store(rs,server):
    return store_resources_as_file(rs,server)

def load(server):
    return load_resources_from_file(server)


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
    data = query_resource(site, 'SELECT * FROM "{}"'.format(resource_id), API_key)
    return data

def load_resource_archive(site,API_key):
    rarid = resource_archive_resource_id = "fill-this-in"
    data = query_resource(site, 'SELECT * FROM "{}"'.format(rarid), API_key)
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
        pause()
    except:
        print("get_number_of_rows threw an exception. Returning 'None'.")
        return None

    print("  get_number_of_rows: count = {}".format(count))
    return count

def get_schema(site,resource_id,API_key=None):
    # On later versions of CKAN, it should be possible to do this using the
    # datastore_info endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
        pause()
    except:
        return None

    return schema

def sort_and_stringify_field(p,field,header):
    # Sorting before converting fields to a string is a good idea because
    # if you don't do it, the fields wind up strung together in a random
    # order (because this function uses sets to reduce a list to just the
    # unique values), making cross-file comparisons difficult.
    field_string = ''
    if field in p:
        xs = p[field]
        field_string = '|'.join(sorted(set([x[header] for x in xs])))
    return field_string

def sort_and_stringify_groups(p):
    return sort_and_stringify_field(p,'groups','title')

def name_of_resource(resource):
    if 'name' not in resource:
        return "Unnamed resource" # This is how CKAN labels such resources.
    else:
        return resource['name']

def download_url_of_resource(resource):
    return resource['url'] if 'url' in resource else None

def size_estimate(resource,old_tracks,force_sizing=False):
    """Returns the size of the resource, when it's possible to determine it from the response
    to the HEAD call. Otherwise, it randomly decides whether to download the file and determine
    the size that way. Else, it returns None (which it also does if it can't determine the
    file size for some other reason).

    Thus, if this function is run to update the size, even if None is returned, it seems
    reasonable to update the last_sized timestamp.

    The second parameter returned indicates whether the function attempted to size the resource.
    """
    if resource['format'] in ['HTML','html']:
        r_name = name_of_resource(resource)
        download_url = download_url_of_resource(resource)
        #print("Not saving a file size estimate since {} ({}) appears to be a link to a web page".format(r_name,download_url))
        return None, True
    if 'url' in resource:
        url = resource['url']
        resource_id = resource['id'] # 'id', not 'resource_id'
                  # since this is still the raw CKAN response.
    elif 'download_url' in resource:
        url = resource['download_url']
        resource_id = resource['resource_id']
    else:
        return None, True

    if url == 'http://#':   # Handle local convention for
        return None, True  # disabling downloading of big
                            # tables in the datastore.

    try:
        response = requests.head(url,timeout=60)
        pause()
    except requests.exceptions.Timeout:
        print("Timed out while getting the head from {}".format(url))
        return None, True
    except requests.exceptions.ConnectionError:
        if 'resource_id' in resource:
            id = resource['resource_id']
        else:
            id = 'No ID found in resource'
        print("Got a requests.exceptions.ConnectionError while trying to estimate the size of {} ({})".format(name_of_resource(resource),id))
        return None, False # <= This False value for sizing_attempted deviates from the convention used so far, but will make
            # it easier to spot the resources that are not getting sized.

    if response.status_code in [404]:
        return None, True
    if 'Content-Range' in response.headers:
        estimate = int(response.headers['Content-Range'].split('/')[1])
        return estimate, True
    elif 'Content-Length' in response.headers:
        return int(response.headers['Content-Length']), True
    else:
        #print("Unable to identify the size of the transferred file from these headers: {}".format(response.headers))
        # I believe that almost all files in this category are files that need to be dumped from datastore tables.

        # Determine whether size is known from old_tracks.
        size_is_known = False
        for t in old_tracks:
            if t['resource_id'] == resource_id:
                if 'size' in t and t['size'] is not None:
                    size_is_known = True
        size_it = (random.random() < 0.1 - 0.05*size_is_known) or force_sizing
        if size_it:
            # Actually fetch the resource and determine the file size.
            print("Getting {} to determine its file size.".format(url))

            try:
                r2 = requests.get(url,timeout=60)
                pause()
            except requests.exceptions.Timeout:
                print("Timed out while getting {}".format(url))
                return resource['size'], True

            if 'Content-Range' in r2.headers:
                estimate = int(r2.headers['Content-Range'].split('/')[1])
                print("   Content-Range: Determined {} to have a size of {}.".format(resource_id,estimate))
                return estimate, True
            elif 'Content-Length' in r2.headers:
                estimate = int(r2.headers['Content-Length'])
                print("   Content-Length: Determined {} to have a size of {}.".format(resource_id,estimate))
                return estimate, True
            elif 'Content-Type' in r2.headers and r2.headers['Content-Type'] == 'text/csv':
                estimate = len(r2.text)
                print("   len(r2.text): Determined {} to have a size of {}.".format(resource_id,estimate))
                return estimate, True
            print("Unable to identify the size of the transferred file from the HEAD headers {} or from the GET headers".format(response.headers,r2.headers))
            return resource['size'], True # I think this should work both for CKAN API response resources and tracks.
        else:
            return resource['size'], False # False here means that the function didn't try to size the resource.

        # This next line should never need to be called:
        #return resource['size'], False # I think this should work both for CKAN API response resources and tracks.

def parse_time_isoformat(timestring):
    if type(timestring) != str:
        raise ValueError("{} is not a string!".format(timestring))
    try:
        parsed_time = datetime.strptime(timestring,"%Y-%m-%dT%H:%M:%S.%f")
    except ValueError: # isoformat sometimes outputs in the following format:
        parsed_time = datetime.strptime(timestring,"%Y-%m-%dT%H:%M:%S")
    return parsed_time

def extract_features(package,resource,old_tracks,speedmode_seed=False,sizing_override=False):
    # speedmode can be set to False by the user, but presently this
    # can be overridden by situations like when we've seen the
    # resource today.

    speedmode = bool(speedmode_seed)
    old_ids = [t['resource_id'] for t in old_tracks]
    if resource['id'] in old_ids:
        tracked_r = old_tracks[old_ids.index(resource['id'])]
        # If we've already looked at this resource today, use speedmode
        # to skip over time-consuming steps for that resource.
        if tracked_r['last_seen'][:10] == datetime.today().date().isoformat():
           speedmode = True
    if sizing_override:
        speedmode = False
    if speedmode:
        rows = columns = None
    elif resource['format'] in ['CSV','csv','.csv']: #'XLSX','XLS']:
        rows = get_number_of_rows(site,resource['id'],API_key)
        if rows is None: # We need to disambiguate the "None" returned by the
            rows = 0 # CKAN API when the table is empty from the "None" used
            # by dataset-tracker to indicate there is no value in that field.
        schema = get_schema(site,resource['id'],API_key)
        if schema is None:
            columns = None
        else:
            columns = len(schema)
    else:
        rows = columns = None
    resource_name = name_of_resource(resource)
    package_url_path = "/dataset/" + package['name']
    package_url = site + package_url_path
    resource_url_path = package_url_path + "/resource/" + resource['id']
    resource_url = site + resource_url_path
    download_link_status = None
    download_url = download_url_of_resource(resource)

    tag_dicts = package['tags']
    tags = [td['name'] for td in tag_dicts]
    if '_etl' in tags:
        # This is the package-level tag, so not every resource inside will be ETLed.
        # For the Air Quality dataset, Excel, CSV, and PDF files all seem to be ETLed.
        # Let's exclude data dictionaries:
        if re.search('data dictionary',resource_name,re.IGNORECASE) is not None or resource['format'] in ['HTML','html']:
            loading_method = 'manual'
        else:
            loading_method = 'etl'
    elif '_harvested' in tags:
        loading_method = 'harvested'
    else:
        r_names = [r['name'] if 'name' in r else 'Unnamed resource' for r in package['resources']]
        if 'Esri Rest API' in r_names:
            loading_method = 'harvested'
        else:
            loading_method = 'manual' # These
            # are probably manually uploaded.


    if speedmode:
        estimated_size = None
        sizing_attempted = False
    else:
        estimated_size, sizing_attempted = size_estimate(resource,old_tracks)

    groups_string = sort_and_stringify_groups(package)
    tags_string = sort_and_stringify_field(package,'tags','name')
    now = datetime.now().isoformat()
    r_tuples = [('resource_name',resource_name),
        ('resource_id',resource['id']),
        ('package_name',package['title']),
        ('package_id',resource['package_id']),
        ('organization',package['organization']['title']),
        ('resource_url',resource_url),
        ('package_url',package_url),
        ('download_url',download_url),
        ('download_link_status',download_link_status),
        ('created',resource['created']),
        ('first_published',None),
        ('first_seen',now),
        ('last_seen',now),
        ('total_days_seen',1),
        ('last_modified',resource['last_modified']),
        ('rows',rows),
        ('columns',columns),
        ('size',estimated_size),
        ('last_sized',now if sizing_attempted else None),
        ('loading_method',loading_method),
        ('format',resource['format']),
        ('tags',tags_string),
        ('groups',groups_string)]

    return OrderedDict(r_tuples)


def check_resource_for_growth(change_log,record,x,modified_record,live_package,now,last_seen):
    now = datetime.now()
    if 'rows' in x and 'rows' in record:
        resource_name = record['resource_name']
        package_name = record['package_name']

        if x['rows'] is None:

            # Maybe check if this one needs to be checked before getting the number of rows:
            print("Getting the number of rows for {}|{} ({}) since x['rows'] = None.".format(resource_name,package_name,record['resource_id']))
            x['rows'] = get_number_of_rows(site,x['resource_id'])
        if x['rows'] is None:
            print("  It looks like get_number_of_rows returned a value of None for {}".format(resource_name))
            return modified_record, change_log
        if record['rows'] != x['rows']:
            # Either the resource has grown or shrunk.
            print("   For {}, record['rows'] != x['rows']: {}!= {}".format(resource_name,record['rows'],x['rows']))
            if 'last_modified' in record and record['last_modified'] is not None: # [ ] This should really be based on time_of_last_size_change.
                timespan = (now - parse_time_isoformat(record['last_modified'])).total_seconds()/3600.0
                if x['rows'] > record['rows']:
                    print("      {} more rows which appeared at a rate of {} rows/hour".format(x['rows'] - record['rows'], (x['rows'] - record['rows'])/timespan))
                else:
                    print("      {} fewer rows, with rows disappearing at a rate of {} rows/hour".format(-x['rows'] + record['rows'], (-x['rows'] + record['rows'])/timespan))

                change_log[record['resource_id']] = {'row_count_change': x['rows'] - record['rows'],
                        'timespan_in_hours': timespan,
                        'previously_modified': parse_time_isoformat(record['last_modified']),
                        }
                smart_pprint(change_log[record['resource_id']])
            time_of_last_size_change = now
            modified_record['time_of_last_size_change'] = now.isoformat()
        else:
            # Check whether the duration between changes is too large.
            if 'time_of_last_size_change' in record:
                time_of_last_size_change = parse_time_isoformat(timestring=record['time_of_last_size_change'])

                # Find live package by resource ID.
                if 'frequency_publishing' in live_package.keys():
                    publishing_frequency = live_package['frequency_publishing']
                    if publishing_frequency in period:
                        publishing_period = period[publishing_frequency]
                    else:
                        publishing_period = None
                        if publishing_frequency not in nonperiods:
                            raise ValueError("{}: {} is not a known publishing frequency".format(resource_name,publishing_frequency))
                else:
                    print('frequency_publishing is not specified for this package:')
                    publishing_frequency = None
                    publishing_period = None
                if 'frequency_data_change' in live_package:
                    data_change_rate = live_package['frequency_data_change']
                else:
                    data_change_rate = None
                package_id = live_package['id']
                if x['last_modified'] is None:
                    resource_last_modified = None
                else:
                    resource_last_modified = parse_time_isoformat(x['last_modified'])

                stale_resources = {}
                stale_count = 0
                #print("{} ({}) was last modified {} (according to its metadata). {}".format(resource_name,package_id,metadata_modified,package['frequency_publishing']))


                if resource_last_modified is None:
                    print("  What should we do when x['last_modified'] is None (like for this resource)?")
                elif publishing_period is not None:
                    lateness = now - (resource_last_modified + publishing_period)
                    change_delay = now - (time_of_last_size_change + publishing_period)
                    print("      lateness = {}, while change_delay = {}".format(lateness, change_delay))
                    if package_id in extensions.keys():
                        if lateness.total_seconds() > 0 and lateness.total_seconds() < extensions[package_id]['extra_time'].total_seconds():
                            print("{} is technically stale ({} cycles late), but we're giving it a pass because either there may not have been any new data to upsert or the next day's ETL job should fill in the gap.".format(resource_name,lateness.total_seconds()/publishing_period.total_seconds()))
                        lateness -= extensions[package_id]['extra_time']

                    if change_delay.total_seconds() > 0:
                        print("  No change in the last publishing period ({}). change_delay = {}".format(publishing_period,change_delay))

    return modified_record, change_log

def reset_size_change_times(server):
    tracks = load(server)
    # Reset time_of_last_size_change values:
    for record in tracks:
        if 'time_of_last_size_change' in record: # Reset time_of_last_size_change values to more reasonable parameters.
            del record['time_of_last_size_change']

    store_resources_as_file(tracks,server)
    print("Deleted time_of_last_size_change parameter from most tracked resources.")

def set_resource_parameter(server,resource_id,parameter,value):
    # Manually set the value of a parameter of one of the tracked resources
    # in the JSON file (or wherever the store* function below stores the tracks.)
    tracks = load(server)
    for record in tracks:
        if record['resource_id'] == resource_id:
            unmodified_resource = dict(record)
            converted_value = value #converted_value = guess_type_and_convert(value)
            record[parameter] = converted_value
            break
    else:
        raise ValueError("Unable to find resource ID {}".format(resource_id))

    store_resources_as_file(tracks,server)

    u = unmodified_resource
    if parameter in unmodified_resource.keys():
        if u[parameter] == converted_value and type(u[parameter]) == type(converted_value):
            # The type-checking is necessary because of an oddity wherein integer values
            # (like 1) are considered equivalent to booleans (which should look like
            # true in the JSON output).:
            print("The '{}' parameter of {} ({}) was already equal to {}.".format(parameter,
            u['resource_name'], u['resource_id'], converted_value))
        else:
            print("Changed the '{}' parameter of {} ({}) from {} to {}.".format(parameter,
            u['resource_name'], u['resource_id'], u[parameter], converted_value))
    else:
        print("Added the '{}' parameter to {} ({}) with a value of {}.".format(parameter,
            u['resource_name'], u['resource_id'], converted_value))

def update(change_log,record,x,live_package,speedmode):
    # record appears to be converted from the JSON file, so its dates need to be parsed,
    # whereas x comes from ckanapi and should be properly typed already.
    assert record['resource_id'] == x['resource_id']
    assert record['package_id'] == x['package_id']
    assert record['created'] == x['created']
    # The linking code is presumed to be immutable, based on how it's being defined.

    last_seen = parse_time_isoformat(record['last_seen'])
    now = datetime.now()

    modified_record = OrderedDict(record)
    #if not speedmode: # In speed mode, row and column values aren't extracted,
    #    # so it makes no sense to check for growth (or shrinking) of a resource.
    if 'rows' in record and record['rows'] is not None:
        modified_record, change_log = check_resource_for_growth(change_log,record,x,modified_record,live_package,now,last_seen)

    #smart_pprint(modified_record)

    modified_record['last_seen'] = now.isoformat()
    if last_seen.date() != now.date():
        modified_record['total_days_seen'] += 1

    # Update row counts, column counts, etc.
    modified_record['resource_name'] = x['resource_name'] # Keep resource names updated.
    modified_record['resource_url'] = x['resource_url']
    #modified_record['linking_code'] = x['linking_code'] # Don't update linking codes.

    # The package name could easily change, so these URLs need to be updated.
    modified_record['package_name'] = x['package_name']
    modified_record['package_url'] = x['package_url']
    modified_record['download_url'] = x['download_url']
    modified_record['download_link_status'] = x['download_link_status']
    modified_record['last_modified'] = x['last_modified']
    modified_record['rows'] = x['rows'] if x['rows'] is not None else record['rows']
    modified_record['columns'] = x['columns'] if x['columns'] is not None else record['columns']
    modified_record['size'] = x['size'] if x['size'] is not None else record['size'] # Only update the
    # 'size' field if a new value has been obtained.
    modified_record['last_sized'] = x['last_sized'] if x['last_sized'] is not None else record['last_sized']
    # Only update the 'last_sized' field if a new (non-None) value has been obtained.
    modified_record['loading_method'] = x['loading_method']
    modified_record['format'] = x['format']
    modified_record['tags'] = x['tags']
    modified_record['groups'] = x['groups']
    return modified_record, change_log

def domain(url):
    """Take a URL and return just the domain."""
    return url.split("://")[1].split('/')[0]

def print_and_format(resource_name,durl):
    printable = "{}: Dead link found ({}).".format(resource_name,durl)
    print(printable)
    item = "{} ({})".format(resource_name,durl)
    return item

def list_unnamed(tracks=None):
    if tracks is None:
        tracks = load_resources_from_file(server)
    items = []
    for k,r in enumerate(tracks):
        if 'resource_name' not in r or r['resource_name'] in ['Unnamed resource', '', None]:
            print("{} in {} has no name. It's listed as being in the {} format. Here's the URL: {}".format(r['resource_id'], r['package_name'], r['format'], r['resource_url']))
            item = "{} in {} ({})".format(r['format'],r['package_name'],r['resource_id'])

    if len(items) > 0:
        msg = pluralize("unnamed resource",items) + " found:" + ", ".join(items)
        print("\n"+msg)

def find_empty_tables(tracks=None,alerts_on=False):
    if tracks is None:
        tracks = load_resources_from_file(server)
    items = []
    for k,r in enumerate(tracks):
        if ('active' in r and r['active']) and 'columns' in r and r['columns'] is not None and 'rows' in r and r['rows'] == 0:
            filter_function = lambda x : x['resource_id'] == str(r['resource_id'])
            really_empty = check_row_count(filter_function, None)
            if really_empty:
                print("{} in {} has no rows and {} columns. It looks like the upload or ETL script broke. Here's the URL: {}".format(r['resource_id'], r['package_name'], r['columns'], r['resource_url']))
                item = "{} in {} ({})".format(r['format'],r['package_name'],r['resource_id'])
                items.append(item)


    if len(items) > 0:
        msg = pluralize("empty table",items) + " found:" + ", ".join(items)
        if alerts_on:
            send_to_slack(msg,username='dataset-tracker',channel='@david',icon=':koolaid:')
        print("\n"+msg)
    else:
        print("No empty tables found.")

def find_duplicate_packages(live_only=True,tracks=None,alerts_on=False):
    # This function is designed to find harvested resources that
    # are no longer getting updated (because they've been deleted
    # from the source server but stick around as phantom datasets
    # on the data portal).

    if tracks is None:
        tracks = load_resources_from_file(server)
    items = []
    id_by_index = {}
    name_by_index = {}
    for k,r in enumerate(tracks):
        include = False
        if live_only:
            if 'active' in r and r['active']:
                include = True
        else:
            include = True

        if include:
            if r['package_name'] in name_by_index.values(): # The name has previously appeared.
                if r['package_id'] not in id_by_index.values(): # But the ID has not.
                    item = "{} ({}) is a duplicate package from {}".format(r['package_name'],r['package_id'],r['organization'])
                    items.append(item)
            id_by_index[k] = r['package_id']
            name_by_index[k] = r['package_name']

    if len(items) > 0:
        intro = pluralize("duplicate package",items) + " found:"
        msg = intro + ", ".join(items)
        if alerts_on:
            send_to_slack(msg,username='dataset-tracker',channel='@david',icon=':koolaid:')
        print(intro)
        for item in items:
            print("  {}".format(item))
    else:
        print("No duplicate packages found.")

def check_row_count(filter_function,tracks=None):
    # Check the sizes of all resources that make it through the filter function
    # and update tracks accordingly.
    if tracks is None:
        tracks = load_resources_from_file(server)
    updated_something = False
    for k,r in enumerate(tracks):
        if filter_function(r):
            rows = get_number_of_rows(site,r['resource_id'],API_key)
            now = datetime.now().isoformat()
            if 'rows' in r:
                if r['rows'] != rows:
                    print("The row count of {} changed from {} to {}.".format(r['resource_name'],r['rows'],rows))
                # Otherwise the size estimate has not changed, but we still need to update the last_sized field.
            if rows is None:
                print("The row count of {} couldn't be determined. It's listed as being in the {} format. Here's the download URL: {}".format(r['resource_id'], r['format'], r['resource_url']))
                really_empty = True
            else:
                print("The row count of {} wasn't previously determined. It's listed as being in the {} format. It looks like it's actually {}".format(r['resource_id'], r['format'], rows))
                really_empty = (rows == 0)
            r['rows'] = rows
            #r['last_sized'] = now # Do we need to track when the row count was made too?
    store_resources_as_file(tracks,server)

    return really_empty

def check_all_unknown_sizes(tracks=None):
    """This function hasn't been needed so far since the resources with unknown sizes tend to be either
    links or missing files.

    This function hasn't been tested recently, and will likely break on dead resources."""
    if tracks is None:
        tracks = load_resources_from_file(server)
    updated_something = False
    for k,r in enumerate(tracks):
        if 'size' not in r or r['size'] in [None]:
            estimate, sizing_attempted = size_estimate(r,tracks,True)
            now = datetime.now().isoformat()
            if 'size' in r:
                if r['size'] != estimate:
                    print("The size estimate of {} changed from {} to {}.".format(r['resource_name'],r['size'],estimate))
                # Otherwise the size estimate has not changed, but we still need to update the last_sized field.
            if estimate is None:
                print("The size of {} hadn't been determined and still can't be determined. It's listed as being in the {} format. Here's the download URL: {}".format(r['resource_id'], r['format'], r['resource_url']))
            else:
                print("The size of {} wasn't previously determined. It's listed as being in the {} format. It looks like it's actually {}".format(r['resource_id'], r['format'], estimate))
            r['size'] = estimate
            r['last_sized'] = now
    store_resources_as_file(tracks,server)

def is_harvested_package(raw_package):
    # Our current rule-of-thumb as to whether a package is harvested is whether it contains an
    # 'Esri Rest API' resource. This works for two reasons: 1) We are only harvesting ESRI
    # stuff. 2) When the harvest breaks, it tends to manage to get the first two resources
    # (including the 'Esri Rest API' one) and break on the CSV extraction.
    r_names = [r['name'] if 'name' in r else 'Unnamed resource' for r in raw_package['resources']]
    if 'Esri Rest API' in r_names:
        return True
    return re.search('this dataset is harvested on a weekly basis',raw_package['notes']) is not None

def check_live_licenses():
    # 1) We only care about whether active resources have licenses.
    # 2) It's a pain to go back and deal with the inactive resources for which license information has not been tracked.
    # 3) Maybe it's better to put such functions in pocket-watch and leave archive or crossover stuff to dataset-tracker.
    #   *) Currently, pocket-watch is very minimalistic with no use of the fire library, so queries of the live site
    #      will probably stay in dataset-tracker for a while.

    ckan = ckanapi.RemoteCKAN(site) # Without specifying the apikey field value,
    # the next line will only return non-private packages.
    packages = ckan.action.current_package_list_with_resources(limit=999999)

    items = []
    unlicensed = []
    license_counts = defaultdict(int)
    for k,p in enumerate(packages):
        if 'license_title' not in p:
            items.append("{}".format(p['title']))
            license_counts['No license'] += 1
            unlicensed.append(p)
        else:
            license_counts[p['license_title']] += 1
            if p['license_title'] is None:
                items.append("{}".format(p['title']))
                unlicensed.append(p)

    print("Distribution of licenses by package:")
    smart_pprint(dict(license_counts))
    if len(items) > 0:
        msg = "{} without licenses found: ".format(pluralize('package',items), ", ".join(items))
        print(msg)
        # These tend to be harvested packages.
        nonharvested = []
        for p in unlicensed:
            if not is_harvested_package(p):
                nonharvested.append(p['title'])
        if len(items) == 1:
            msg = "This single package is particularly interesting because it is a non-harvested package without a license: {}".format(len(nonharvested),", ".join(nonharvested))
        else:
            msg = "These {} packages are particularly interesting because they are non-harvested packages without licenses: {}".format(len(nonharvested),", ".join(nonharvested))
        print(msg)


def check_formats(tracks=None):
    if tracks is None:
        tracks = load_resources_from_file(server)
    standard_formats = ['CSV','HTML','ZIP','GeoJSON','Esri REST','KML',
        'PDF','XLSX','XLS','TXT','DOCX','JSON','XML','RTF','GIF','API']
    items = []
    for k,r in enumerate(tracks):
        if 'format' in r:
            if r['format'] not in standard_formats:
                # ['.csv','csv','',' ','.html','html','.xlsx','.zip','.xls',None,'None','pdf','.pdf']:
                items.append("{} ({})".format(r['resource_name'],r['format'] if r not in [None,''] else "<missing format>"))

    if len(items) > 0:
        msg = "{} with non-standard formats found: {}".format(pluralize("resource",items), ", ".join(items))
        print(msg)

def check_links(tracks=None):
    if tracks is None:
        tracks = load_resources_from_file(server)
    items = []
    last_domain = ''
    checked_urls = {}
    for k,r in enumerate(tracks):
        if 'download_url' in r and r['download_url'] is not None and domain(r['download_url']) != domain(site) and r['download_url'] != 'http://#':
            durl = r['download_url']
            if durl not in checked_urls.keys():
                if last_domain == domain(durl):
                    time.sleep(0.1)
                else:
                    time.sleep(0.01)

                try:
                    response = requests.head(durl,timeout=60)
                except requests.exceptions.Timeout:
                    print("Timed out while getting the head from {}".format(durl))
                    items.append("Unable to get the head from {} for {}".format(durl,r['resource_name']))

                # 405 Method Not Allowed (the server refuses to respond to a HEAD request.)
                if response.status_code == 405:
                    try:
                        response = requests.get(durl,timeout=60)
                    except requests.exceptions.Timeout:
                        print("Timed out while getting {}".format(url))
                        items.append("Unable to get {} for {}".format(url,r['resource_name']))

                checked_urls[durl] = response.status_code
                r['download_link_status'] = response.status_code
                last_domain = domain(durl)
                if response.status_code != 200:
                    print("   {}: {}".format(durl, response.status_code))
                #if response.status_code == 308: # 308 was seen when running check_links many times
                ## in a row (from the ArcGIS servers) but it has not been encountered on the most
                ## recent test run, so this code will be commented out for now.
                #    print("         {}: {}".format(durl, dir(response)))
                #    if 'Location' in dir(response):
                #        print("         {} redirect Location: {}.format(durl, response.Location))
                if response.status_code == 404:
                    items.append(print_and_format(r['resource_name'],durl))
                # Other responses to consider:
                # 202 ACCEPTED The request has been accepted for processing, but the processing has not been completed. The request might or might not eventually be acted upon, as it might be disallowed when processing actually takes place. The 202 response is intentionally noncommittal. The representation sent with this response ought to describe the request's current status and point to (or embed) a status monitor that can provide the user with an estimate of when the request will be fulfilled.
                # For example, one link resulted in this response:
                #   // 20180118105744
                #    // https://pghgis-pittsburghpa.opendata.arcgis.com/datasets/34735757b7384fde97960cc01c4f3318_0.geojson
                #    {
                #      "processingTime": "2.2493333333333334 minutes",
                #      "status": "Failed",
                #      "generating": {
                #
                #      },
                #      "error": {
                #        "message": "Service returned count of 0",
                #        "code": 500
                #      }
                # So one could get the true (temporary) code from there.
                # Returning to this link multiple times gives different responses, but the file never seems to get generated.


                # The HTTP 204 No Content success status response code indicates that the request has succeeded, but that the client doesn't need to go away from its current page. (Links with this response seem to load just fine.)
                # The HTTP 302 Found redirect status response code indicates that the resource requested has been temporarily moved to the URL given by the Location header.
                # The HyperText Transfer Protocol (HTTP) 308 Permanent Redirect redirect status response code indicates that the resource requested has been definitively moved to the URL given by the Location headers.
                # The HTTP 401 Unauthorized client error status response code indicates that the request has not been applied because it lacks valid authentication credentials for the target resource.

                # 500 Internal Server Error

                # It's also interesting to note that apparently most
                # sites do not correctly set the last-modified value in
                # the header responses, so it's not a good idea to try
                # to use it unless you know that a particular site
                # keeps it updated.
            elif checked_urls[durl] == 404:
                items.append(print_and_format(r['resource_name'],durl))

    if len(items) > 0:
        msg = "{} found: {}".format(pluralize("dead link",items), ", ".join(items))
        print(msg)

    store_resources_as_file(tracks,server)

def get_terminal_size():
    rows, columns = os.popen('stty size', 'r').read().split()
    return int(rows), int(columns)

def print_another_table(table):
    if sys.stdout.isatty():
        #Running from command line
        rows, columns = get_terminal_size()

        big_fields = 2
        row_count_width = 10
        big_width = int(math.floor((rows - row_count_width) / big_fields))
        template = "{{:<{}.{}}}  {{:<{}.{}}}  {{:<{}}}"
        fmt = template.format(big_width,big_width,big_width,big_width,row_count_width)
        fmt = "{:<60.60}  {:<60.60}  {:>10} {:>4}  {:<35.35}"

        used_columns = len(fmt.format("aardvark","bumblebee",
            "chupacabra","dragon","electric eel","flying rod",
            "gorilla"))
        border = "{}".format("="*used_columns)
        print(fmt.format("Package name", "Resource name" ,"Rows", "Cols", "Resource ID"))
        print(border)

        for d in table:
            fields = [d['package_name'],d['resource_name'],d['rows'],d['columns'],d['resource_id']]

            print(fmt.format(*fields))
        print("{}\n".format(border))

def check_for_partial_uploads(tracks=None):
    """Check all active resources and note the ones that have a multiple of 250 rows and a CSV
    or Excel download, as these are telltale signs of partial uploads (where MessyTables
    incorrectly guessed some data type and the upload broke in the middle)."""
    if tracks is None:
        tracks = load_resources_from_file(server)

    entries = []
    zero_rows = 0
    for row in tracks:
        # Check if maybe this resource might be only partially uploaded:
        if 'active' in row and row['active']:
            if 'rows' in row:
                if row['rows'] is not None and row['rows'] % 250 == 0:
                    if row['download_url'][-3:].lower() in ['csv','xls','lsx']:
                        warning = "<{}|{}> in {} has {} rows and the download URL ({}) makes it look like the file didn't completely upload.".format(row['resource_url'],row['resource_name'],row['package_name'],row['rows'],row['download_url'])
                        print(warning)
                        entry = {'resource_name': row['resource_name'], 'package_name': row['package_name'], 'rows': row['rows'], 'columns': row['columns'] if row['columns'] is not None else '', 'resource_id': row['resource_id']}
                        entries.append(entry)

                        if row['rows'] == 0:
                            zero_rows += 1
    if len(entries) > 0:
        print("")
        print_another_table(entries)
        print("There are a total of {} resources that look like partial uploads. {} of these are 0-row resources".format(len(entries),zero_rows))


def check_all(tracks=None):
    tracks = load_resources_from_file(server)
    list_unnamed(tracks)
    check_all_unknown_sizes(tracks)
    check_formats(tracks)
    check_links(tracks)
    check_live_licenses()
    find_empty_tables(tracks,False)
    check_for_partial_uploads(tracks)
    find_duplicate_packages(True,tracks,False)

def fetch_live_resources(site,API_key,server,speedmode,sizing_override):#:(speedmode=False,return_data=False,sizing_override=False):
### Think through what else needs to be moved around to make this self-contained and
### to make the code from inventory() removable.
    ckan = ckanapi.RemoteCKAN(site) # Without specifying the apikey field value,
# the next line will only return non-private packages.
    try:
        packages = ckan.action.current_package_list_with_resources(limit=999999)
    except:
        packages = ckan.action.current_package_list_with_resources(limit=999999)
    # This is a list of all the packages with all the resources nested inside and all the current information.

#    old_data = load_resource_archive(site,API_key)
    old_data = load_resources_from_file(server)
    list_of_odicts = []
    old_package_ids = [r['package_id'] for r in old_data]
    #package_ids = [p['id'] for p in packages]
    #print("len(package_ids) = {}. There are {} unique package IDs.".format(len(package_ids),len(set(package_ids))))
    #old_package_names = [r['package_name'] for r in old_data]
    #    if p['title'] not in old_package_names:
    #        print("{} ({}) is not being tracked.".format(p['title'],p['id']))

    # The above code only prints
    # City of Pittsburgh Signalized Intersections (f470a3d5-f5cb-4209-93a6-c974f7d5a0a4) is not being tracked.
    # but it actually is being tracked.

    for p in packages:
        if p['id'] not in old_package_ids:
            print("{} ({}) has not been tracked yet.".format(p['title'],p['id']))

    for p in packages:
        for r in p['resources']:
            current_row = extract_features(p,r,old_data,speedmode,sizing_override)
            linking_code, link_type = generate_linking_code(current_row)
            if link_type != 'manually added':
                current_row['linking_code'] = linking_code
            list_of_odicts.append(current_row)
            print(".", end="", flush=True)

    return list_of_odicts, old_data, packages

def linking_code_template(datum):
    return datum['package_id'] + ' | ' + datum['resource_name']

def generate_linking_code(tracked_resource):
    # This function generates effective IDs to link together different resources that contain
    # the same information. (There are a bunch of these because of how the CKAN harvest
    # extension works.)

    if 'loading_method' in tracked_resource and tracked_resource['loading_method'] == 'harvested':
        code = linking_code_template(tracked_resource)
        return code, 'harvested'
    elif 'comments' not in tracked_resource or tracked_resource['comments'] != 'Manually added':
        return tracked_resource['resource_id'], 'manually added'
    else:
        assert 'linking_code' in tracked_resource
        code = tracked_resource['linking_code']
        return code, 'unchanged'

def find_package_by_id(tracks,ref_id):
    if tracks is None:
        tracks = load_resources_from_file(server)
    matches = []
    for r in tracks:
        if r['package_id'] == ref_id:
            matches.append(r) # Tracked resources with the reference package ID.

    # Sort resources by resource name before returning.
    return sorted(matches,key=lambda r: r['package_name'])

def link_packages(source_id,receiver_id,tracks=None):
    # Provide a command to link two datasets in a directed way (source and receiver)
    # and then autocopy over all the linking codes.
    if tracks is None:
        tracks = load_resources_from_file(server)

    source_resources = [r for r in find_package_by_id(tracks,source_id) if ('active' in r and r['active'])]
    receiver_resources = [r for r in find_package_by_id(tracks,receiver_id) if ('active' in r and r['active'])]
    
    # We can only copy over linking codes if the receiver resources are a subset
    # of the source resources.

    matched_source_resources = []
    for rr in receiver_resources:
        chosen_index = None
        for k,sr in enumerate(source_resources):
            if rr['resource_name'] == sr['resource_name']:
                chosen_index = k
        if chosen_index is None:
            raise ValueError("Unable to find a source for the receiver resource {}".format(rr['resource_name']))
        matched_source_resources.append(source_resources[chosen_index])

    assert len(matched_source_resources) == len(receiver_resources)
    for s,r in zip(matched_source_resources,receiver_resources):
        assert s['resource_name'] == r['resource_name']

    for s,r in zip(matched_source_resources,receiver_resources):
        print("Changing {}|{} to link to {}|{}.".format(r['package_id'],r['resource_name'],s['package_id'],s['resource_name']))
        prompt_for("Hit return to proceed")
        r['linking_code'] = str(s['linking_code'])
        # Modifying the object should modify the tracks
        # data structure.

    store_resources_as_file(tracks,server)

# stats functions
def identity(x):
    return x

def find_min(m,field,f = identity):
    minimum = 99999999999
    value = None
    for r in m:
        if field in r and f(r[field]) < minimum:
            minimum = f(r[field])
            value = r[field]
    return minimum, value

def find_max(m,field,f = identity):
    maximum = -99999999999999999999999
    value = None
    for r in m:
        if field in r and f(r[field]) > maximum:
            maximum = f(r[field])
            value = r[field]
    return maximum, value

def stats(tracks=None):
    if tracks is None:
        tracks = load_resources_from_file(server)
    package_set = set()
    resource_count = len(tracks)
    organization_set = set()
    max_rows = 0
    max_cols = 0
    for r in tracks:
        package_set.add(r['package_id'])
        organization_set.add(r['organization'])
        if 'rows' in r and r['rows'] is not None and r['rows'] > max_rows:
            max_rows = r['rows']
        if 'columns' in r and r['columns'] is not None and r['columns'] > max_cols:
            max_cols = r['columns']

    # Find the shortest nontrivial download URL:
    def len_mod(x):
        return 999999999999 if x is None else 9999999999 if x[0:8]=="http://#" else len(x)

    shortest_download_url_length,shortest_download_url = find_min(tracks,'download_url',len_mod)


    print("There appear to be {} packages in the tracking file and {} resources under {} organizations.".format(len(package_set),resource_count,len(organization_set)))

    print("The longest table has {} rows, while the widest table has {} columns.".format(max_rows,max_cols))
    print("The shortest download URL ({}) has {} characters.".format(shortest_download_url,shortest_download_url_length))
# end stats functions

def utf8_encode(x):
    try:
        return str(x)
    except:
        if x is None:
            return "None"
        if type(x) in [int, float]:
            return str(x)
        if type(x) in [list]:
            return [utf8_encode(i) for i in x]
        try:
            return x.encode('utf-8')
        except AttributeError:
            return "<Some weird variable (of type {}) that utf8_encode can't handle>".format(x)

def inventory(alerts_on=True,speedmode=False,return_data=False,sizing_override=False):
    current_rows, old_data, packages = fetch_live_resources(site,API_key,server,speedmode,sizing_override)

    live_package_lookup = {r['id']: p for p in packages for r in p['resources']}
    live_package_by_id = {p['id']: p for p in packages}

    merged = []
    processed_current_ids = []
    # current_rows, old_data = fetch_live_resources(site,API_key,server,speedmode,sizing_override)
    print("len(current_rows) = {}".format(len(current_rows)))
    current_resource_ids = [r['resource_id'] for r in current_rows]
    old_harvest_linking_codes = []
    disappeared_dict = defaultdict(list) # A dictionary that lists formatted message strings about each
    # resource that has just disappeared in this iteration (based on whether it has just
    # flipped to inactive), under a key equal to the package ID, except for harvested resources,
    # the disappearance of which is ignored as normal.


    ## BEGIN sizing ##
    if not speedmode and not sizing_override:
        # When it's OK for the code to take a while to run, size the
        # n resources that most need sizing.
        resources_to_size = 5
        ###### Sizing code intended to size code that most needs sizing
        # [ ] The last_modified date of the package/resource should also
        # be weighed in this decision.
        sizing_dates = []
        old_ids = [t['resource_id'] for t in old_data]
        a_long_time_ago = '1018-02-05T15:22:47.686048'
        for k,current_row in enumerate(current_rows): # This might not be the best place to do this...
            if current_row['resource_id'] in old_ids:
                tracked_r = old_data[old_ids.index(current_row['resource_id'])]
                date_sized = tracked_r['last_sized'] if tracked_r['last_sized'] is not None else a_long_time_ago
                #sizing_dates.append((k,date_sized))
                sizing_dates.append(date_sized)
        sds = sizing_dates
        simply_sorted_sds, indices = zip(*sorted(zip(sizing_dates,range(0,len(sizing_dates))), key=lambda w: w[0]))
        # Such sorting is not the most efficient algorithm, but it will do when the number of rows is
        # as small as it is.
        # If 'last_sized' is None, we should definitely prioritize sizing it since
        # even a failed attempt should update the 'last_sized' field.
        for k in indices[:resources_to_size]:
            current_row = current_rows[k]
            print("Sizing row {}: {}".format(k,current_row['resource_name']))
            new_estimate, _ = size_estimate(current_row,old_data,force_sizing=True)
            assert _ == True
            if new_estimate is not None or 'size' not in current_row:
                current_row['size'] = new_estimate
                current_row['last_sized'] = datetime.now().isoformat()
            elif new_estimate is None:
                if 'size' in current_row:
                    if current_row['size'] is None:
                        current_row['last_sized'] = datetime.now().isoformat()
                else:
                    current_row['last_sized'] = datetime.now().isoformat()
            if BACKUP_DATA:
                current_row = backup_to_disk(current_row) # For now, just backup files when they're sized.
            # Eventually maybe use a smarter schedule, backing up files based on how many update cycles
            # the last backup and current version differ by (either based on nominal publication
            # frequency or one inferred from row count or timestamps in the live data).
            current_rows[k] = current_row # Should do nothing if current_row is the same
            # object as current_rows[k].
        #########
    ## END sizing ##


    ## BEGIN Review all existing resources and look for updates from current_rows ##
    change_log = {}
    resources_to_relink = []
    modified_resources_lookup = defaultdict(list)
    for datum in old_data:
        old_id = datum['resource_id']
        if 'loading_method' in datum and datum['loading_method'] == 'harvested':
            if 'linking_code' in datum:
                if datum['linking_code'] is not None:
                    old_harvest_linking_codes.append(datum['linking_code'])
            else:
                # Generate one (just for reference purposes, not to add to the old resource).
                harvest_linking_code, _ = generate_linking_code(datum)
                if harvest_linking_code is not None:
                    old_harvest_linking_codes.append(harvest_linking_code)


        # BEGIN Deal with deleted resources or those that need to be updated. #
        if old_id not in current_resource_ids:
            #print("Keeping the following old resource: {} | {} | {}".format(old_id,datum['resource_name'],datum['organization']))
            if 'active' in datum and datum['active'] in ['True',True]:
                # This resource appears to have been made private or deleted since the last scan.
                d_msg = "{} ({})".format(datum['resource_name'],datum['resource_id'])
                package_key = "{} [{}]".format(datum['package_name'], datum['package_id'])
                if 'loading_method' in datum and datum['loading_method'] == 'harvested':
                    # [ ] If this resource is a harvested resource and goes inactive,
                    # its disappearance need not be noted here, though it would be
                    # useful to eventually use this opportunity to automatically link
                    # those resources with the newly created ones in the same package.
                    pass
                else:
                    disappeared_dict[package_key].append(d_msg)
                datum['active'] = False
            merged.append(datum)
        else: # A case where an existing record needs to be
        # updated has been found.
            x = current_rows[current_resource_ids.index(old_id)]
            live_package = live_package_lookup[x['resource_id']]
            modified_datum,change_log = update(change_log,datum,x,live_package,speedmode)
            modified_datum['active'] = True
            merged.append(modified_datum)
            processed_current_ids.append(old_id)
            # Here we merge all the current information about pre-existing resources with the tracking information
            # where possible. (It can't be done here for reharvests).
        # END Deal with deleted resources or those that need to be updated. #

    if len(disappeared_dict) > 0:
        print("Resources that disappeared, grouped by package ID:")
        extras = []
        for k,dd in enumerate(disappeared_dict):
            print("{}: {}".format(dd,', '.join(disappeared_dict[dd])))
            extras.append("{}) {}: {}".format(k+1, dd, ', '.join(disappeared_dict[dd])))

        msg = "Resources that disappeared, grouped by package ID: {}".format("; ".join(extras))
        print(msg)
        if alerts_on:
            send_to_slack(msg,username='dataset-tracker',channel='#notifications',icon=':clubs:')
    ## END Review all existing resources and look for updates from current_rows ##

    ## BEGIN Review all live packages and check merged list for lack of growth ##
    # At this point check_resource_for_growth has been run on all the
    # pre-existing resources that are being updated, and the 'time_of_last_size_change'
    # field has been updated.
    # Now, we can iterate through the live packages and check whether there's at
    # least one resource in there that has been updated fast enough such that the
    # resource can be considered to be growing.
    # This might mean at least one new row during the last publishing period.

    # Specific checks could also be included.
    #if not speedmode:
    packages_from_file = load_packages_from_file(server)

    ## BEGIN Check for new packages ##
    old_package_ids = [p['package_id'] for p in packages_from_file]

    new_packages = []
    for p_id,p in live_package_by_id.items():
        if p_id not in old_package_ids:
            new_packages.append(p)

    for np in new_packages:
        if 'package_name' not in np:
            print("This package has no 'package_name' field:")
            try:
                smart_pprint(np) # pprint sometimes fails, like if there's unprintable Unicode in the package description.
            except UnicodeEncodeError:
                for k,v in np.items():
                    print("{}: {}".format(utf8_encode(k),utf8_encode(v))) # It's possible that a solution like this is better than smart_pprint

    print("new_packages = {}".format([np['package_name'] for np in new_packages if 'package_name' in np]))

    ## END Check for new packages ##


    ## BEGIN Review and process live entries and add them to the merged list ##
    print("len(merged) = {}".format(len(merged)))
    old_harvest_linking_codes = list(set(old_harvest_linking_codes))
    print("len(processed_current_ids) = {}".format(len(processed_current_ids)))
    brand_new = []
    reharvest_count = 0
    current_package_ids = []
    for current_row in current_rows:
        current_row['active'] = True
        if current_row['resource_id'] not in processed_current_ids:
            # These are new resources that haven't ever been added or tracked.

            # However, harvested resources that have new resource IDs but are otherwise the same as previous resources need to be identified.
            reharvested = False
            if current_row['loading_method'] == 'harvested':
                if current_row['linking_code'] in old_harvest_linking_codes:
                    reharvested = True

            if reharvested:
                reharvest_count += 1
            else:
                item = "<{}|{}> in {} from {}".format(current_row['resource_url'],current_row['resource_name'],current_row['package_name'],current_row['organization'])
                printable = "{} ({}) in {} from {}".format(current_row['resource_name'],current_row['resource_url'],current_row['package_name'],current_row['organization'])
                brand_new.append(item)

                # Check if maybe this new resource might be only partially uploaded:
                if 'rows' in current_row:
                    if current_row['rows'] is not None and current_row['rows'] % 250 == 0:
                        if current_row['download_url'][-3:].lower() in ['csv','xls','lsx']:
                            warning = "<{}|{}> in {} has {} rows and the download URL ({}) makes it look like the file didn't completely upload.".format(current_row['resource_url'],current_row['resource_name'],current_row['package_name'],current_row['rows'],current_row['download_url'])
                            print(warning)
                            send_to_slack(warning,username='dataset-tracker',channel='@david',icon=':koolaid:')

                try:
                    created_dt = datetime.strptime(current_row['created'],"%Y-%m-%dT%H:%M:%S.%f")
                except ValueError:
                    created_dt = datetime.strptime(current_row['created'],"%Y-%m-%dT%H:%M:%S")
                if datetime.now() - created_dt < timedelta(days = 6):
                    current_row['first_published'] = current_row['created']
                else: # Some resources were created long ago and only recently published.
                    current_row['first_published'] = current_row['first_seen']

                current_package_ids.append(current_row['package_id'])
                msg = "dataset-tracker found an entirely new resource: " + printable
                print(msg)
            merged.append(current_row) # Whether it's reharvested or not, it's
            # got to be added as a resource to track, so that the resource IDs
            # are known and the URLs can be looked up in Google Analytics.
    ## END Review and process live entries and add them to the merged list ##

    current_package_ids = list(set(current_package_ids))
    if len(brand_new) > 0:
        if len(brand_new) == 1:
            msg = "dataset-tracker found an entirely new resource: " + brand_new[0]
        else:
            plural = (len(brand_new) != 1)
            msg = "In {}, dataset-tracker found {} {}: ".format(pluralize("package",current_package_ids), "these" if plural else "this", pluralize("entirely new resource",brand_new))
            msg += ', '.join(brand_new)
        if alerts_on:
            send_to_slack(msg,username='dataset-tracker',channel='#new-resources',icon=':tophat:')
            #send_to_slack(msg,username='dataset-tracker',channel='@david',icon=':tophat:')

    if reharvest_count > 0:
        msg = "dataset-tracker observed that {} were reharvested.".format(pluralize("resource",None,reharvest_count))
        if alerts_on:
            send_to_slack(msg,username='dataset-tracker',channel='#notifications',icon=':pineapple:')
        print(msg)
        #send_to_slack(msg,username='dataset-tracker',channel='@david',icon=':tophat:')

        # * Observation of a harvest should trigger a check of all
        # active harvested resources from that organization.
        # Any that are not reharvested should be noted and
        # a notification should be sent to Slack.





    store_resources_as_file(merged,server)

    # This seems like an important enough check to stick it in here,
    # but really maybe another function should be designed that
    # checks critical things and calls inventory and find_empty_tables
    # and whatever else should be checked regularly.
    if alerts_on:
        find_duplicate_packages(True,merged,alerts_on)
        find_empty_tables(merged,alerts_on)

    print("{} currently has {} and {}.".format(site,pluralize("dataset",packages),pluralize("resource",current_rows)))
    if return_data:
        return merged

def force_sizing():
    # This script prefers speedmode, since obtaining the dimensions for all the data tables and
    # sizes for some of the files takes (for some reason) routinely over half an hour (maybe one
    # request tends to hang for a long time), whereas speedmode requires only one request.
    # But sometimes we need to slowly go through and update a bunch of sizes. This is the
    # function that does that.
    inventory(False,False,False,True)

def upload():
    # Upload resource tracking data to a new CKAN resource under the given package ID.

    schema = ResourceTrackingSchema
    fields0 = schema().serialize_to_ckan_fields()
    # Eliminate fields that we don't want to upload.
    #fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
    #fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
    #fields0.append({'id': 'assignee', 'type': 'text'})
    fields_to_publish = fields0
    print("fields_to_publish = {}".format(fields_to_publish))

    print("site = {}, package_id = {}, API_key = {}".format(site,package_id,API_key))
    _, domain = site.split("://")
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': 'Tracking data on {} resources'.format(domain)}
    #else:
        #kwargs = {'resource_id': ''}

    target = PATH + "/resources.csv"
    testing = False
    if not testing:
        list_of_dicts = inventory(False,False,True,False)
    else: # Use the below entry for rapid testing (since it takes so long
          # to compile the real results.
        list_of_dicts = [{'package_id': 'Squornshellous Zeta', 'package_name': 'text',
            'organization': 'text', 'created': '2000-01-10T11:01:10.101010',
            'first_published': '2010-04-13T09:15:11.0',
            'first_seen': '2010-04-13T09:15:11.0', 'last_seen': '2010-04-13T09:15:11.0',
            'total_days_seen': 1, 'resource_id': 'Hypertext',
            'resource_name': 'sought it with forks', 'rows': 8, 'columns': 1502,
            'size': None, 'format': 'TSV', 'resource_url': 'https://zombo.com',
            'package_url': 'https://deranged.millionaire.com', 'groups': 'Limbo'}]

    #fields_to_publish = [{'id': 'package_id', 'type': 'text'}, {'id': 'package_name', 'type': 'text'}, {'id': 'organization', 'type': 'text'}, {'id': 'first_published', 'type': 'timestamp'}, {'id': 'first_seen', 'type': 'timestamp'}, {'id': 'last_seen', 'type': 'timestamp'}, {'id': 'total_days_seen', 'type': 'int'}, {'id': 'resource_id', 'type': 'text'}, {'id': 'resource_name', 'type': 'text'}, {'id': 'rows', 'type': 'int'}, {'id': 'columns', 'type': 'int'}, {'id': 'size', 'type': 'int'}, {'id': 'format', 'type': 'text'}]
    field_names = [x['id'] for x in fields_to_publish]

    write_to_csv(target,list_of_dicts,field_names)

    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,list(kwargs.values())[0],package_id,site))
    time.sleep(1.0)

    piping_method = 'upsert'

    t_pipeline = pl.Pipeline('tracking_pipeline',
                              'Tracking Pipeline',
                              log_status=False,
                              settings_file=SETTINGS_FILE,
                              settings_from_file=True,
                              start_from_chunk=0
                              ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['resource_id'],
              method=piping_method,
              **kwargs).run()

    log = open('uploaded.log', 'w+')
    if specify_resource_by_name:
        print("Piped data to {} on {} ({}).".format(kwargs['resource_name'],site,server))
        log.write("Finished upserting {} at {}\n".format(kwargs['resource_name'], datetime.now()))
    else:
        print("Piped data to {} on {} ({})".format(kwargs['resource_id'],site,server))
        log.write("Finished upserting {} at {}\n".format(kwargs['resource_id'], datetime.now()))
    log.close()

def prompt_for(input_field):
    try:
        text = raw_input(input_field+": ")  # Python 2
    except:
        text = input(input_field+": ")  # Python 3
    return text

def prompt_to_edit_field(d, base_prompt, field):
    new_value = prompt_for('{} ({})'.format(base_prompt, d[field]))
    if new_value == '':
        return d[field]
    else:
        return new_value

def add_datestamp(d,field_name):
    datestamp = prompt_for("{} [YYYY-MM-DD | Enter for now | 'None' for never]".format(field_name))
    if datestamp == 'None':
        d[field_name] = None
    elif datestamp == '':
        d[field_name] = datetime.strftime(datetime.now(),"%Y-%m-%dT%H:%M:%S.%f")
    else:
        d[field_name] = datetime.strftime(datetime.strptime(datestamp,"%Y-%m-%d"), "%Y-%m-%dT%H:%M:%S.%f")
    return d

def add(resource_id=None):
    resources = load(server)
    d = {'resource_id': resource_id}
    if resource_id is None:
        d['resource_id'] = prompt_for('Resource ID')

    if d['resource_id'] in [r['resource_id'] for r in resources]:
        print("There's already a resource under that ID. Try \n     > python track.py edit {}".format(d['resource_id']))
        return

    #d = add_datestamp(d,field_name)
    d['resource_name'] = prompt_for('resource_name')
    d['package_id'] = prompt_for('package_id')

    base_prompt = "Linking code (default: {})".format(d['resource_id'])
    linking_code = prompt_for('{}'.format(base_prompt))
    if linking_code == '':
        d['linking_code'] = str(d['resource_id'])

    d['package_name'] = prompt_for('package_name')
    d['organization'] = prompt_for('organization')
    d['resource_url'] = prompt_for('resource_url')
    d['package_url'] = prompt_for('package_url')
    d['format'] = prompt_for('format')
    d['comments'] = 'Manually added'

    resources = [d] + resources
    store(resources,server)
    print('"{}" was added to the resources being tracked.'.format(d['resource_name']))

def edit(resource_id=None):
    resources = load(server)
    if resource_id is None:
        print("You have to specify the ID of an existing resource to edit.")
        resource_id = prompt_for('Enter the resource ID')
    ids = [r['resource_id'] for r in resources]
    while resource_id not in ids:
        print("There's no resource under that ID. Try again.")
        resource_id = prompt_for('Enter the ID of the resource you want to edit')

    index = ids.index(resource_id)
    r = resources[index]
#    r['first_published'] = prompt_to_edit_field(r,'First published','first_published')
#    r['period_in_days'] = float(prompt_to_edit_field(p,'Period in days','period_in_days'))
#
    base_prompt = "First published [YYYY-MM-DD | 'now' | 'None' for never]"
    field = 'first_published'
    first_published = prompt_for('{} ({})'.format(base_prompt, r[field]))
    if first_published != '':
        if first_published == 'None':
            r['first_published'] = None
        elif first_published == 'now':
            r['first_published'] = datetime.strftime(datetime.now(),"%Y-%m-%dT%H:%M:%S.%f")
        else:
            r['first_published'] = datetime.strftime(datetime.strptime(first_published,"%Y-%m-%d"), "%Y-%m-%dT%H:%M:%S.%f")
    # resources has now been updated since r points to the corresponding element in resources.
    store(resources,server)
    print('"{}" has been edited.'.format(r['resource_id']))

def refresh_csv():
    resources = load(server)
    store_resources_as_file(resources,server)

server = "test-production"
#server = "sandbox"
with open(SETTINGS_FILE) as f:
    settings = json.load(f)
    API_key = settings["loader"][server]["ckan_api_key"]
    site = settings["loader"][server]["ckan_root_url"]
    package_id = settings["loader"][server]["package_id"]

if __name__ == '__main__':
    try:
        if len(sys.argv) == 1:
            inventory() # Make this the default.
        else:
            fire.Fire()
    except:
        e = sys.exc_info()[0]
        msg = "Error: {} : \n".format(e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        msg = ''.join('!! ' + line for line in lines)
        print(msg) # Log it or whatever here
        send_to_slack(msg,username='dataset-tracker',channel='@david',icon=':illuminati:')
