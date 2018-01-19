import os, sys, re, csv, time, itertools, textwrap, ckanapi, random
import requests
import fire

from datetime import datetime, timedelta
from pprint import pprint
from json import loads, dumps
import json
from collections import OrderedDict, defaultdict
from parameters.local_parameters import SETTINGS_FILE, PATH
from notify import send_to_slack

#abspath = os.path.abspath(__file__)
#dname = os.path.dirname(abspath)
#os.chdir(dname)
#local_path = dname+"/latest_pull"
## If this path doesn't exist, create it.
#if not os.path.exists(local_path):
#    os.makedirs(local_path)

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

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

def get_resources_filepath(server):
    return "{}/resources-{}.json".format(PATH,server)

def load_resources_from_file(server):
    resources_filepath = get_resources_filepath(server)
    if os.path.exists(resources_filepath):
        with open(resources_filepath,'r',encoding='utf-8') as f:
            resources = loads(f.read())
        return resources
    else:
        return []

def store_resources_as_file(rs,server,field_names_seed=None):
    resources_filepath = get_resources_filepath(server)
    with open(resources_filepath,'w',encoding='utf-8') as f:
        f.write(dumps(rs, indent=4))

    if field_names_seed is not None:
        field_names = field_names_seed
    else:
        # Make sure to get every possible field name (since some
        # JSON rows lack some fields).
        lists_of_field_names = [r.keys() for r in rs]
        field_names = list(set(itertools.chain.from_iterable(lists_of_field_names)))
    print("field_names = {}".format(field_names))
    target = PATH + "/resources.csv"
    write_to_csv(target,rs,field_names)

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
    except:
        return None

    return count

def get_schema(site,resource_id,API_key=None):
    # On later versions of CKAN, it should be possible to do this using the 
    # datastore_info endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
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

def size_estimate(resource,old_tracks):
    if resource['format'] in ['HTML','html']:
        r_name = name_of_resource(resource)
        download_url = download_url_of_resource(resource)
        #print("Not saving a file size estimate since {} ({}) appears to be a link to a web page".format(r_name,download_url))
        return None
    if 'url' in resource:
        url = resource['url']
        resource_id = resource['id'] # 'id', not 'resource_id' 
                  # since this is still the raw CKAN response.
    elif 'download_url' in resource:
        url = resource['download_url']
        resource_id = resource['resource_id']
    else:
        return None

    if url == 'http://#': # Handle local convention for 
        return None       # disabling downloading of big 
                          # tables in the datastore.
                                          
    response = requests.head(url)
    #print("response.headers = {}".format(response.headers))
    if response.status_code in [404]:
        return None
    if 'Content-Range' in response.headers:
        estimate = int(response.headers['Content-Range'].split('/')[1])
        return estimate
    elif 'Content-Length' in response.headers:
        return int(response.headers['Content-Length'])
    else:
        #print("Unable to identify the size of the transferred file from these headers: {}".format(response.headers))
        # I believe that almost all files in this category are files that need to be dumped from datastore tables.

        # Determine whether size is known from old_tracks.
        size_is_known = False
        for t in old_tracks:
            if t['resource_id'] == resource_id: 
                if 'size' in t and t['size'] is not None:
                    size_is_known = True
        if random.random() < 0.1 - 0.05*size_is_known:
            # Actually fetch the resource and determine the file size.
            print("Getting {} to determine its file size.".format(url))
            r2 = requests.get(url)
            if 'Content-Range' in r2.headers:
                estimate = int(r2.headers['Content-Range'].split('/')[1])
                print("   Content-Range: Determined {} to have a size of {}.".format(resource_id,estimate))
                return estimate
            elif 'Content-Length' in r2.headers:
                estimate = int(r2.headers['Content-Length'])
                print("   Content-Length: Determined {} to have a size of {}.".format(resource_id,estimate))
                return estimate
            elif 'Content-Type' in r2.headers and r2.headers['Content-Type'] == 'text/csv':
                estimate = len(r2.text)
                print("   len(r2.text): Determined {} to have a size of {}.".format(resource_id,estimate))
                return estimate
            print("Unable to identify the size of the transferred file from the HEAD headers {} or from the GET headers".format(response.headers,r2.headers))

        return resource['size'] # I think this should work both for CKAN API response resources and tracks.

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


    groups_string = sort_and_stringify_groups(package)
    tags_string = sort_and_stringify_field(package,'tags','name')
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
        ('first_seen',datetime.now().isoformat()),
        ('last_seen',datetime.now().isoformat()),
        ('total_days_seen',1),
        ('rows',rows),
        ('columns',columns),
        ('size',None if speedmode else size_estimate(resource,old_tracks)),
        ('loading_method',loading_method),
        ('format',resource['format']),
        ('tags',tags_string),
        ('groups',groups_string)]

    return OrderedDict(r_tuples)

def update(record,x):
    assert record['resource_id'] == x['resource_id']
    assert record['package_id'] == x['package_id']
    assert record['created'] == x['created']
    # The linking code is presumed to be immutable, based on how it's being defined.
    modified_record = OrderedDict(record)
    last_seen = datetime.strptime(record['last_seen'],"%Y-%m-%dT%H:%M:%S.%f")
    now = datetime.now()
    modified_record['last_seen'] = now.isoformat()
    if last_seen.date() != now.date():
        modified_record['total_days_seen'] += 1

    # Update row counts, column counts, etc.
    modified_record['resource_name'] = x['resource_name'] # Keep resource names updated.
    modified_record['resource_url'] = x['resource_url']
    modified_record['linking_code'] = x['linking_code']
    # The package name could easily change, so these URLs need to be updated.
    modified_record['package_url'] = x['package_url'] 
    modified_record['download_url'] = x['download_url']
    modified_record['download_link_status'] = x['download_link_status']
    modified_record['rows'] = x['rows'] if x['rows'] is not None else record['rows']
    modified_record['columns'] = x['columns'] if x['columns'] is not None else record['columns']
    modified_record['size'] = x['size'] if x['size'] is not None else record['size'] # Only update the
    # 'size' field if a new value has been obtained.
    modified_record['loading_method'] = x['loading_method']
    modified_record['format'] = x['format']
    modified_record['tags'] = x['tags']
    modified_record['groups'] = x['groups']
    return modified_record

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
        msg = "{} unnamed resources found:" + ", ".join(items)
        print("\n"+msg)

def check_all_unknown_sizes(tracks=None):
    if tracks is None:
        tracks = load_resources_from_file(server)
    updated_something = False
    for k,r in enumerate(tracks):
        if 'size' not in r or r['size'] in [None]:
            estimate = size_estimate(r,tracks)
            if estimate is None:
                print("The size of {} hadn't been determined and still can't be determined. It's listed as being in the {} format. Here's the download URL: {}".format(r['resource_id'], r['format'], r['resource_url']))
            else:
                print("The size of {} wasn't previously determined. It's listed as being in the {} format. It looks like it's actually {}".format(r['resource_id'], r['format'], estimate))
                updated_something = True
    if updated_something:
        store_resources_as_file(tracks,server)
        print("One or more resource sizes were updated.")

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
    pprint(dict(license_counts))
    if len(items) > 0:
        msg = "{} packages without licenses found: ".format(len(items), ", ".join(items))
        print(msg)
        # These tend to be harvested packages.
        nonharvested = []
        for p in unlicensed:
            if not is_harvested_package(p):
                nonharvested.append(p['title'])
        if len(items) == 1:
            msg = "This single package is particularly interesting because it is a non-harvested packages without a license: {}".format(len(nonharvested),", ".join(nonharvested))
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
        msg = "{} resources with non-standard formats found: {}".format(len(items), ", ".join(items))
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
                response = requests.head(durl)
                # 405 Method Not Allowed (the server refuses to respond to a HEAD request.)
                if response.status_code == 405:
                    response = requests.get(durl)
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
        msg = "{} dead links found:" + ", ".join(items)

    store_resources_as_file(tracks,server)


def check_all(tracks=None):
    tracks = load_resources_from_file(server)
    list_unnamed(tracks)
    check_all_unknown_sizes(tracks)
    check_formats(tracks)
    check_links(tracks)
    check_live_licenses()

def linking_code_template(datum):
    return datum['package_id'] + ' | ' + datum['resource_name']

def generate_linking_code(tracked_resource):
    # This function generates effective IDs to link together different resources that contain 
    # the same information. (There are a bunch of these because of how the CKAN harvest 
    # extension works.)

    if 'loading_method' in tracked_resource and tracked_resource['loading_method'] == 'harvested':
        code = linking_code_template(tracked_resource)
        return code
    elif 'comments' not in tracked_resource or tracked_resource['comments'] != 'Manually added':
        return tracked_resource['resource_id']
    else:
        assert 'linking_code' in tracked_resource
        code = tracked_resource['linking_code']
        if 'loading_method' in tracked_resource:
            if tracked_resource['loading_method'] == 'harvested':
                return code, code
            else:
                return code, None # Both of these lines might
        return code, None # be wrong... If the loading_method is undefined
        # or blank it might or might not be a harvested thing (if there's
        # some old or incorrectly tagged resource in there.)
        
def inventory(speedmode=False,return_data=False,sizing_override=False):
    ckan = ckanapi.RemoteCKAN(site) # Without specifying the apikey field value,
    # the next line will only return non-private packages.
    packages = ckan.action.current_package_list_with_resources(limit=999999) 
    # This is a list of all the packages with all the resources nested inside and all the current information.
  
#    old_data = load_resource_archive(site,API_key)
    old_data = load_resources_from_file(server)
    old_resource_ids = [r['resource_id'] for r in old_data]
    resources = []
    list_of_odicts = []
    
    
    
    old_package_names = [r['package_name'] for r in old_data]
    old_package_ids = [r['package_id'] for r in old_data]
    package_ids = [p['id'] for p in packages]
    #print("len(package_ids) = {}. There are {} unique package IDs.".format(len(package_ids),len(set(package_ids))))

    #    if p['title'] not in old_package_names:
    #        print("{} ({}) is not being tracked.".format(p['title'],p['id']))

    # The above code only prints 
    # City of Pittsburgh Signalized Intersections (f470a3d5-f5cb-4209-93a6-c974f7d5a0a4) is not being tracked.
    # but it actually is being tracked. 

    for p in packages:
        if p['id'] not in old_package_ids:
            print("{} ({}) is not being tracked.".format(p['title'],p['id']))


    for p in packages:
        resources += p['resources'] #
        for r in p['resources']:
            new_row = extract_features(p,r,old_data,speedmode,sizing_override)
            linking_code = generate_linking_code(new_row)
            new_row['linking_code'] = linking_code
            list_of_odicts.append(new_row)
            print(".", end="", flush=True)
   
    merged = [] 
    processed_new_ids = []
    new_rows = list_of_odicts # new_rows, harvest_linking_codes = fetch_live_resources(...)
    print("len(list_of_odicts)) = {}".format(len(list_of_odicts)))
    print("len(new_rows) = {}".format(len(new_rows)))
    new_resource_ids = [r['resource_id'] for r in new_rows]
    old_harvest_linking_codes = []
    for datum in old_data:
        old_id = datum['resource_id']
        if 'loading_method' in datum and datum['loading_method'] == 'harvested':
            if 'linking_code' in datum:
                if datum['linking_code'] is not None:
                    old_harvest_linking_codes.append(datum['linking_code'])
            else:
                # Generate one.
                harvest_linking_code = generate_linking_code(datum)
                if harvest_linking_code is not None:
                    old_harvest_linking_codes.append(harvest_linking_code)

        if old_id not in new_resource_ids:
            #print("Adding the following resource: {} | {} | {}".format(old_id,datum['resource_name'],datum['organization']))
            merged.append(datum)
        else: # A case where an existing record needs to be 
        # updated has been found.
            x = new_rows[new_resource_ids.index(old_id)]
            modified_datum = update(datum,x)
            merged.append(modified_datum)
            processed_new_ids.append(old_id)

    print("len(merged) = {}".format(len(merged)))
    old_harvest_linking_codes = list(set(old_harvest_linking_codes))
    print("len(processed_new_ids) = {}".format(len(processed_new_ids)))
    brand_new = []
    reharvest_count = 0
    new_package_ids = []
    for new_row in new_rows:
        if new_row['resource_id'] not in processed_new_ids:
            # These are new resources that haven't ever been added or tracked.

            # However, harvested resources that have new resource IDs but are otherwise the same as previous resources need to be identified.
            reharvested = False
            if new_row['loading_method'] == 'harvested':
                if new_row['linking_code'] in old_harvest_linking_codes:
                    reharvested = True

            if reharvested:
                reharvest_count += 1
            else:
                item = "<{}|{}> in {} from {}".format(new_row['resource_url'],new_row['resource_name'],new_row['package_name'],new_row['organization'])
                printable = "{} ({}) in {} from {}".format(new_row['resource_name'],new_row['resource_url'],new_row['package_name'],new_row['organization'])
                brand_new.append(item)
                if datetime.now() - datetime.strptime(new_row['created'],"%Y-%m-%dT%H:%M:%S.%f") < timedelta(days = 6):
                    new_row['first_published'] = new_row['created'] 
                else: # Some resources were created long ago and only recently published.
                    new_row['first_published'] = new_row['first_seen'] 

                new_package_ids.append(new_row['package_id'])
                msg = "dataset-tracker found an entirely new resource: " + printable
                print(msg)
                merged.append(new_row)
    
    new_package_ids = list(set(new_package_ids))
    if len(brand_new) > 0:
        if len(brand_new) == 1:
            msg = "dataset-tracker found an entirely new resource: " + brand_new[0]
        else:
            msg = "In {} new packages, dataset-tracker found these {} entirely new resources: ".format(len(new_package_ids),len(brand_new))
            msg += ', '.join(brand_new)
        send_to_slack(msg,username='dataset-tracker',channel='#new-resources',icon=':tophat:')


    #if reharvest_count > 0:
    #    msg = "dataset-tracker observed that {} resources were reharvested.".format(reharvest_count)
    #    send_to_slack(msg,username='dataset-tracker',channel='#notifications',icon=':tophat:')
    #    send_to_slack(msg,username='dataset-tracker',channel='@david',icon=':tophat:')
# [ ] Debug this one!
    store_resources_as_file(merged,server,new_rows[0].keys())
    assert len(resources) == len(list_of_odicts) # We might not need both.
    print("{} currently has {} datasets and {} resources.".format(site,len(packages),len(resources)))
    if return_data:
        return merged

def force_sizing():
    # This script prefers speedmode, since obtaining the dimensions for all the data tables and
    # sizes for some of the files takes (for some reason) routinely over half an hour (maybe one
    # request tends to hang for a long time), whereas speedmode requires only one request.
    # But sometimes we need to slowly go through and update a bunch of sizes. This is the 
    # function that does that.
    inventory(False,False,True)

def upload():
    # Upload resource tracking data to a new CKAN resource under the given package ID.
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
        rows = fields.Integer(allow_none=True)
        columns = fields.Integer(allow_none=True)
        size = fields.Integer(allow_none=True)
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
        list_of_dicts = inventory(False,True,False)
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
        log.write("Finished upserting {}\n".format(kwargs['resource_name']))
    else:
        print("Piped data to {} on {} ({})".format(kwargs['resource_id'],site,server))
        log.write("Finished upserting {}\n".format(kwargs['resource_id']))
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

server = "test-production"
#server = "sandbox"
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
