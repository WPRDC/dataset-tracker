import csv, time, json, ckanapi

try:
    from urlparse import urlparse # Python 2
except:
    from urllib.parse import urlparse # Python 3 renamed urlparse.


def get_package_parameter(site,package_id,parameter=None,API_key=None):
    """Gets a CKAN package parameter. If no parameter is specified, all metadata
    for that package is returned."""
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.package_show(id=package_id)
    if parameter is None:
        return metadata
    else:
        if parameter in metadata:
            return metadata[parameter]
        else:
            return None

def set_package_parameters_to_values(site,package_id,parameters,new_values,API_key,mute=False):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    original_values = [get_package_parameter(site,package_id,p,API_key) for p in parameters]
    payload = {}
    payload['id'] = package_id
    for parameter,new_value in zip(parameters,new_values):
        payload[parameter] = new_value
    results = ckan.action.package_patch(**payload)
    if not mute:
        print("Changed the parameters {} from {} to {} on package {}".format(parameters, original_values, new_values, package_id))

def add_tag(site, API_key, package, tag='_harvested'):
    """Check that the package has the desired tag. If not, add it."""
    tag_dicts = package['tags']
    tags = [td['name'] for td in tag_dicts]
    if tag not in tags:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        new_tag_dict = {'name': tag}
        tag_dicts.append(new_tag_dict)
        set_package_parameters_to_values(site,package['id'],['tags'],[tag_dicts],API_key,True)
        return True

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        #for d in list_of_dicts:
        #    dict_writer.writerow({k:v.encode('utf8') if type(v) == type('A') else v for k,v in d.items()})
        dict_writer.writerows(list_of_dicts)

def get_site(settings,server):
    # From the dictionary obtained from ckan_settings.json,
    # extract the URL for a particular CKAN server and return it.
    url = settings["URLs"][server]["CKAN"]
    scheme = urlparse(url).scheme
    hostname = urlparse(url).hostname
    return "{}://{}".format(scheme,hostname)

def open_a_channel(settings_file,server):
    # Get parameters to communicate with a CKAN instance
    # from the specified JSON file.
    with open(settings_file) as f:
        settings = json.load(f)
        API_key = settings["loader"][server]["ckan_api_key"]
        site = settings["loader"][server]["ckan_root_url"]
        package_id = settings['loader'][server]['package_id']
    return site, API_key, package_id, settings

def get_resource_data(site,resource_id,API_key=None,count=50,offset=0,fields=None):
    # Use the datastore_search API endpoint to get <count> records from
    # a CKAN resource starting at the given offset and only returning the
    # specified fields in the given order (defaults to all fields in the
    # default datastore order).
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    if fields is None:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset)
    else:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset, fields=fields)
    # A typical response is a dictionary like this
    #{u'_links': {u'next': u'/api/action/datastore_search?offset=3',
    #             u'start': u'/api/action/datastore_search'},
    # u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'limit': 3,
    # u'records': [{u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}],
    # u'resource_id': u'd1e80180-5b2e-4dab-8ec3-be621628649e',
    # u'total': 88232}
    data = response['records']
    return data

def get_number_of_rows(site,resource_id,API_key=None):
    # This is pretty similar to get_fields and DRYer code might take
    # advantage of that.

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

def get_all_records(site,resource_id,API_key=None,chunk_size=5000):
    all_records = []
    failures = 0
    k = 0
    offset = 0 # offset is almost k*chunk_size (but not quite)
    row_count = get_number_of_rows(site,resource_id,API_key)
    print("row count = {}".format(row_count))
    if row_count == 0: # or if the datastore is not active
       print("No data found in the datastore.")
       success = False
    while len(all_records) < row_count and failures < 5:
        time.sleep(0.01)
        try:
            records = get_resource_data(site,resource_id,API_key,chunk_size,offset)
            if records is not None:
                all_records += records
            failures = 0
            offset += chunk_size
        except:
            failures += 1

        # If the number of rows is a moving target, incorporate
        # this step:
        #row_count = get_number_of_rows(site,resource_id,API_key)
        k += 1
        print("{} iterations, {} failures, {} records, {} total records".format(k,failures,len(records) if records is not None else 0,len(all_records)))

        # Another option for iterating through the records of a resource would be to
        # just iterate through using the _links results in the API response:
        #    "_links": {
        #  "start": "/api/action/datastore_search?limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1",
        #  "next": "/api/action/datastore_search?offset=5&limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"
        # Like this:
            #if r.status_code != 200:
            #    failures += 1
            #else:
            #    URL = site + result["_links"]["next"]

        # Information about better ways to handle requests exceptions:
        #http://stackoverflow.com/questions/16511337/correct-way-to-try-except-using-python-requests-module/16511493#16511493

    return all_records

def get_fields(site,resource_id,API_key=None):
    # In principle, it should be possible to do this using the datastore_info
    # endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
        fields = [d['id'] for d in schema]
    except:
        return None

    return fields

def dealias(site,pseudonym):
    # If a resource ID is an alias for the real resource ID, this function will
    # convert the pseudonym into the real resource ID and return it.
    ckan = ckanapi.RemoteCKAN(site)
    alias_response = ckan.action.datastore_search(id='_table_metadata',filters={'name': pseudonym})
    aliases = alias_response['records']
    if len(aliases) > 0:
        resource_id = aliases[0]['alias_of']
        return resource_id
    else:
        return None

def resource_show(ckan,resource_id):
    # A wrapper around resource_show (which could be expanded to any resource endpoint)
    # that tries the action, and if it fails, tries to dealias the resource ID and tries
    # the action again.
    try:
        metadata = ckan.action.resource_show(id=resource_id)
    except ckanapi.errors.NotFound:
        # Maybe the resource_id is an alias for the real one.
        real_id = dealias(site,resource_id)
        metadata = ckan.action.resource_show(id=real_id)
    except:
        msg = "{} was not found on that CKAN instance".format(resource_id)
        print(msg)
        raise ckanapi.errors.NotFound(msg)

    return metadata

def get_metadata(site,resource_id,API_key=None):
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = resource_show(ckan,resource_id)
    except:
        return None

    return metadata
