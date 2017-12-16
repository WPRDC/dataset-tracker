import os, sys, csv, time, textwrap, ckanapi
import fire

from datetime import datetime, timedelta
from pprint import pprint
from json import loads, dumps
import json
from collections import OrderedDict
from parameters.local_parameters import SETTINGS_FILE, PATH

#abspath = os.path.abspath(__file__)
#dname = os.path.dirname(abspath)
#os.chdir(dname)
#local_path = dname+"/latest_pull"
## If this path doesn't exist, create it.
#if not os.path.exists(local_path):
#    os.makedirs(local_path)

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
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
        with open(resources_filepath,'r') as f:
            resources = loads(f.read())
        return resources
    else:
        return []

def store_resources_as_file(rs,server):
    resources_filepath = get_resources_filepath(server)
    with open(resources_filepath,'w') as f:
        f.write(dumps(rs, indent=4))

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

def stringify_groups(p):
    groups_string = ''
    if 'groups' in p:
        groups = p['groups']
        groups_string = '|'.join(set([g['title'] for g in groups]))
    return groups_string

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
    package_url_path = "/dataset/" + package['name']
    package_url = site + package_url_path
    resource_url_path = package_url_path + "/resource/" + resource['id']
    resource_url = site + resource_url_path

    groups_string = stringify_groups(package)
    r_tuples = [('resource_name',resource_name),
        ('resource_id',resource['id']),
        ('package_name',package['title']),
        ('package_id',resource['package_id']),
        ('organization',package['organization']['title']),
        ('resource_url',resource_url),
        ('package_url',package_url),
        ('created',resource['created']),
        ('first_published',None),
        ('first_seen',datetime.now().isoformat()),
        ('last_seen',datetime.now().isoformat()),
        ('total_days_seen',1),
        ('rows',rows),
        ('columns',columns),
        ('size',resource['size']),
        ('format',resource['format']),
        ('groups',groups_string)]

    return OrderedDict(r_tuples)

def update(record,x):
    assert record['resource_id'] == x['resource_id']
    assert record['package_id'] == x['package_id']
    assert record['created'] == x['created']
    modified_record = OrderedDict(record)
    last_seen = datetime.strptime(record['last_seen'],"%Y-%m-%dT%H:%M:%S.%f")
    now = datetime.now()
    modified_record['last_seen'] = now.isoformat()
    if last_seen.date() != now.date():
        modified_record['total_days_seen'] += 1

    # Update row counts, column counts, etc.
    modified_record['resource_url'] = x['resource_url']
    # The package name could easily change, so these URLs need to be updated.
    modified_record['package_url'] = x['package_url'] 
    modified_record['rows'] = x['rows']
    modified_record['columns'] = x['columns']
    modified_record['size'] = x['size'] # Currently CKAN is always 
    # returning a 'size' value of null.
    modified_record['format'] = x['format']
    modified_record['groups'] = x['groups']
    return modified_record

def inventory():
    ckan = ckanapi.RemoteCKAN(site) # Without specifying the apikey field value,
    # the next line will only return non-private packages.
    packages = ckan.action.current_package_list_with_resources(limit=999999) 
    # This is a list of all the packages with all the resources nested inside and all the current information.
   
#    old_data = load_resource_archive(site,API_key)
    old_data = load_resources_from_file(server)
    old_resource_ids = [r['resource_id'] for r in old_data]
    resources = []
    list_of_odicts = []
    print("=== Printing resources with non-standard formats ===")
    standard_formats = ['CSV','HTML','ZIP','GeoJSON','Esri REST','KML',
        'PDF','XLSX','XLS','TXT','DOCX','JSON','XML','RTF','GIF','API']
    for p in packages:
        resources += p['resources']
        for r in p['resources']:
            new_row = extract_features(p,r)
            list_of_odicts.append(new_row)
            if new_row['format'] not in standard_formats:
                # ['.csv','csv','',' ','.html','html','.xlsx','.zip','.xls',None,'None','pdf','.pdf']:
                print("{}: {}".format(new_row['resource_name'],new_row['format']))
   
    merged = [] 
    processed_new_ids = []
    new_rows = list_of_odicts
    print("len(new_rows) = {}".format(len(new_rows)))
    new_resource_ids = [r['resource_id'] for r in new_rows]
    for datum in old_data:
        old_id = datum['resource_id']
        if old_id not in new_resource_ids:
            #print("Adding the following resource: {} | {} | {}".format(old_id,datum['resource_name'],datum['organization']))
            merged.append(datum)
        else: # A case where an existing record needs to be 
        # updated has been found.
            x = new_rows[new_resource_ids.index(old_id)]
            modified_datum = update(datum,x)
            merged.append(modified_datum)
            processed_new_ids.append(old_id)

    print("len(processed_new_ids) = {}".format(len(processed_new_ids)))
    for new_row in new_rows:
        if new_row['resource_id'] not in processed_new_ids:
            # These are new resources that haven't ever been added or tracked.
            print("Found an entirely new resource: {} | {} | {}".format(new_row['resource_id'],new_row['resource_name'],new_row['organization']))
            merged.append(new_row)
                
    store_resources_as_file(merged,server)
    print("{} currently has {} datasets and {} resources.".format(site,len(packages),len(resources)))
    field_names = new_rows[0].keys()
    target = PATH + "/resources.csv"
    write_to_csv(target,merged,field_names)
    return merged

    #wobbly_ps_sorted = sorted(wobbly_plates, 
    #                        key=lambda u: -u['cycles_late'])
    #print("\nPlates by Wobbliness: ")
    #print_table(wobbly_ps_sorted)

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
        organization =  fields.String(allow_none=False)
        resource_url = fields.String(allow_none=False)
        package_url = fields.String(allow_none=False)
        created = fields.DateTime(allow_none=True)
        first_published = fields.DateTime(allow_none=True)
        first_seen = fields.DateTime(default=datetime.now().isoformat(),allow_none=True)
        last_seen = fields.DateTime(dump_only=True,dump_to='last_seen',default=datetime.now().isoformat())
        total_days_seen = fields.Integer(allow_none=True)
        rows = fields.Integer(allow_none=True)
        columns = fields.Integer(allow_none=True)
        size = fields.Integer(allow_none=True)
        _format = fields.String(dump_to='format',allow_none=False)

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
        list_of_dicts = inventory()
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
