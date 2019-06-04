import os, json, time
from datetime import datetime

from parameters.local_parameters import BACKUP_PATH, SETTINGS_FILE
from gadgets import open_a_channel, write_to_csv, get_all_records, get_site, get_fields, get_metadata


def download_resource_file(resource_record,destination_folder):
    print("This function is only designed to download from datastores.")

    # This function pulls information from a particular resource on a
    # CKAN instance and outputs it to a CSV file.

    # Tests suggest that there are some small differences between this
    # file and the one downloaded directly through the CKAN web
    # interface: 1) the row order is different (this version is ordered
    # by _id (logically) while the downloaded version has the same weird
    # order as that shown in the Data Explorer (which can have an _id
    # sequence like 1,2,3,4,5,6,7,45,8,9,10... for no obvious reason
    # (it may be that the data is being sorted by the order they appear
    # in the database, which may be by their time of last update...))
    # and 2) weird non-ASCII characters are not being handled optimally,
    # so probably some work on character encoding is in order in the
    # Python script.

    server ='test-production'
    site, API_key, _, _ = open_a_channel(SETTINGS_FILE, server) # API_key would only be necessary for downloading private datasets.

    r_id = resource_record['resource_id']
    filename = "{}.csv".format(r_id)

    #script_path = os.path.realpath(__file__)
    #base_dir = '/'.join(script_path.split('/')[:-1])
    #path = base_dir + '/tmp/'
    path = destination_folder
    if not os.path.exists(path):
        os.mkdir(path)

    list_of_dicts = get_all_records(site, r_id, API_key, chunk_size=5000)
    print("len(list_of_dicts) = {}".format(len(list_of_dicts)))
    fields = get_fields(site,r_id,API_key)
    print("len(fields) = {}".format(len(fields)))
    metadata = get_metadata(site,r_id,API_key)

    #Eliminate _id field
    fields.remove("_id")
    print("The resource has the following fields: {}".format(fields))
    filepath = path + filename
    write_to_csv(filepath,list_of_dicts,fields)
    metaname = filename + '-metadata.json'
    metapath = path + metaname
    with open(metapath, 'w', encoding='utf-8') as outfile:
        json.dump(metadata, outfile, indent=4, sort_keys=True)

    return filepath, metapath

def backup_resource_to_folder(resource_record,destination_folder):
    """This function looks at the resource and downloads its data
    from the CKAN instance to the local folder."""

    uploaded_files = []
    file_path, metadata_file_path = download_resource_file(resource_record,destination_folder)
    # https://github.com/box/box-python-sdk/blob/master/docs/usage/files.md#upload-a-file
    assert file_path != metadata_file_path
    return [uploaded_file, uploaded_metadata_file]

def create_folder(path):
    if not os.path.exists(path):
        print("Creating {}".format(current_path))
        os.makedirs(current_path)

def backup_to_disk(resource_record):
    # Check whether last_backed_up < resource_record['last_modified'] (the parameter that I think indicates when the resource was last modified)
    # Extract 1) resource file and 2) metadata as JSON file
    # Store in timestamped folder
        # backups/data.wprdc.org/<package_id>/<resource_id>/<timestamp>/
    resource_id = resource_record['resource_id']
    package_id = resource_record['package_id']
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    root_folder = BACKUP_PATH
    resource_folder = "{}{}/{}/{}".format(root_folder,'data.wprdc.org',package_id,resource_id)
    destination_folder = "{}/{}/".format(resource_folder,timestamp)
    create_folder(destination_folder)
    filepaths = backup_resource_to_folder(resource_record, destination_folder)
    # Update last_backed_up field

    return resource_record

if __name__ == '__main__':
    # As a test, back up the Library Locations resource.
    resource_record = {'resource_id': '14babf3f-4932-4828-8b49-3c9a03bae6d0',
            'package_id': '5d52127f-3606-4ed9-a019-1daf4381902a'}
    print(backup_to_disk(resource_record))
