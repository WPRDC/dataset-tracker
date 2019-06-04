import os, json, time
from datetime import datetime
from boxsdk import OAuth2, Client #, JWTAuth

from parameters.local_parameters import SETTINGS_FILE #, BOX_AUTH_SETTINGS_FILE
from gadgets import open_a_channel, write_to_csv, get_all_records, get_site, get_fields, get_metadata


from parameters.credentials import YOUR_CLIENT_ID, YOUR_CLIENT_SECRET, YOUR_DEVELOPER_TOKEN
auth = OAuth2(
    client_id=YOUR_CLIENT_ID,
    client_secret=YOUR_CLIENT_SECRET,
    access_token=YOUR_DEVELOPER_TOKEN,
)
client = Client(auth)

user = client.user().get()

# As usual, authentication is complex. One option is a "service account" which by default stores files
# in a place other than user accounts. However, one can make the service account a collaborator on
# a folder in one's Box account:

# https://community.box.com/t5/Platform-and-Development-Forum/How-to-add-service-account-as-collaborator-on-user-folder/td-p/61445
#auth = JWTAuth.from_settings_file(BOX_AUTH_SETTINGS_FILE)
#client = Client(auth)
#service_account = client.user().get()

# The service-account/JWT authentication is useless because it requires that the enterprise
# admin business console settings allow authorization of apps
#       https://developer.box.com/docs/setting-up-a-jwt-app
# which is not allowed for this programmer's account.
# This leaves OAuth options or manually generating and adding a 1-hour developer token.
# Both options require too much manual interaction.

def create_box_folder(client,folder,folder_name):
# https://github.com/box/box-python-sdk/blob/master/docs/usage/folders.md#create-a-folder
    """A folder can be created by calling folder.create_subfolder(name) on the
    parent folder with the name of the subfolder to be created. This method
    returns a new Folder representing the created subfolder."""
    #subfolder = client.folder('0').create_subfolder(folder_name) # Create sub-folder of parent folder
    subfolder = client.folder(folder_id=folder.id).create_subfolder(folder_name) # Create sub-folder of parent folder
    return subfolder

def get_all_items_in_folder(folder):
    """The Box SDK has a limitation; get_items calls must include a limit parameter, and
    apparently no more than 1000 items can be retrieved in a call. This function works
    around this limit."""
    all_items = []
    offset = 0
    next_batch = ['cookie1', 'monster2']
    limit = 1000
    while len(next_batch) > 0: # We could also use the total_count parameter to figure out whether
        # all items have been obtained yet.
        next_batch = client.folder(folder_id=folder.id).get_items(limit=limit,offset=offset)
        all_items += next_batch
        offset += len(next_batch)
    return all_items

def find_item_in_folder(folder,item_name,create_if_missing=False):
    items = get_all_items_in_folder(folder)
    for item in items:
        if item.name == item_name:
            return item
    if create_if_missing:
        new_folder = create_box_folder(client,folder,item_name)
        return new_folder
    else:
        return None

def find_folder_from_path(current_folder,path_parts):
    if len(path_parts) == 0:
        return current_folder
    next_folder = find_item_in_folder(current_folder,path_parts[0],create_if_missing=True)
    return find_folder_from_path(next_folder,path_parts[1:])

def download_resource_file(resource_record,filename=None):
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
    if filename is None:
        filename = "{}.csv".format(r_id)

    script_path = os.path.realpath(__file__)
    base_dir = '/'.join(script_path.split('/')[:-1])
    path = base_dir + '/tmp/'
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

def upload_resource_to_box(destination_folder,resource_record):
    """This function looks at the resource and decides how to get its data
    from the CKAN instance to Box. In some cases, this might mean downloading the
    file to a temporary file and then uploading it. In other cases the file URL
    from CKAN might be providable more directly to Box."""

    uploaded_files = []
    file_path, metadata_file_path = download_resource_file(resource_record)
    # https://github.com/box/box-python-sdk/blob/master/docs/usage/files.md#upload-a-file
    assert file_path != metadata_file_path
    uploaded_file = destination_folder.upload(file_path, file_name=None, preflight_check=False, preflight_expected_size=0)
    uploaded_metadata_file = destination_folder.upload(metadata_file_path, file_name=None, preflight_check=False, preflight_expected_size=0)
    # By default, the file uploaded to Box will have the same file name as the
    # one on disk; you can override this by passing a different name in the
    # file_name parameter. You can, optionally, also choose to set a file
    # description upon upload by using the file_description parameter. This
    # method returns a File object representing the newly-uploaded file.

    # [ ] To upload a file from a readable stream, call folder.upload_stream(file_stream, file_name, file_description=None, preflight_check=False, preflight_expected_size=0) with the stream and a name for the file.

    # For large files or in cases where the network connection is less reliable, you may want to upload the file in parts. This allows a single part to fail without aborting the entire upload, and failed parts can then be retried.
    # See: https://github.com/box/box-python-sdk/blob/master/docs/usage/files.md#chunked-upload

    return [uploaded_file, uploaded_metadata_file]


def backup_to_box(resource_record):
    # Check whether last_backed_up < resource_record['last_modified'] (the parameter that I think indicates when the resource was last modified)
    # Extract 1) resource file and 2) metadata as JSON file
    # Store in timestamped folder
        # data.wprdc.org-backups/<package_id>/<resource_id>/<timestamp>/
    resource_id = resource_record['resource_id']
    package_id = resource_record['package_id']
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    path_parts = ['data.wprdc.org-backups', package_id, resource_id]
    #root_folder = client.root_folder().get() # The root folder.
    root_folder = client.folder('0').get()
    resource_folder = find_folder_from_path(root_folder,path_parts)
    destination_folder = create_box_folder(client,resource_folder,timestamp)
    print(destination_folder.name)
    uploaded_box_files = upload_resource_to_box(destination_folder,resource_record)
    # Update last_backed_up field

    return resource_record

if __name__ == '__main__':
    # As a test, back up the Library Locations resource.
    resource_record = {'resource_id': '14babf3f-4932-4828-8b49-3c9a03bae6d0',
            'package_id': '5d52127f-3606-4ed9-a019-1daf4381902a'}
    print(backup_to_box(resource_record))
