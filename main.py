# Load libraries
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import os
import zipfile
import pyarrow
import json
import pysftp
import time

sftp_output_dir = "sftp"

def download_from_sftp(sftp_host, sftp_username, sftp_password, sftp_root_dir, skip_host_key_validation):
    # Disable SSH host key Validation
    cnopts = pysftp.CnOpts()
    if skip_host_key_validation:
        print("disabled hostkey")
        cnopts.hostkeys = None
    
    print(sftp_host)
    print(sftp_username)
    print(sftp_password)

    with pysftp.Connection(sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
        os.makedirs(os.path.join(sftp_output_dir, sftp_root_dir[:sftp_root_dir.rfind('/')]), exist_ok=True)
        sftp.get_r(sftp_root_dir, sftp_output_dir)

def recreate_container(container_client):
    try:
        container_client.delete_container()
    except Exception as ex:
        print('Failed to remove container:')
        print(ex)

    retry = 0
    created = False    
    while retry < 20 and not(created):   
        try:
            container_client.create_container()
            created = True
        except Exception as ex:
            retry += 1
            print('Failed to remove container:')
            print(ex)
            time.sleep(5)    

def un_zipFiles(path, container_client):
    files = os.listdir(path)
    for file in files:
        if file.endswith('.zip'):
            output_dir_name = file[:-4]
            filePath = os.path.join(path, file)
            zip_file = zipfile.ZipFile(filePath)
            for name in zip_file.namelist():
                info = zip_file.getinfo(name)
                if not(info.is_dir()) and name.endswith('.csv'):
                  df = pd.read_csv(zip_file.open(name))
                  # '/' is used and not os.path.sep, because zipfile uses '/'
                  output_file_path = '/'.join([output_dir_name,
                                               name[:-4]+'.parquet'])
                  os.makedirs(
                      output_file_path[:output_file_path.rfind('/')], exist_ok=True)
                  df.to_parquet(output_file_path)
                  # writing to blob
                  blob_client = container_client.get_blob_client(blob=output_file_path)
                  with open(output_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite = True)
            zip_file.close()

def entry():
    container_name = "output"

    activity = json.loads(open('activity.json').read())
    ext_p = activity["typeProperties"]["extendedProperties"]
    sftp_password = ext_p["sftpPassword"]
    blob_connect_str = ext_p["blobConnectionString"]
    sftp_root_dir = ext_p["sftpRootDir"]

    linkedServices = json.loads(open('linkedServices.json').read())
    type_p = linkedServices[0]["properties"]["typeProperties"]
    sftp_host = type_p["host"]
    skip_host_key_validation = type_p["skipHostKeyValidation"]
    sftp_username =  type_p["userName"]
    print(skip_host_key_validation)

    blob_service_client = BlobServiceClient.from_connection_string(blob_connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    download_from_sftp(sftp_host, sftp_username, sftp_password, sftp_root_dir, skip_host_key_validation)
    recreate_container(container_client)
    un_zipFiles(os.path.join(sftp_output_dir, sftp_root_dir), container_client)

entry()
