import os
import zarr
import azure.storage.blob

def get_files_from_blob(folder,container='vessel-emissions-2020'):
    
    blob_service_client = azure.storage.blob.BlobServiceClient.from_connection_string(os.environ['HACKATHON_CONNECTION_STR'])
    container_client = blob_service_client.get_container_client(container)

    file_list = list(set([os.path.join(f'abfs://{container}/',b.name) for b in container_client.walk_blobs(folder, delimiter='/')  ]))
    file_list.sort()
    
    return file_list

def get_zarr_from_blob(folder,container='vessel-emissions-2020'):
    
    blob_service_client = azure.storage.blob.BlobServiceClient.from_connection_string(os.environ['HACKATHON_CONNECTION_STR'])
    container_client = blob_service_client.get_container_client(container)
    
    file_list = list(
        set(
            [
                b.name
                for b in container_client.walk_blobs(folder, delimiter="/")
            ]
        )
    )
    file_list.sort()

    store_list = []
    for file in file_list:
        store = zarr.ABSStore(prefix=file, client=container_client)
        store_list.append(store)
        
        
    return store_list

        
    
