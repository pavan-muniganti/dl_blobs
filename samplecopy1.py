from typing import Container
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from azure.storage.blob import ResourceTypes, AccountSasPermissions
from azure.storage.blob import generate_account_sas    
from datetime import *

today = str(datetime.now().date())
print(today)


#================================ SOURCE ===============================
# Source Client
connection_string = 'BlobEndpoint=https://dlsdatalinkdatanp.blob.core.windows.net/;QueueEndpoint=https://dlsdatalinkdatanp.queue.core.windows.net/;FileEndpoint=https://dlsdatalinkdatanp.file.core.windows.net/;TableEndpoint=https://dlsdatalinkdatanp.table.core.windows.net/;SharedAccessSignature=sv=2022-11-02&ss=bfqt&srt=sco&sp=rwlacupyx&se=2023-08-12T17:22:04Z&st=2023-08-11T09:22:04Z&spr=https&sig=sJoz4TOrqJALaoZdnihX%2BopS9%2F38QjHsJpVqSqEX5x4%3D' # The connection string for the source container
account_key = 'ZLCggSzENywbp82s8g0tKhDsveC7qb3rcPWNfVeiCe7OyUDqTOQVYQhDBVA0vxHvmc8biESqHimx+ASt9oQkMw==' # The account key for the source container
# source_container_name = 'newblob' # Name of container which has blob to be copied

# Create client
client = BlobServiceClient.from_connection_string(connection_string) 




client = BlobServiceClient.from_connection_string(connection_string)
all_containers = client.list_containers(include_metadata=True)
for container in all_containers:
    # Create sas token for blob
    if(container['name'] != 'test-platform'):
        continue
    sas_token = generate_account_sas(
        account_name = client.account_name,
        account_key = account_key,
        resource_types = ResourceTypes(object=True, container=True),
        permission= AccountSasPermissions(read=True,list=True),
        # start = datetime.now(),
        expiry = datetime.utcnow() + timedelta(hours=12) # Token valid for 4 hours
    )

    
    
    print(container['name'], container['metadata'])
    print("==========================")
    # print("==========================")
    container_client = client.get_container_client(container.name)
    # print(container_client)
    blobs_list = container_client.list_blobs()
    for blob in blobs_list:
        # Create blob client for source blob
        source_blob = BlobClient(
        client.url,
        container_name = container['name'],
        blob_name = blob.name,
        credential = sas_token
    )
        #if "Content" in blob.name and "CCDA" in blob.name and blob.name.lower().endswith('.xml'):
        #    print(blob.name)
        
            # Copy the XML blob to the destination folder
        destination_container_name = "test-platform1-pm"
        destination_folder = container['name']+"/"

        #destination_blob_name = blob.name
        destination_blob_name = destination_folder + blob.name.split('/')[-1]
        destination_blob_client = client.get_blob_client(
            container=destination_container_name,
            blob=destination_blob_name
        )
        destination_blob_client.start_copy_from_url(source_blob.url)

        # Check the copy status
        copy_status = destination_blob_client.get_blob_properties().copy.status
        while copy_status == "pending":
            copy_status = destination_blob_client.get_blob_properties().copy.status
            time.sleep(1)

        if copy_status == "success":
            print(f"Copied: {blob.name} -> {destination_blob_name}")
        else:
            print(f"Failed to copy: {blob.name} -> {destination_blob_name}")