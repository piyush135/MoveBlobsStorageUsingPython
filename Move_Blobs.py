from azure.storage.blob import BlobServiceClient

from azure.storage.blob import BlobServiceClient

def move_blobs_with_prefix(connection_string, container_name, source_prefix, destination_prefix):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Get a list of all blobs in the source prefix (folder)
    source_blobs = container_client.list_blobs(name_starts_with=source_prefix)

    # Move each blob to the destination prefix (folder)
    for source_blob in source_blobs:
        # Get the source blob properties
        source_blob_client = container_client.get_blob_client(source_blob.name)
        source_blob_properties = source_blob_client.get_blob_properties()

        # Copy the blob to the destination with the new name
        destination_blob_name = source_blob.name.replace(source_prefix, destination_prefix, 1)
        destination_blob_client = container_client.get_blob_client(destination_blob_name)
        destination_blob_client.start_copy_from_url(source_blob_client.url)

        # Delete the source blob
        source_blob_client.delete_blob()

# Usage
connection_string = ""
container_name = ""
source_prefix = ""
destination_prefix = ""

move_blobs_with_prefix(connection_string, container_name, source_prefix, destination_prefix)




