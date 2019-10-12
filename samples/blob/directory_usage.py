import uuid

from tests.settings_real import (
    STORAGE_ACCOUNT_NAME,
    STORAGE_ACCOUNT_KEY,
    HIERARCHICAL_NAMESPACE_ACCOUNT_NAME,
    HIERARCHICAL_NAMESPACE_ACCOUNT_KEY,
    ACTIVE_DIRECTORY_APPLICATION_ID
)
from azure.storage.blob import (
    BlockBlobService
)

# toggle this constant to see different behaviors
# when HNS is enabled:
#   - get/set access control operations are supported
#   - rename/delete operations are atomic
IS_HNS_ENABLED = False


def run():
    # swap in your test accounts
    if IS_HNS_ENABLED:
        blob_service = BlockBlobService(HIERARCHICAL_NAMESPACE_ACCOUNT_NAME, HIERARCHICAL_NAMESPACE_ACCOUNT_KEY)
    else:
        blob_service = BlockBlobService(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, protocol="http")

    # set up a test container
    container_name = "testcontainer"
    blob_service.create_container(container_name)

    try:
        demonstrate_directory_usage(blob_service, container_name)
    finally:
        # clean up the test container
        blob_service.delete_container(container_name)


def demonstrate_directory_usage(blob_service, container_name):
    directory_name = "dir"

    # usage 1: create a directory with metadata inside the container
    props = blob_service.create_directory(container_name, directory_path=directory_name, metadata={"test": "value"})

    # show the etag and lmt of the newly created directory
    print("Etag: {}".format(props.etag))
    print("Lmt: {}".format(props.last_modified))

    # populate the created directory with some blobs
    _create_blobs(blob_service, container_name, directory_name, num_of_blobs=200)

    # these APIs only work against an account where HNS is enabled
    if IS_HNS_ENABLED:
        # usage 2: set the access control properties on the directory
        test_owner = ACTIVE_DIRECTORY_APPLICATION_ID
        test_group = ACTIVE_DIRECTORY_APPLICATION_ID
        test_permissions = 'rwxrw-rw-'
        blob_service.set_path_access_control(container_name, directory_name, owner=test_owner, group=test_group,
                                             permissions=test_permissions)

        # usage 3: fetch the access control information on the directory
        access_control_props = blob_service.get_path_access_control(
            container_name, directory_name, user_principle_names=True)

        # print out values
        print("Owner: {}".format(access_control_props.owner))
        print("Permissions: {}".format(access_control_props.permissions))
        print("Group: {}".format(access_control_props.group))
        print("Acl: {}".format(access_control_props.acl))

    # usage 4: rename directory, see method for more details
    new_directory_name = "dir2"
    rename_directory(blob_service, container_name, new_directory_name, directory_name)

    # usage 5: delete the directory, see method for more details
    delete_directory(blob_service, container_name, new_directory_name)


def rename_directory(blob_service, container_name, new_directory_name, old_directory_name):
    marker = blob_service.rename_path(container_name, new_directory_name, old_directory_name)

    # if HNS is enabled, the rename operation is atomic and no marker is returned
    # if HNS is not enabled, and there are too more files/subdirectories in the directories to be renamed
    # in a single call, the service returns a marker, so that we can follow it and finish renaming
    # the rest of the files/subdirectories
    count = 1
    while marker is not None:
        marker = blob_service.rename_path(container_name, new_directory_name, old_directory_name, marker=marker)
        count += 1

    print("Took {} call(s) to finish renaming.".format(count))


def delete_directory(blob_service, container_name, directory_name):
    deleted, marker = blob_service.delete_directory(container_name, directory_name, recursive=True)

    # if HNS is enabled, the delete operation is atomic and no marker is returned
    # if HNS is not enabled, and there are too more files/subdirectories in the directories to be deleted
    # in a single call, the service returns a marker, so that we can follow it and finish deleting
    # the rest of the files/subdirectories
    count = 1
    while marker is not None:
        deleted, marker = blob_service.delete_directory(container_name, directory_name,
                                                        marker=marker, recursive=True)
        count += 1

    print("Took {} calls(s) to finish deleting.".format(count))


def _create_blobs(blob_service, container_name, directory_name, num_of_blobs):
    import concurrent.futures
    import itertools
    # Use a thread pool because it is too slow otherwise
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        def create_blob():
            # generate a random name
            blob_name = "{}/{}".format(directory_name, str(uuid.uuid4()).replace('-', ''))

            # create a blob under the directory
            # blob_service.create_blob_from_bytes(container_name, blob_name, b"test")
            blob_service.create_directory(container_name, blob_name)

        futures = {executor.submit(create_blob) for _ in itertools.repeat(None, num_of_blobs)}
        concurrent.futures.wait(futures)
        print("Created {} blobs under the directory: {}".format(num_of_blobs, directory_name))


if __name__ == '__main__':
    run()
