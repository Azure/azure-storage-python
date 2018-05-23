# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from tests.settings_real import STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY
from azure.storage.blob import BlockBlobService
from azure.common import AzureException
import concurrent.futures


def purge_blob_containers(account, account_key):
    """
        Delete all blob containers in the given storage account.
        USE AT OWN RISK.
    """
    bs = BlockBlobService(account, account_key)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # use a map to keep track of futures
        future_to_container_map = {executor.submit(delete_container, bs, container): container for container in bs.list_containers()}

        # as the futures are completed, print results
        for future in concurrent.futures.as_completed(future_to_container_map):
            container_name = future_to_container_map[future].name

            try:
                is_deleted = future.result()
                if is_deleted:
                    print("Deleted container {} on first try".format(container_name))
                else:
                    print("Skipped container {} as it no longer exists".format(container_name))
            except AzureException as e:
                # if the deletion failed because there's an active lease on the container, we will break it
                # since it is most likely left-over from previous tests
                if 'lease' in str(e):
                    bs.break_container_lease(container_name)
                    is_deleted = bs.delete_container(container_name)

                    if is_deleted:
                        print("Deleted container {} after having broken lease".format(container_name))
                    else:
                        print("Skipped container {} as it stopped existing after having broken lease".format(container_name))
                else:
                    raise e
            except Exception as e:
                print("Skipped container " + container_name + " due to error " + str(e))


def delete_container(bs, container):
    return bs.delete_container(container.name)


if __name__ == '__main__':
    purge_blob_containers(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
