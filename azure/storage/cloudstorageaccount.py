#-------------------------------------------------------------------------
# Copyright (c) Microsoft.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------------

# Note that we import BlobService/QueueService/TableService on demand
# because this module is imported by azure/storage/__init__
# ie. we don't want 'import azure.storage' to trigger an automatic import
# of blob/queue/table packages.

from .sharedaccesssignature import (
    SharedAccessSignature,
)
from .models import (
    ResourceTypes,
    Services,
    AccountPermissions,
)
from ._error import _validate_not_none

class CloudStorageAccount(object):

    """
    Provides a factory for creating the blob, queue, table, and file services
    with a common account name and account key or sas token.  Users can either 
    use the factory or can construct the appropriate service directly.
    """

    def __init__(self, account_name=None, account_key=None, sas_token=None):
        self.account_name = account_name
        self.account_key = account_key
        self.sas_token = sas_token

    def create_block_blob_service(self):
        from .blob.blockblobservice import BlockBlobService
        return BlockBlobService(self.account_name, self.account_key, 
                                sas_token=self.sas_token)

    def create_page_blob_service(self):
        from .blob.pageblobservice import PageBlobService
        return PageBlobService(self.account_name, self.account_key,
                               sas_token=self.sas_token)

    def create_append_blob_service(self):
        from .blob.appendblobservice import AppendBlobService
        return AppendBlobService(self.account_name, self.account_key,
                                 sas_token=self.sas_token)

    def create_table_service(self):
        from .table.tableservice import TableService
        return TableService(self.account_name, self.account_key,
                            sas_token=self.sas_token)

    def create_queue_service(self):
        from .queue.queueservice import QueueService
        return QueueService(self.account_name, self.account_key,
                            sas_token=self.sas_token)

    def create_file_service(self):
        from .file.fileservice import FileService
        return FileService(self.account_name, self.account_key,
                           sas_token=self.sas_token)

    def generate_shared_access_signature(self, services, resource_types, 
                                         permission, expiry, start=None, 
                                         ip=None, protocol=None):
        '''
        Generates a shared access signature for the account.
        Use the returned signature with the sas_token parameter of the service 
        or to create a new account object.

        :param Services services:
            Specifies the services accessible with the account SAS. You can 
            combine values to provide access to more than one service. 
        :param ResourceType resource_type:
            Specifies the resource types that are accessible with the account 
            SAS. You can combine values to provide access to more than one 
            resource type. 
        :param AccountPermissions permission:
            The permissions associated with the shared access signature. The 
            user is restricted to operations allowed by the permissions. 
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has been 
            specified in an associated stored access policy. You can combine 
            values to provide more than one permission.
        :param expiry:
            The time at which the shared access signature becomes invalid. 
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has 
            been specified in an associated stored access policy. Azure will always 
            convert values to UTC. If a date is passed in without timezone info, it 
            is assumed to be UTC.
        :type expiry: date or str
        :param start:
            The time at which the shared access signature becomes valid. If 
            omitted, start time for this call is assumed to be the time when the 
            storage service receives the request. Azure will always convert values 
            to UTC. If a date is passed in without timezone info, it is assumed to 
            be UTC.
        :type start: date or str
        :param str ip:
            Specifies an IP address or a range of IP addresses from which to accept requests.
            If the IP address from which the request originates does not match the IP address
            or address range specified on the SAS token, the request is not authenticated.
            For example, specifying sip=168.1.5.65 or sip=168.1.5.60-168.1.5.70 on the SAS
            restricts the request to those IP addresses.
        :param str protocol:
            Specifies the protocol permitted for a request made. Possible values are
            both HTTPS and HTTP (https,http) or HTTPS only (https). The default value
            is https,http. Note that HTTP only is not a permitted value.
        '''
        _validate_not_none('self.account_name', self.account_name)
        _validate_not_none('self.account_key', self.account_key)

        sas = SharedAccessSignature(self.account_name, self.account_key)
        return sas.generate_account(services, resource_types, permission, 
                                    expiry, start=start, ip=ip, protocol=protocol)
