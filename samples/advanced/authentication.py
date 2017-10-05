# coding: utf-8

from azure.storage.blob import BlockBlobService
# -------------------------------------------------------------------------
# Copyright (c) Microsoft.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this blob except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
from azure.storage.common import CloudStorageAccount


class AuthenticationSamples():
    def __init__(self):
        pass

    def run_all_samples(self):
        self.key_auth()
        self.sas_auth()
        self.emulator()
        self.public()
        self.connection_string()

    def key_auth(self):
        # With account
        account = CloudStorageAccount(account_name='<account_name>', account_key='<account_key>')
        client = account.create_block_blob_service()

        # Directly
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>')

    def sas_auth(self):
        # With account
        account = CloudStorageAccount(account_name='<account_name>', sas_token='<sas_token>')
        client = account.create_block_blob_service()

        # Directly
        client = BlockBlobService(account_name='<account_name>', sas_token='<sas_token>')

    def emulator(self):
        # With account
        account = CloudStorageAccount(is_emulated=True)
        client = account.create_block_blob_service()

        # Directly
        client = BlockBlobService(is_emulated=True)

        # The emulator does not at the time of writing support append blobs or 
        # the file service.

    def public(self):
        # This applies to the blob services only
        # Public access must be enabled on the container or requests will fail

        # With account
        account = CloudStorageAccount(account_name='<account_name>')
        client = account.create_block_blob_service()

        # Directly
        client = BlockBlobService(account_name='<account_name>')

    def connection_string(self):
        # Connection strings may be retrieved from the Portal or constructed manually
        connection_string = 'AccountName=<account_name>;AccountKey=<account_key>;'
        client = BlockBlobService(connection_string=connection_string)
