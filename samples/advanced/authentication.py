# coding: utf-8

from azure.storage.blob import BlockBlobService
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
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
