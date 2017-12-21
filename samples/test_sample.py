# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
import unittest

from azure.storage.common import CloudStorageAccount
from samples.advanced import (
    AuthenticationSamples,
    ClientSamples,
)
from samples.blob import (
    BlobSasSamples,
    ContainerSamples,
    BlockBlobSamples,
    AppendBlobSamples,
    PageBlobSamples,
    BlobEncryptionSamples,
)
from samples.file import (
    FileSasSamples,
    ShareSamples,
    DirectorySamples,
    FileSamples,
)
from samples.queue import (
    QueueSasSamples,
    QueueSamples,
    QueueEncryptionSamples,
)


@unittest.skip('Skip sample tests.')
class SampleTest(unittest.TestCase):
    def setUp(self):
        super(SampleTest, self).setUp()

        try:
            import samples.config as config
        except:
            raise ValueError('Please specify configuration settings in config.py.')

        if config.IS_EMULATED:
            self.account = CloudStorageAccount(is_emulated=True)
        else:
            # Note that account key and sas should not both be included
            account_name = config.STORAGE_ACCOUNT_NAME
            account_key = config.STORAGE_ACCOUNT_KEY
            sas = config.SAS
            self.account = CloudStorageAccount(account_name=account_name,
                                               account_key=account_key,
                                               sas_token=sas)

    def test_container_samples(self):
        container = ContainerSamples(self.account)
        container.run_all_samples()

    def test_block_blob_samples(self):
        blob = BlockBlobSamples(self.account)
        blob.run_all_samples()

    def test_append_blob_samples(self):
        blob = AppendBlobSamples(self.account)
        blob.run_all_samples()

    def test_page_blob_samples(self):
        blob = PageBlobSamples(self.account)
        blob.run_all_samples()

    def test_queue_samples(self):
        queue = QueueSamples(self.account)
        queue.run_all_samples()

    def test_share_samples(self):
        share = ShareSamples(self.account)
        share.run_all_samples()

    def test_directory_samples(self):
        directory = DirectorySamples(self.account)
        directory.run_all_samples()

    def test_file_samples(self):
        file = FileSamples(self.account)
        file.run_all_samples()

    def test_blob_sas_samples(self):
        sas = BlobSasSamples(self.account)
        sas.run_all_samples()

    def test_queue_sas_samples(self):
        sas = QueueSasSamples(self.account)
        sas.run_all_samples()

    def test_file_sas_samples(self):
        sas = FileSasSamples(self.account)
        sas.run_all_samples()

    def test_authentication_samples(self):
        auth = AuthenticationSamples()
        auth.run_all_samples()

    def test_client_samples(self):
        client = ClientSamples()
        client.run_all_samples()

    def test_queue_encryption_samples(self):
        encryption = QueueEncryptionSamples(self.account)
        encryption.run_all_samples()

    def test_blob_encryption_samples(self):
        encryption = BlobEncryptionSamples(self.account)
        encryption.run_all_samples()


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
