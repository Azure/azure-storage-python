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
import unittest

from azure.storage import CloudStorageAccount
from .queue import (
    QueueSasSamples,
    QueueSamples,
)
from .table import (
    TableSasSamples,
    TableSamples,
)
from .file import (
    FileSasSamples,
    ShareSamples,
    DirectorySamples,
    FileSamples,
)

ACCOUNT_NAME = ''
ACCOUNT_KEY = ''
account = CloudStorageAccount(ACCOUNT_NAME, ACCOUNT_KEY)

class SampleTest(unittest.TestCase):

    def test_queue_samples(self):
        queue = QueueSamples(account)
        queue.run_all_samples()

    def test_table_samples(self):
        table = TableSamples(account)
        table.run_all_samples()

    def test_share_samples(self):
        share = ShareSamples(account)
        share.run_all_samples()

    def test_directory_samples(self):
        directory = DirectorySamples(account)
        directory.run_all_samples()

    def test_file_samples(self):
        file = FileSamples(account)
        file.run_all_samples()

    def test_queue_sas_samples(self):
        sas = QueueSasSamples(account)
        sas.run_all_samples()

    def test_table_sas_samples(self):
        sas = TableSasSamples(account)
        sas.run_all_samples()

    def test_file_sas_samples(self):
        sas = FileSasSamples(account)
        sas.run_all_samples()