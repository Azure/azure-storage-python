# -------------------------------------------------------------------------
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
# --------------------------------------------------------------------------
import unittest

from azure.storage.blob import BlockBlobService
from azure.storage.common.retry import (
    LinearRetry
)
from azure.storage.queue import QueueService
from azure.storage.table import TableService
from tests.testcase import (
    StorageTestCase,
    record,
)

SERVICE_UNAVAILABLE_RESP_BODY = '<?xml version="1.0" encoding="utf-8"?><StorageServiceStats><GeoReplication><Status' \
                                '>unavailable</Status><LastSyncTime></LastSyncTime></GeoReplication' \
                                '></StorageServiceStats> '


# --Test Class -----------------------------------------------------------------
class ServiceStatsTest(StorageTestCase):
    # --Helpers-----------------------------------------------------------------
    def _assert_stats_default(self, stats):
        self.assertIsNotNone(stats)
        self.assertIsNotNone(stats.geo_replication)

        self.assertEqual(stats.geo_replication.status, 'live')
        self.assertIsNotNone(stats.geo_replication.last_sync_time)

    def _assert_stats_unavailable(self, stats):
        self.assertIsNotNone(stats)
        self.assertIsNotNone(stats.geo_replication)

        self.assertEqual(stats.geo_replication.status, 'unavailable')
        self.assertIsNone(stats.geo_replication.last_sync_time)

    @staticmethod
    def override_response_body_with_unavailable_status(response):
        response.body = SERVICE_UNAVAILABLE_RESP_BODY

    # --Test cases per service ---------------------------------------

    @record
    def test_blob_service_stats(self):
        # Arrange
        bs = self._create_storage_service(BlockBlobService, self.settings)

        # Act
        stats = bs.get_blob_service_stats()

        # Assert
        self._assert_stats_default(stats)

    @record
    def test_blob_service_stats_when_unavailable(self):
        # Arrange
        bs = self._create_storage_service(BlockBlobService, self.settings)
        bs.response_callback = self.override_response_body_with_unavailable_status
        bs.retry = LinearRetry(backoff=1).retry

        # Act
        stats = bs.get_blob_service_stats()

        # Assert
        self._assert_stats_unavailable(stats)

    @record
    def test_queue_service_stats(self):
        # Arrange
        qs = self._create_storage_service(QueueService, self.settings)

        # Act
        stats = qs.get_queue_service_stats()

        # Assert
        self._assert_stats_default(stats)

    @record
    def test_queue_service_stats_when_unavailable(self):
        # Arrange
        qs = self._create_storage_service(QueueService, self.settings)
        qs.response_callback = self.override_response_body_with_unavailable_status
        qs.retry = LinearRetry(backoff=1).retry

        # Act
        stats = qs.get_queue_service_stats()

        # Assert
        self._assert_stats_unavailable(stats)

    @record
    def test_table_service_stats(self):
        # Arrange
        ts = self._create_storage_service(TableService, self.settings)

        # Act
        stats = ts.get_table_service_stats()

        # Assert
        self._assert_stats_default(stats)

    @record
    def test_table_service_stats_when_unavailable(self):
        # Arrange
        ts = self._create_storage_service(TableService, self.settings)
        ts.response_callback = self.override_response_body_with_unavailable_status
        ts.retry = LinearRetry(backoff=1).retry

        # Act
        stats = ts.get_table_service_stats()

        # Assert
        self._assert_stats_unavailable(stats)


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
