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
import time
import uuid

from azure.common import (
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
)

from azure.storage.common import (
    Metrics,
    CorsRule,
)


class ShareSamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_file_service()

        self.create_share()
        self.delete_share()
        self.share_metadata()
        self.share_properties()
        self.share_stats()
        self.share_exists()

        self.list_shares()

        # This method contains sleeps, so don't run by default
        # self.service_properties()

    def _get_share_reference(self, prefix='share'):
        return '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))

    def _create_share(self, prefix='share'):
        share_name = self._get_share_reference(prefix)
        self.service.create_share(share_name)
        return share_name

    def create_share(self):
        # Basic
        share_name1 = self._get_share_reference()
        created = self.service.create_share(share_name1)  # True

        # Quota
        share_name2 = self._get_share_reference()
        created = self.service.create_share(share_name2, quota=1)  # True

        # Metadata
        metadata = {'val1': 'foo', 'val2': 'blah'}
        share_name3 = self._get_share_reference()
        created = self.service.create_share(share_name3, metadata=metadata)  # True

        # Fail on exist
        share_name4 = self._get_share_reference()
        created = self.service.create_share(share_name4)  # True
        created = self.service.create_share(share_name4)  # False
        try:
            self.service.create_share(share_name4, fail_on_exist=True)
        except AzureConflictHttpError:
            pass

        self.service.delete_share(share_name1)
        self.service.delete_share(share_name2)
        self.service.delete_share(share_name3)
        self.service.delete_share(share_name4)

    def delete_share(self):
        # Basic
        share_name = self._create_share()
        deleted = self.service.delete_share(share_name)  # True

        # Fail not exist
        share_name = self._get_share_reference()
        deleted = self.service.delete_share(share_name)  # False
        try:
            self.service.delete_share(share_name, fail_not_exist=True)
        except AzureMissingResourceHttpError:
            pass

    def share_metadata(self):
        share_name = self._create_share()
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Basic
        self.service.set_share_metadata(share_name, metadata=metadata)
        metadata = self.service.get_share_metadata(share_name)  # metadata={'val1': 'foo', 'val2': 'blah'}

        # Replaces values, does not merge
        metadata = {'new': 'val'}
        self.service.set_share_metadata(share_name, metadata=metadata)
        metadata = self.service.get_share_metadata(share_name)  # metadata={'new': 'val'}

        # Capital letters
        metadata = {'NEW': 'VAL'}
        self.service.set_share_metadata(share_name, metadata=metadata)
        metadata = self.service.get_share_metadata(share_name)  # metadata={'new': 'VAL'}

        # Clearing
        self.service.set_share_metadata(share_name)
        metadata = self.service.get_share_metadata(share_name)  # metadata={}

        self.service.delete_share(share_name)

    def share_properties(self):
        share_name = self._create_share()
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Basic
        # Sets the share quota to 1 GB
        self.service.set_share_properties(share_name, 1)
        share = self.service.get_share_properties(share_name)
        quota = share.properties.quota  # 1

        # Metadata
        self.service.set_share_metadata(share_name, metadata=metadata)
        share = self.service.get_share_properties(share_name)
        metadata = share.metadata  # metadata={'val1': 'foo', 'val2': 'blah'}

        self.service.delete_share(share_name)

    def share_stats(self):
        share_name = self._create_share()
        self.service.create_file_from_text(share_name, None, 'file1', b'hello world')

        # Basic
        share_usage = self.service.get_share_stats(share_name)  # 1

        self.service.delete_share(share_name)

    def share_exists(self):
        share_name = self._get_share_reference()

        # Basic
        exists = self.service.exists(share_name)  # False
        self.service.create_share(share_name)
        exists = self.service.exists(share_name)  # True

        self.service.delete_share(share_name)

    def list_shares(self):
        share_name1 = self._get_share_reference()
        self.service.create_share('share1', metadata={'val1': 'foo', 'val2': 'blah'})

        share_name2 = self._create_share('share2')
        share_name3 = self._create_share('thirdshare')

        # Basic
        # Commented out as this will list every share in your account
        # shares = list(self.service.list_shares())
        # for share in shares:
        #    print(share.name) # share1, share2, thirdq, all other shares created in the service        

        # Num results
        # Will return in alphabetical order. 
        shares = list(self.service.list_shares(num_results=2))
        for share in shares:
            print(share.name)  # share1, share2

        # Prefix
        shares = list(self.service.list_shares(prefix='share'))
        for share in shares:
            print(share.name)  # share1, share2

        # Metadata
        shares = list(self.service.list_shares(prefix='share', include_metadata=True))
        share = next((q for q in shares if q.name == 'share1'), None)
        metadata = share.metadata  # {'val1': 'foo', 'val2': 'blah'}

        self.service.delete_share(share_name1)
        self.service.delete_share(share_name2)
        self.service.delete_share(share_name3)

    def service_properties(self):
        # Basic
        self.service.set_file_service_properties(hour_metrics=Metrics(enabled=True, include_apis=True),
                                                 minute_metrics=Metrics(enabled=True, include_apis=False),
                                                 cors=[CorsRule(allowed_origins=['*'], allowed_methods=['GET'])])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = self.service.get_file_service_properties()  # props = ServiceProperties() w/ all properties specified above

        # Omitted properties will not overwrite what's already on the self.service
        # Empty properties will clear
        self.service.set_file_service_properties(cors=[])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = self.service.get_file_service_properties()  # props = ServiceProperties() w/ CORS rules cleared
