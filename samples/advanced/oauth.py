# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from azure.storage.common import (
    TokenCredential
)
from azure.storage.blob import (
    BlockBlobService,
)
import tests.settings_real as settings
import time
import threading
import adal


class AutoUpdatedTokenCredential(TokenCredential):
    """
    This class can be used as a TokenCredential, it periodically updates its token through a timer triggered operation.
    It shows one way of making sure the credential does not become expired.
    """
    def __init__(self):
        # get the first token
        first_token, first_interval = self.get_token_func()
        super(AutoUpdatedTokenCredential, self).__init__(first_token)

        # a timer is used to trigger a callback to update the token
        # the timer needs to be protected, as later on it is possible that one thread is setting a new timer and
        # another thread is trying to cancel the timer
        self.lock = threading.Lock()
        self.timer = threading.Timer(first_interval, self.timer_callback)
        self.timer.start()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop_refreshing_token()

    def timer_callback(self):
        print("TOKEN UPDATER WAS TRIGGERED")

        # call the user-provided function to get a new token, as well as when to call it again
        token, next_interval = self.get_token_func()

        # the token is set instantaneously, and can be used by BlockBlobService right away
        self.token = token

        with self.lock:
            self.timer = threading.Timer(next_interval, self.timer_callback)
            self.timer.start()

    def stop_refreshing_token(self):
        # the timer needs to be canceled if the application is terminating
        # if not the timer will keep going
        with self.lock:
            self.timer.cancel()

    @staticmethod
    def get_token_func():
        # in this example, the OAuth token is obtained using the ADAL library
        context = adal.AuthenticationContext(
            str.format("https://login.microsoftonline.com/{}", settings.ACTIVE_DIRECTORY_TENANT_ID),
            api_version=None, validate_authority=True)

        oauth_token = context.acquire_token_with_client_credentials(
            "https://storage.azure.com",
            settings.ACTIVE_DIRECTORY_APPLICATION_ID,
            settings.ACTIVE_DIRECTORY_APPLICATION_SECRET)

        # here we are assuming that the token expiration is at least longer than 3 minutes
        # oauth_token['expiresIn'] - 180
        return oauth_token['accessToken'], 60


def test_token_credential_with_timer():
    # AutoUpdatedTokenCredential behaves like a context manager
    # it cancels the timer when exiting this block so that the script could terminate
    with AutoUpdatedTokenCredential() as token_credential:
        # initialize a BlockBlobService with the token credential that was just created
        service = BlockBlobService(settings.STORAGE_ACCOUNT_NAME, token_credential=token_credential)

        # loop for 100 minutes, and periodically check whether a container exists
        # this verifies whether the token gets updated properly
        for i in range(10):
            result = service.exists("test")
            if result is None:
                print("something is wrong")

            # time.sleep(600)
            time.sleep(60)


if __name__ == '__main__':
    test_token_credential_with_timer()