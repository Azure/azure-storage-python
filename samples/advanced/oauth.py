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
import datetime
import threading
import adal


class AutoUpdatedTokenCredential(TokenCredential):
    """
    This class can be used as a TokenCredential, it periodically updates its token through a timer triggered operation.
    It shows one way of making sure the credential does not become expired.
    """
    def __init__(self):
        super(AutoUpdatedTokenCredential, self).__init__()

        # a timer is used to trigger a callback to update the token
        # the timer needs to be protected, as later on it is possible that one thread is setting a new timer and
        # another thread is trying to cancel the timer
        self.lock = threading.Lock()
        self.timer_stopped = False

        # get the initial token and schedule the timer for the very first time
        self.refresh_token()

    # support context manager
    def __enter__(self):
        return self

    # support context manager
    def __exit__(self, *args):
        self.stop_refreshing_token()

    def refresh_token(self):
        # call the get token function to get a new token, as well as the time to wait before calling it again
        token, next_interval = self.get_token_func()

        # the token is set instantaneously, and can be used by BlockBlobService right away
        self.token = token

        with self.lock:
            if self.timer_stopped is False:
                self.timer = threading.Timer(next_interval, self.refresh_token)
                self.timer.start()

    def stop_refreshing_token(self):
        """
        The timer needs to be canceled if the application is terminating, if not the timer will keep going.
        """
        with self.lock:
            self.timer_stopped = True
            self.timer.cancel()

    @staticmethod
    def get_token_func():
        """
        This function makes a call to AAD to fetch an OAuth token
        :return: the OAuth token and the interval to wait before refreshing it
        """
        print("{}: token updater was triggered".format(datetime.datetime.now()))

        # in this example, the OAuth token is obtained using the ADAL library
        # however, the user can use any preferred method
        context = adal.AuthenticationContext(
            str.format("https://login.microsoftonline.com/{}", settings.ACTIVE_DIRECTORY_TENANT_ID),
            api_version=None, validate_authority=True)

        oauth_token = context.acquire_token_with_client_credentials(
            "https://storage.azure.com",
            settings.ACTIVE_DIRECTORY_APPLICATION_ID,
            settings.ACTIVE_DIRECTORY_APPLICATION_SECRET)

        # return the token itself and the interval to wait before this function should be called again
        # generally oauth_token['expiresIn'] - 180 is a good interval to give, as it tells the caller to
        # refresh the token 3 minutes before it expires, so here we are assuming that the token expiration
        # is at least longer than 3 minutes, the user should adjust it according to their AAD policy
        return oauth_token['accessToken'], oauth_token['expiresIn'] - 180


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
                print("{}: something is wrong".format(datetime.datetime.now()))
            else:
                print("{}: all is well".format(datetime.datetime.now()))

            time.sleep(600)


if __name__ == '__main__':
    test_token_credential_with_timer()