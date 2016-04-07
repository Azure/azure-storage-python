﻿#-------------------------------------------------------------------------
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
from __future__ import division
from contextlib import contextmanager
import copy
import inspect
import json
import os
import os.path
import time
import vcr
import zlib
import math
import uuid
import unittest
import sys
import random
import tests.settings_real as settings
import tests.settings_fake as fake_settings

should_log = os.getenv('SDK_TESTS_LOG', '0')
if should_log.lower() == 'true' or should_log == '1':
    import logging
    logger = logging.getLogger('azure.common.filters')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())


class TestMode(object):
    none = 'None'.lower() # this will be for unit test, no need for any recordings
    playback = 'Playback'.lower() # run against stored recordings
    record = 'Record'.lower() # run tests against live storage and update recordings
    run_live_no_record = 'RunLiveNoRecord'.lower() # run tests against live storage without altering recordings

    @staticmethod
    def is_playback(mode):
        return mode == TestMode.playback

    @staticmethod
    def need_recording_file(mode):
        return mode == TestMode.playback or mode == TestMode.record

    @staticmethod
    def need_real_credentials(mode):
        return mode == TestMode.run_live_no_record or mode == TestMode.record


class StorageTestCase(unittest.TestCase):

    def setUp(self):
        self.working_folder = os.path.dirname(__file__)    

        self.settings = settings        
        self.fake_settings = fake_settings

        self.test_mode = self.settings.TEST_MODE.lower() or TestMode.playback
        
        if self.test_mode == TestMode.playback:
            self.settings = self.fake_settings

        # example of qualified test name:
        # test_mgmt_network.test_public_ip_addresses
        _, filename = os.path.split(inspect.getsourcefile(type(self)))
        name, _ = os.path.splitext(filename)
        self.qualified_test_name = '{0}.{1}'.format(
            name,
            self._testMethodName,
        )

    def sleep(self, seconds):
        if not self.is_playback():
            time.sleep(seconds)

    def is_playback(self):
        return self.test_mode == TestMode.playback

    def get_resource_name(self, prefix):
        # Append a suffix to the name, based on the fully qualified test name
        # We use a checksum of the test name so that each test gets different
        # resource names, but each test will get the same name on repeat runs,
        # which is needed for playback.
        # Most resource names have a length limit, so we use a crc32
        if self.test_mode.lower() == TestMode.run_live_no_record.lower():
            return prefix + str(uuid.uuid4()).replace('-', '')
        else:
            checksum = zlib.adler32(self.qualified_test_name.encode()) & 0xffffffff
            name = '{}{}'.format(prefix, hex(checksum)[2:])
            if name.endswith('L'):
                name = name[:-1]
            return name

    def get_random_bytes(self, size):
        if self.test_mode.lower() == TestMode.run_live_no_record.lower():
            rand = random.Random()
        else:
            checksum = zlib.adler32(self.qualified_test_name.encode()) & 0xffffffff
            rand = random.Random(checksum)
        result = bytearray(size)
        for i in range(size):
            result[i] = rand.randint(0, 255)
        return bytes(result)

    def get_random_text_data(self, size):
        '''Returns random unicode text data exceeding the size threshold for
        chunking blob upload.'''
        checksum = zlib.adler32(self.qualified_test_name.encode()) & 0xffffffff
        rand = random.Random(checksum)
        text = u''
        words = [u'hello', u'world', u'python', u'啊齄丂狛狜']
        while (len(text) < size):
            index = rand.randint(0, len(words) - 1)
            text = text + u' ' + words[index]

        return text

    @staticmethod
    def _set_service_options(service, settings):
        if settings.USE_PROXY:
            service.set_proxy(
                settings.PROXY_HOST,
                settings.PROXY_PORT,
                settings.PROXY_USER,
                settings.PROXY_PASSWORD,
            )

    def _create_storage_service(self, service_class, settings, account_name=None, account_key=None):
        if settings.IS_EMULATED:
            service = service_class(
                is_emulated=True
            )
        else:
            account_name = account_name or settings.STORAGE_ACCOUNT_NAME
            account_key = account_key or settings.STORAGE_ACCOUNT_KEY
            protocol = settings.PROTOCOL or 'https'
            service = service_class(
                account_name,
                account_key,
                protocol=protocol,
            )
        self._set_service_options(service, settings)
        return service

    def assertNamedItemInContainer(self, container, item_name, msg=None):
        def _is_string(obj):
            if sys.version_info >= (3,):
                return isinstance(obj, str)
            else:
                return isinstance(obj, basestring)
        for item in container:
            if _is_string(item):
                if item == item_name:
                    return
            elif item.name == item_name:
                return

        standardMsg = '{0} not found in {1}'.format(
            repr(item_name), repr(container))
        self.fail(self._formatMessage(msg, standardMsg))

    def assertNamedItemNotInContainer(self, container, item_name, msg=None):
        for item in container:
            if item.name == item_name:
                standardMsg = '{0} unexpectedly found in {1}'.format(
                    repr(item_name), repr(container))
                self.fail(self._formatMessage(msg, standardMsg))

    if sys.version_info < (2,7):
        def assertIsNone(self, obj):
            self.assertEqual(obj, None)

        def assertIsNotNone(self, obj):
            self.assertNotEqual(obj, None)

        def assertIsInstance(self, obj, type):
            self.assertTrue(isinstance(obj, type))

        def assertGreater(self, a, b):
            self.assertTrue(a > b)

        def assertGreaterEqual(self, a, b):
            self.assertTrue(a >= b)

        def assertLess(self, a, b):
            self.assertTrue(a < b)

        def assertLessEqual(self, a, b):
            self.assertTrue(a <= b)

        def assertIn(self, member, container):
            if member not in container:
                self.fail('{0} not found in {1}.'.format(
                    safe_repr(member), safe_repr(container)))

        def assertRaises(self, excClass, callableObj=None, *args, **kwargs):
            @contextmanager
            def _assertRaisesContextManager(self, excClass):
                try:
                    yield
                    self.fail('{0} was not raised'.format(safe_repr(excClass)))
                except excClass:
                    pass
            if callableObj:
                super(ExtendedTestCase, self).assertRaises(
                    excClass,
                    callableObj,
                    *args,
                    **kwargs
                )
            else:
                return self._assertRaisesContextManager(excClass)

    def recording(self):
        if TestMode.need_recording_file(self.test_mode):
            cassette_name = '{0}.yaml'.format(self.qualified_test_name)

            my_vcr = vcr.VCR(
                before_record_request = self._scrub_sensitive_request_info,
                before_record_response = self._scrub_sensitive_response_info,
                record_mode = 'none' if TestMode.is_playback(self.test_mode) else 'all' 
            )

            self.assertIsNotNone(self.working_folder)
            return my_vcr.use_cassette(
                os.path.join(self.working_folder, 'recordings', cassette_name),
                filter_headers=['authorization'],
            )
        else:
            @contextmanager
            def _nop_context_manager():
                yield
            return _nop_context_manager()

    def _scrub_sensitive_request_info(self, request):
        if not TestMode.is_playback(self.test_mode):
            request.uri = self._scrub(request.uri)
            if request.body is not None:
                request.body = self._scrub(request.body)
        return request

    def _scrub_sensitive_response_info(self, response):
        if not TestMode.is_playback(self.test_mode):
            # We need to make a copy because vcr doesn't make one for us.
            # Without this, changing the contents of the dicts would change
            # the contents returned to the caller - not just the contents
            # getting saved to disk. That would be a problem with headers
            # such as 'location', often used in the request uri of a
            # subsequent service call.
            response = copy.deepcopy(response)
            headers = response.get('headers')
            if headers:
                for name, val in headers.items():
                    for i in range(len(val)):
                        val[i] = self._scrub(val[i])
            body = response.get('body')
            if body:
                body_str = body.get('string')
                if body_str:
                    response['body']['string'] = self._scrub(body_str)

        return response

    def _scrub(self, val):
        old_to_new_dict = {
            self.settings.STORAGE_ACCOUNT_NAME: self.fake_settings.STORAGE_ACCOUNT_NAME,
            self.settings.STORAGE_ACCOUNT_KEY: self.fake_settings.STORAGE_ACCOUNT_KEY,
            self.settings.REMOTE_STORAGE_ACCOUNT_KEY: self.fake_settings.REMOTE_STORAGE_ACCOUNT_KEY,
            self.settings.REMOTE_STORAGE_ACCOUNT_NAME: self.fake_settings.REMOTE_STORAGE_ACCOUNT_NAME,
        }
        replacements = list(old_to_new_dict.keys())

        # if we have 'val1' and 'val10', we want 'val10' to be replaced first
        replacements.sort(reverse=True)

        for old_value in replacements:
            if old_value:
                new_value = old_to_new_dict[old_value]
                if old_value != new_value:
                    if isinstance(val, bytes):
                        val = val.replace(old_value.encode(), new_value.encode())
                    else:
                        val = val.replace(old_value, new_value)
        return val

    def assert_upload_progress(self, size, max_chunk_size, progress, unknown_size=False):
        '''Validates that the progress chunks align with our chunking procedure.'''
        index = 0
        total = None if unknown_size else size
        small_chunk_size = size % max_chunk_size
        self.assertEqual(len(progress), 1 + math.ceil(size / max_chunk_size))
        for i in progress:
            self.assertTrue(i[0] % max_chunk_size == 0 or i[0] % max_chunk_size == small_chunk_size)
            self.assertEqual(i[1], total)

    def assert_download_progress(self, size, max_chunk_size, progress, single_download=True):
        '''Validates that the progress chunks align with our chunking procedure.'''
        if single_download:
            self.assertEqual(len(progress), 2)
            self.assertEqual((0, None), progress[0])
            self.assertEqual((size, size), progress[1])
        else:
            small_chunk_size = size % max_chunk_size
            self.assertEqual(len(progress), 1 + math.ceil(size / max_chunk_size))
            for i in progress:
                self.assertTrue(i[0] % max_chunk_size == 0 or i[0] % max_chunk_size == small_chunk_size)
                self.assertEqual(i[1], size)

def record(test):
    def recording_test(self):
        with self.recording():
            test(self)
    recording_test.__name__ = test.__name__
    return recording_test
