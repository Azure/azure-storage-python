# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

# NOTE: these keys are fake, but valid base-64 data, they were generated using:
# base64.b64encode(os.urandom(64))

STORAGE_ACCOUNT_NAME = "storagename"
STORAGE_ACCOUNT_KEY = "NzhL3hKZbJBuJ2484dPTR+xF30kYaWSSCbs2BzLgVVI1woqeST/1IgqaLm6QAOTxtGvxctSNbIR/1hW8yH+bJg=="
BLOB_STORAGE_ACCOUNT_NAME = "blobstoragename"
BLOB_STORAGE_ACCOUNT_KEY = "NzhL3hKZbJBuJ2484dPTR+xF30kYaWSSCbs2BzLgVVI1woqeST/1IgqaLm6QAOTxtGvxctSNbIR/1hW8yH+bJg=="
REMOTE_STORAGE_ACCOUNT_NAME = "remotestoragename"
REMOTE_STORAGE_ACCOUNT_KEY = "3pJwX7wjxoQEW2nKAhJZARpQWpKvPOUN9JwoV/HhlMmJlS1pORhzzHpPfQqgFcwGsriu6dwYqnugWOjGShC5VQ=="
PREMIUM_STORAGE_ACCOUNT_NAME = "premiumstoragename"
PREMIUM_STORAGE_ACCOUNT_KEY = "NzhL3hKZbJBuJ2484dPTR+xF30kYaWSSCbs2BzLgVVI1woqeST/1IgqaLm6QAOTxtGvxctSNbIR/1hW8yH+bJg=="

# Use instead of STORAGE_ACCOUNT_NAME and STORAGE_ACCOUNT_KEY if custom settings are needed
CONNECTION_STRING = ""
BLOB_CONNECTION_STRING = ""
PREMIUM_CONNECTION_STRING = ""

# Use 'https' or 'http' protocol for sending requests, 'https' highly recommended
PROTOCOL = "https"

# Set to true to target the development storage emulator
IS_EMULATED = False

# Set to true if server side file encryption is enabled
IS_SERVER_SIDE_FILE_ENCRYPTION_ENABLED = True

# Decide which test mode to run against. Possible options:
#   - Playback: run against stored recordings
#   - Record: run tests against live storage and update recordings
#   - RunLiveNoRecord: run tests against live storage without altering recordings
TEST_MODE = 'RunLiveNoRecord'

# Set up proxy support
USE_PROXY = False
PROXY_HOST = "192.168.15.116"
PROXY_PORT = "8118"
PROXY_USER = ""
PROXY_PASSWORD = ""
