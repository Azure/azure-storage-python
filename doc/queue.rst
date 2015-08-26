Queue Storage
===============================

The **create\_queue** method can be used to ensure a queue exists:

.. code:: python

    from azure.storage.queue import QueueService
    queue_service = QueueService(account_name, account_key)
    queue_service.create_queue('taskqueue')

The **put\_message** method can then be called to insert the message
into the queue:

.. code:: python

    from azure.storage.queue import QueueService
    queue_service = QueueService(account_name, account_key)
    queue_service.put_message('taskqueue', 'Hello world!')

It is then possible to call the **get\_messages** method, process the
message and then call **delete\_message** with the message id and
receipt. This two-step process ensures messages don't get lost when they
are removed from the queue.

.. code:: python

    from azure.storage.queue import QueueService
    queue_service = QueueService(account_name, account_key)
    messages = queue_service.get_messages('taskqueue')
    queue_service.delete_message('taskqueue', messages[0].message_id, messages[0].pop_receipt)

If you want to give access to a queue to a third party without revealing
your account key, you can use a shared access signature. The shared
access signature includes an access policy, with optional start, expiry
and permission.

To generate a shared access signature, use:

.. code:: python

    from azure.storage import AccessPolicy, SharedAccessPolicy
    from azure.storage.queue import QueueService, QueueSharedAccessPermissions
    queue_service = QueueService(account_name, account_key)
    ap = AccessPolicy(
        expiry='2016-10-12',
        permission=QueueSharedAccessPermissions.READ,
    )
    sas_token = queue_service.generate_shared_access_signature(
        'taskqueue',
        SharedAccessPolicy(ap),
    )

Alternatively, you can define a named access policy on the server:

.. code:: python

    from azure.storage import SharedAccessPolicy, SignedIdentifier
    from azure.storage.queue import QueueService, QueueSharedAccessPermissions
    queue_service = QueueService(account_name, account_key)

    policy_name = 'readonlyvaliduntilnextyear'

    si = SignedIdentifier()
    si.id = policy_name
    si.access_policy.expiry = '2016-01-01'
    si.access_policy.permission = QueueSharedAccessPermissions.READ
    identifiers = SignedIdentifiers()
    identifiers.signed_identifiers.append(si)

    queue_service.set_queue_acl(
        queue_name='taskqueue',
        signed_identifiers=identifiers,
    )

And generate a shared access signature for that named access policy:

.. code:: python

    sas_token = queue_service.generate_shared_access_signature(
        'taskqueue',
        SharedAccessPolicy(signed_identifier=policy_name),
    )

Using a predefined access policy has the advantage that it can be
revoked from the server side. To revoke, call ``set_queue_acl`` with the
new list of signed identifiers. You can pass in ``None`` or an empty
list to remove all.

.. code:: python

    queue_service.set_container_acl(
        queue_name='taskqueue',
        signed_identifiers=None,
    )

The third party can use the shared access signature token to
authenticate, instead of an account key:

.. code:: python

    from azure.storage.queue import QueueService
    queue_service = QueueService(account_name, sas_token=sas_token)
    messages = queue_service.peek_messages('taskqueue')