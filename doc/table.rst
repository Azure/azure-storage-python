Table Storage
===============================

To ensure a table exists, call **create\_table**:

.. code:: python

    from azure.storage.table import TableService
    table_service = TableService(account_name, account_key)
    table_service.create_table('tasktable')

A new entity can be added by calling **insert\_entity**:

.. code:: python

    from datetime import datetime
    table_service = TableService(account_name, account_key)
    table_service.create_table('tasktable')
    table_service.insert_entity(
         'tasktable',
         {
            'PartitionKey' : 'tasksSeattle',
            'RowKey': '1',
            'Description': 'Take out the trash',
            'DueDate': datetime(2011, 12, 14, 12) 
        }
    )

The method **get\_entity** can then be used to fetch the entity that was
just inserted:

.. code:: python

    table_service = TableService(account_name, account_key)
    entity = table_service.get_entity('tasktable', 'tasksSeattle', '1')

If you want to give access to a table to a third party without revealing
your account key, you can use a shared access signature. The shared
access signature includes an access policy, with optional start, expiry
and permission.

To generate a shared access signature, use:

.. code:: python

    from azure.storage import AccessPolicy, SharedAccessPolicy
    from azure.storage.table import TableService, TableSharedAccessPermissions
    table_service = TableService(account_name, account_key)
    ap = AccessPolicy(
        expiry='2016-10-12',
        permission=TableSharedAccessPermissions.QUERY,
    )
    sas_token = table_service.generate_shared_access_signature(
        'tasktable',
        SharedAccessPolicy(ap),
    )

Alternatively, you can define a named access policy on the server:

.. code:: python

    from azure.storage import SharedAccessPolicy, SignedIdentifier
    from azure.storage.table import TableService, TableSharedAccessPermissions
    table_service = TableService(account_name, account_key)

    policy_name = 'readonlyvaliduntilnextyear'

    si = SignedIdentifier()
    si.id = policy_name
    si.access_policy.expiry = '2016-01-01'
    si.access_policy.permission = TableSharedAccessPermissions.QUERY
    identifiers = SignedIdentifiers()
    identifiers.signed_identifiers.append(si)

    table_service.set_queue_acl(
        table_name='tasktable',
        signed_identifiers=identifiers,
    )

And generate a shared access signature for that named access policy:

.. code:: python

    sas_token = table_service.generate_shared_access_signature(
        'tasktable',
        SharedAccessPolicy(signed_identifier=policy_name),
    )

Using a predefined access policy has the advantage that it can be
revoked from the server side. To revoke, call ``set_table_acl`` with the
new list of signed identifiers. You can pass in ``None`` or an empty
list to remove all.

.. code:: python

    table_service.set_table_acl(
        table_name='tasktable',
        signed_identifiers=None,
    )

The third party can use the shared access signature token to
authenticate, instead of an account key:

.. code:: python

    from azure.storage.table import TableService
    table_service = TableService(account_name, sas_token=sas_token)
    entity = table_service.get_entity('tasktable', 'tasksSeattle', '1')