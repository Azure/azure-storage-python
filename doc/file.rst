File Storage
===============================

The **create\_share** method can be used to create a share in
which to store a directory or file:

.. code:: python

    from azure.storage.file import FileService
    file_service = FileService(account_name, account_key)
    file_service.create_share('myshare')

To create a directory 'uploads' within the share, the method
**create\_directory** can be used:

.. code:: python

    from azure.storage.file import FileService
    file_service = FileService(account_name, account_key)
    file_service.create_directory(
        'myshare',
        'uploads',
    )

To upload a file 'localimage.png' from disk to a file named
'image.png', the method **put\_file\_from\_path** can be used:

.. code:: python

    from azure.storage.file import FileService
    file_service = FileService(account_name, account_key)
    file_service.put_file_from_path(
        'myshare',
		'uploads',
        'image.png',
        'localimage.png',
        max_connections=5,
    )

The **max\_connections** parameter is optional, and lets you use multiple
parallel connections to perform uploads and downloads.  This parameter is
available on the various upload and download methods described below.

To upload an already opened local file to an Azure file named 'localimage.png', 
the method **put\_file\_from\_stream** can be used instead. The **count** parameter
is required and indicates how many bytes you want read from the local file and 
upload to the Azure file.

.. code:: python

    with open('localimage.png') as localfile:
        file_service.put_file_from_stream(
            'myshare',
		    'uploads',
            'image.png',
            localfile,
            count=50000,
            max_connections=4,
        )

To upload unicode text, use **put\_file\_from\_text** which will
do the conversion to bytes using the specified encoding.

To upload bytes, use **put\_file\_from\_bytes**.

To download a file named 'image.png' to a file on disk
'downloads/localimage.png', where the 'downloads' folder already exists, the
**get\_file\_to\_path** method can be used:

.. code:: python

    from azure.storage._file import FileService
    file_service = FileService(account_name, account_key)
    file = file_service.get_file_to_path(
        'myshare',
		'uploads',
        'image.png',
        'downloads/localimage.png',
        max_connections=8,
    )

To download to an already opened file, use **get\_file\_to\_stream**.

To download to an array of bytes, use **get\_file\_to\_bytes**.

To download to unicode text, use **get\_file\_to\_text**.