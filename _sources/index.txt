.. pydocumentdb documentation master file, created by
   sphinx-quickstart.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Azure Storage SDK for Python.
========================================

Installation:
-------------
 
You can use ``pip`` to install the latest released version of ``azure-storage``::

    pip install azure-storage

If you want to install ``azure-storage`` from source::

    git clone git://github.com/Azure/azure-storage-python.git
    cd azure-storage-python
    python setup.py install

If you are looking for Azure Service Bus or the Azure management libraries, 
please visit https://github.com/Azure/azure-sdk-for-python
   
Documentation:
--------------
* `Blob <https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-blob-storage/>`__ -- (:doc:`API <ref/azure.storage.blob>`)
* `Queue <https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-queue-storage/>`__ -- (:doc:`API <ref/azure.storage.queue>`)
* `Table <https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-table-storage/>`__ -- (:doc:`API <ref/azure.storage.table>`)
* `File <https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-file-storage/>`__ -- (:doc:`API <ref/azure.storage.file>`)
* :ref:`All Documentation <modindex>`


Features:
---------

-  Blobs

   -  create, list, and delete containers, work with container metadata
      and permissions, list blobs in container
   -  create block and page blobs (from a stream, a file, or a string),
      work with blob blocks and pages, delete blobs
   -  work with blob properties, metadata, leases, snapshot a blob

-  Queues

   -  create, list, and delete queues, and work with queue metadata
   -  create, get, peek, update, delete messages

-  Tables

   -  create and delete tables
   -  create, query, insert, update, merge, and delete entities

-  Files

   -  create, list, and delete shares, work with share metadata, 
      list directories and files in share
   -  create and delete directories, work with directory properties
      and metdata
   -  create files (from a stream, a local file, or a string)
   -  work with file and directory properties and metadata


System Requirements:
--------------------

The supported Python versions are 2.7.x, 3.3.x, and 3.4.x
To download Python, please visit
https://www.python.org/download/


We recommend Python Tools for Visual Studio as a development environment for developing your applications.  Please visit http://aka.ms/python for more information.


Need Help?:
-----------

Be sure to check out the Microsoft Azure `Developer Forums on Stack
Overflow <http://go.microsoft.com/fwlink/?LinkId=234489>`__ if you have
trouble with the provided code.

Contributing:
-------------
Contribute Code or Provide Feedback:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you would like to become an active contributor to this project please
follow the instructions provided in `Microsoft Azure Projects
Contribution
Guidelines <http://windowsazure.github.com/guidelines.html>`__.

If you encounter any bugs with the library please file an issue in the
`Issues <https://github.com/Azure/azure-storage-python/issues>`__
section of the project.

Learn More
==========

`Microsoft Azure Python Developer
Center <http://azure.microsoft.com/en-us/develop/python/>`__


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
  :hidden:

.. toctree::
  :hidden:
  :glob:

  upgrade
  ref/*