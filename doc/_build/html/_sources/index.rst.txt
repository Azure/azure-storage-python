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
* `Blob Getting Started Doc <https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-blob-storage/>`__ -- (:doc:`API <ref/azure.storage.blob>`)
* `Queue Getting Started Doc <https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-queue-storage/>`__ -- (:doc:`API <ref/azure.storage.queue>`)
* `Table Getting Started Doc <https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-table-storage/>`__ -- (:doc:`API <ref/azure.storage.table>`)
* `File Getting Started Doc <https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-file-storage/>`__ -- (:doc:`API <ref/azure.storage.file>`)
* :ref:`Reference Documentation - All Services<modindex>`


Features:
---------

-  Blob

   -  Create/Read/Update/Delete Containers
   -  Create/Read/Update/Delete Blobs
   -  Advanced Blob Operations

-  Queue

   -  Create/Delete Queues
   -  Insert/Peek Queue Messages
   -  Advanced Queue Operations

-  Table

   -  Create/Read/Update/Delete Tables
   -  Create/Read/Update/Delete Entities
   -  Batch operations
   -  Advanced Table Operations

-  Files

   -  Create/Update/Delete Shares
   -  Create/Update/Delete Directories
   -  Create/Read/Update/Delete Files
   -  Advanced File Operations


System Requirements:
--------------------

The supported Python versions are 2.7.x, 3.3.x, 3.4.x, and 3.5.x.
To download Python, please visit
https://www.python.org/download/


We recommend Python Tools for Visual Studio as a development environment for developing your applications. Please visit http://aka.ms/python for more information.


Need Help?:
-----------

Be sure to check out the Microsoft Azure `Developer Forums on Stack
Overflow <http://go.microsoft.com/fwlink/?LinkId=234489>`__ if you have
trouble with the provided code.

Contributing:
-------------
Contribute Code or Provide Feedback:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you would like to become an active contributor to this project, please
follow the instructions provided in `Microsoft Azure Projects
Contribution
Guidelines <http://windowsazure.github.com/guidelines.html>`__.

If you encounter any bugs with the library, please file an issue in the
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