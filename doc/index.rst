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

On Windows, you need to build the ``cffi`` and ``cryptography`` dependencies natively. See the documentation for those modules, or install from wheel: `cffi 1.10.0 <https://pypi.python.org/packages/0a/ad/5b5e6783d7500fb32978ff7869dc77333b0707b6ebf0d76d23b45134f9c1/cffi-1.10.0-cp34-cp34m-win32.whl#md5=0dcb0100c1941e62510e8abc13f7ee9f>`__, `cryptography 1.5 <https://pypi.python.org/packages/83/5b/59b7de8c938a1856585d91cccd648c3ff2af14ec518051b3bfd50d6a9f73/cryptography-1.5-cp34-cp34m-win32.whl#md5=896b2588347b6d2ebdba2754b035ed1a>`__ (both links are for Python 3.4 in 32-bit environments, the default in Azure Web Apps). On Azure Web Apps, you also need to change the ``cp34m`` portion of the wheel file name to ``none``. 

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
