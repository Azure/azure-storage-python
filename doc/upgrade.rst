Upgrade Guide
===============================

This guide is intended to help upgrade code written for the Azure Storage Python 
library before version 0.30.0.

The best way to see how to upgrade a specific API is to take a look at the usage 
samples in the `Samples <https://github.com/Azure/azure-storage-python/tree/master/samples>`__ 
directory on GitHub. A very specific set of changes as well as additions can be 
found in the ChangeLog and BreakingChanges documents. The below is a summary of 
those documents containing the most relevant pieces for the upgrade scenario.

General Changes
===============================

In general, we attempted to use more appropriate Python types for parameter and 
return values rather than always taking and receiving strings. Parameter and return 
values previously prefixed with x_ms were simplified by removing this prefix, and 
parameter values are divided by '_' between words as is idiomatic.

Listing returns a generator rather than a result segment. This generator automatically 
follows continuation tokens as more results are requested.

SAS methods take several individual parameters rather than a single paramter 
object. Similarly, ACL getters and setters take dictionaries mapping id to 
AccessPolicy rather than a list of SignedIdentifiers each holding an id and an 
AccessPolicy. 

Blob
===============================

The single BlobService object was divided into three subservices for the different 
blob types (BlockBlobService, PageBlobService, AppendBlobService) with common 
methods in the abstract BaseBlobService these inherit from. This was done for two 
reasons. First, to reduce confusion when blob-type specific methods were used on 
the incorrect blob type. Second, to simplify the BlobService object which had grown 
to be quite extensive when append blob was added.

ContentSettings objects have replaced all content_* and cache_control parameters 
and return values for applicable APIs. This is intended to highlight that the 
semantics of setting content properties is replace rather than merge, meaning that 
setting any one property will cause any unspecified properties to be cleared on 
the blob.

On the performance side, single-threaded blob download APIs will now download 
the blob without chunking to improve perf and not do an initial get to find the 
size of the blob. However, as a result the progress_callback may receive None 
for its total parameter when parallelism is off to allow for this optimization.

Queue
===============================

The largest change to the QueueService class is that queue messages are both XML 
encoded and decoded by default. In past versions, either messages were not encoded 
or decoded by default, or only encoded. Encoding and decoding methods can be 
modified using the QueueService encode_function and decode_function instance variables.

Methods operating on queue messages and which have return values will return 
QueueMessage objects. QueueMessages objects contain essentially the same fields 
as previously, but the times are returned as dates rather than strings and the 
dequeue count is returned as an int.

Table
===============================

Rather than having a boolean switch for turning batching on and off, batches are 
an object which can be populated and then committed. Entities can be sent as dictionaries 
or as Entity objects, and returned entities are accessible as either objects or 
dictionaries. Methods which access and modify entites have been simplified so that 
if they take an entity object they extract the partition key and row key from that 
object rather than requiring these be sent separately.

All table entity integer values are stored on the service with type Edm.Int64 
unless the type is explicitly overridden as Edm.Int32. Formerly, the type was 
decided based on the size of the number, but this resulted in hard to predict 
types on the service. So, the more consistent option was chosen.

Operations no longer echo content from the service and JSON is used instead of 
AtomPub, improving performance.