
* __Address route and subpipeline for pipeline tranformation (#4528)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Mon, 13 May 2024 15:58:11 -0700
    
    EAD -&gt; refs/heads/main, refs/remotes/origin/main, refs/remotes/origin/HEAD
    Address route and subpipeline for pipeline tranformation
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;

* __Fix an issue that exception messages are masked (#4416)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 13 May 2024 16:25:42 -0500
    
    efs/remotes/origin/2.8
    * Show exception stacktrace
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Tweak log messages
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix aggregate processor local mode (#4529)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 13 May 2024 12:30:19 -0700
    
    
    * Fix aggregate processor local mode
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added comments and removed commented out code in test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed code for failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Key value processor enhancements (#4521)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 13 May 2024 08:53:15 -0700
    
    
    * Added auto mode
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Testing changes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Testing changes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added new config and tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Checkpoint records at an interval for TPS case when AckSet is enabled  (#4526)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Sat, 11 May 2024 08:27:29 -0500
    
    
    * Checkpoint records at an interval for TPS case when AckSet is enabled
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Fix regular checkpoint internal with ack received
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix write json basic test (#4527)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 10 May 2024 14:13:33 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Complete AcknowledgementSet for checkpoint thread &amp; fix BsonTimeStamp field conversion (#4525)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 10 May 2024 10:10:45 -0500
    
    
    * Complete AcknowledgementSet for checkpoint thread when Ack is enabled
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Use time in seconds for BsonTimeStamp field when converting to record
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Write stream events that timeout to write to internal buffer in separate thread (#4524)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 9 May 2024 19:51:23 -0500
    
    
    * Write stream events that timeout to write to internal buffer in separate
    thread
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Fix Log message level and minor PR comments
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Handle exception in Stream Acknowledgement Manager.
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Extend the lease by default time
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Modify typeof operator grammar and add support for ArrayList (#4523)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 9 May 2024 13:54:36 -0700
    
    
    * Modify typeof operator grammar and add support for ArrayList
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkstyle error
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added test case for more code coverage
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Clear system property to disable s3 scan when stream worker exits (#4522)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 9 May 2024 14:38:58 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add logging change to documentdb source and s3 source with folder partitions (#4519)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 9 May 2024 10:22:38 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bump parquet version to 1.14.0. (#4520)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Thu, 9 May 2024 09:50:48 -0500
    
    
    1.14.0 has critical bug fixes for parquet files written in OCSF 1.1
    schema.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Write json processor (#4514)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 8 May 2024 20:48:47 -0700
    
    
    * Write Json processor
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Write Json processor - added tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed the DataPrepperMarker
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add bytesProcessed metric for stream (#4518)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 8 May 2024 21:37:23 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add bucket owner validation support to s3 sink (#4504)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 8 May 2024 13:28:35 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Exclude updateDescription field from Mongo/DocDB change stream (#4516)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 8 May 2024 10:42:38 -0500
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix document version for DocDB/Mongo Stream events (#4513)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 7 May 2024 13:55:37 -0500
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __MAINT: inject external origination timestamp (#4507)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 7 May 2024 13:46:05 -0500
    
    
    * MAINT: inject external origination timestamp
    
    * MAINT: set External origination time in event handle
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    

* __Add metadata for primary key and primary key type and validations for Mongo/DocDb source (#4512)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 7 May 2024 12:03:33 -0500
    
    
    * Add metadata for primary key and primary key type and validations for
    Mongo/DocDb source
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Update docdb metrics (#4508)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 7 May 2024 10:30:40 -0500
    
    
    * Update docdb metrics
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix depth field in template (#4509)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Tue, 7 May 2024 06:53:51 -0500
    
    
    Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;

* __Add BsonType of primary key to metadata (#4506)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 6 May 2024 18:19:11 -0500
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add support to export/full load MongoDB/DocumentDB collection with `_id` field of different data type (#4503)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 6 May 2024 17:24:20 -0500
    
    
    * Add support to export/full load MongoDB/DocumentDB collection with _id field
    of different data type
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add internal type for sorting
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Fix &#39;static&#39; modifier out of order
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add &#39;typeof&#39; operator to DataPrepper expressions (#4500)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 6 May 2024 15:12:44 -0700
    
    
    * Rebased to latest
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed unrelated changes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added new tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkStyle errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkStyle errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add new S3 source option and modify docdb template (#4492)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Mon, 6 May 2024 11:18:21 -0500
    
    
    Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;

* __Move stopping of s3 scan from docdb leader scheduler to stream scheduler (#4498)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 6 May 2024 11:01:52 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Initial work to support core data types in Data Prepper (#4496)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 3 May 2024 12:59:49 -0700
    
    
    Adds an enum to represent core data types in Data Prepper. This is initially
    created to help connect the convert_entry_type processor with upcoming work for
    evaluating type information as part of #4478.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support default mapping for Mongo/DocumentDB data types  (#4499)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 3 May 2024 13:55:00 -0500
    
    
    * Support default mapping for Mongo/DocumentDB data types
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Fix test indentation
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Use ParameterizedTest for Mongo/DocDB dataType mapping
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Support Mongo/DocDB stream Delete and other Operation types (#4497)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 2 May 2024 17:35:04 -0500
    
    
    * Support Mongo/DocDB stream Delete and other Operation types
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add reset checkpoint method to mongo/docdb partition checkpoint class
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __HTTP data chunking support for kafka buffer (#4475)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 2 May 2024 14:51:46 -0700
    
    
    * HTTP data chunking support for kafka buffer
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed comments
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed comments and added tests
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkstyle errors
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkstyle error
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Add system property to allow pausing and resuming s3 scan worker thread processing, optionally set this property to pause on documentdb leader (#4495)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 2 May 2024 14:36:51 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __FIX: null certificate value should be valid in opensearch connection (#4494)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 2 May 2024 10:22:24 -0500
    
    
    * FIX: null certificate value should be valid
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fix JacksonEvent to propagate ExternalOriginalTime if its set at the time of construction (#4489)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 1 May 2024 17:06:53 -0700
    
    
    * Fix JacksonEvent to propagate ExternalOriginalTime if its set at the time of
    construction
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added test
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed code coverage failure by adding more tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Move MongoDB/DocDB stream checkpoint to separate thread (#4477)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 30 Apr 2024 19:16:55 -0500
    
    
    * Move MongoDB/DocDB stream checkpoint to separate thread
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Checkpoint DocDB/MongoDB stream processing periodically
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix transient test failure for subpipelines (#4479)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Tue, 30 Apr 2024 13:37:53 -0700
    
    
    Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;

* __Fix for S3PartitionCreatorScheduler ConcurrentModification Exception (#4473)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 29 Apr 2024 12:41:24 -0500
    
    
    * Fix for S3PartitionCreatorScheduler ConcurrentModification Exception
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Remove warn log message
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Publish data-prepper-expression and data-prepper-logstash-configuration to Maven. (#4474)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 29 Apr 2024 10:37:28 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix DocumentDB source S3PathPrefix null or empty (#4472)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 29 Apr 2024 11:26:40 -0500
    
    
    * Fix DocumentDB source S3PathPrefix null or empty
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Modify documentdb template (#4469)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Fri, 26 Apr 2024 13:19:50 -0500
    
    
    Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;

* __Changing logging level for config transformation and fixing rule (#4466)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Fri, 26 Apr 2024 12:50:08 -0500
    
    
    * Changing logging level for config transformation and fixing rule
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Adding absolute path for template and rule
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Fix template indentation
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Fix template indentation
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Adding event json to codec in template
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Handled PipelinesDataFlow mapping and addressed array type when replacing
    Node
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Add relative path as file stream
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Fix environment variable
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    ---------
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;

* __Let data-prepper-core know if docdb has acknowledgments enabled (#4467)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 26 Apr 2024 10:39:09 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add folder-based partitioning for s3 scan source (#4455)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 25 Apr 2024 23:45:55 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Pipeline Configuration Transformation (#4446)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Wed, 24 Apr 2024 20:13:36 -0700
    
    
    * Adding templates
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Added Dynamic yaml transformer
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Added Rule evaluator
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Added rule evaluator
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Added json walk
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Add transformation logic
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Add dynamic rule
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Almost working
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Adding multiple pipelines part1
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Adding multiple pipelines part2-incomplete
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Works e2e for 1 pipeline
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Added multi pipeline template and pipelinemodel support, works for docDB with
    big template
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * added tests for models and fixed beans, one more fix needed for bean
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Fixed IT and beans
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Update bean to have only pipelineDataModel and not parser
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Add banner
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Code cleanup and add comments
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Support user pipeline configuration dynamic transformation based on
    
    templates and rules
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Address comments
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Added Function Call support in templates
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Added Function Call support in templates
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Modify documentDB template.
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Code clean up
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Code clean up
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    ---------
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;

* __MAINT: use authentication-for-basic-credentials (#4435)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 24 Apr 2024 14:39:25 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Adding Support for &#39;BigDecimal&#39; for integer, long, double, string type. (#4395)__

    [Utkarsh-Aga](mailto:126544832+Utkarsh-Aga@users.noreply.github.com) - Wed, 24 Apr 2024 11:00:27 -0700
    
    
    Adding Support for &#39;BigDecimal&#39; for the following &#39;Target type&#39; - integer,
    long, double, string
     Signed-off-by: Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;
    Co-authored-by:
    Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;

* __MAINT: use authentication block to replace username and password in opensearch sink (#4438)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 24 Apr 2024 12:35:26 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Revert DocumentDB Source String array host to String host (#4457)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 24 Apr 2024 12:02:01 -0500
    
    
    * Revert DocumentDB Source String array host to String host
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    

* __MAINT: allow latest schema version if not specified in confluent schema (#4453)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 24 Apr 2024 11:13:54 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Update DocumentDB source pipeline config parameters  (#4451)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 24 Apr 2024 11:11:46 -0500
    
    
    * Update DocumentDB source pipeline config parameters
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Added support for multiple workers in S3 Scan Source (#4439)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 23 Apr 2024 13:56:14 -0700
    
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing integration tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing check style
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __MAINT: deprecate certificate_content with certificate_key (#4434)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 23 Apr 2024 10:11:55 -0500
    
    
    * MAINT: deprecate certificate_content with certificate_key
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Support for Event Json input and output codecs (#4436)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 22 Apr 2024 16:22:17 -0700
    
    
    * Event Json input and output codecs
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified test case to check for event metadata attributes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified the coverage to 0.9
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixes for failing coverage tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed test coverage
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added more tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added more tests for coverage
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed code coverage failure
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __ENH: docdb credential auto refreshment (#4399)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 22 Apr 2024 16:22:29 -0500
    
    
    * ENH: docdb credential refreshment
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Use positive numbers in S3DlqWriterTest for consistent test success. (#4443)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 22 Apr 2024 08:27:15 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Maint/renaming kafka source plugin setting (#4429)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 19 Apr 2024 09:29:17 -0500
    
    
    * MAINT: schema registry setting renaming
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: allow either plain or plaintext under SASL (#4433)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 19 Apr 2024 09:28:20 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: deprecate pipeline_configurations with extension (#4428)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 19 Apr 2024 09:27:16 -0500
    
    
    * MAINT: deprecate-pipeline_configurations with extension
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Updates Ameria to 1.28.2. To support this change, also updates gRPC to 1.63.0, and Netty to 4.1.108. Fixes unit tests that were expecting a failure. Armeria fixed a bug so the old expectation was no longer valid. (#4440)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Apr 2024 16:32:28 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Make s3 partition size configurable and add unit test for S3 partition creator classes (#4437)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 18 Apr 2024 13:19:44 -0500
    
    
    * Make s3 partition size configurable and add unit test for S3 partition
    creator classes
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Rename export partition size to export batch size
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Change s3 sink client to async client (#4425)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 18 Apr 2024 11:09:26 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Remove creating S3 prefix path partition upfront (#4432)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 17 Apr 2024 14:59:30 -0500
    
    
    This will be done when S3 sink writes data with path prefix.
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Gradle 8.7 (#4417)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 17 Apr 2024 11:02:05 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Rename DocDB/MongoDB config parameters (#4426)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 16 Apr 2024 18:15:34 -0500
    
    
    * Rename DocDB/MongoDB config parameters
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update hostname to host in DocDB/MongDb config parameter
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Encode MongoDB/DocumentDB username and password while constructing connection string (#4423)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 16 Apr 2024 13:30:35 -0500
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Moves the Maven publish Gradle configuration into a Gradle convention plugin. This splits the build logic for publication and allows different projects to determine whether they publish to Maven rather than having to track this with conditionals in the root Gradle project. (#4421)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 16 Apr 2024 10:57:25 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix acknowledgements in DynamoDB (#4419)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 15 Apr 2024 16:45:58 -0700
    
    
    * Fix acknowledgements in DynamoDB
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Fix bug in s3 sink dynamic bucket and catch invalid bucket message (#4413)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 11 Apr 2024 18:48:14 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __AWS DocumentDB Stream/Export improvements with S3 partition (#4409)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 11 Apr 2024 17:02:12 -0500
    
    
    * AWS DocumentDB Stream/Export improvements with S3 partition
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update S3 Folder partition with one global state
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add support for dynamic bucket and default bucket in S3 sink (#4402)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 11 Apr 2024 16:51:19 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Do not write empty lists of DlqObject to the DLQ. Just ignore these requests in the S3DlqWriter class. (#4403)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Apr 2024 12:14:20 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Validate the AWS account Id in the S3 source when configuring either the default_bucket_owner or the bucket_owners map. Implemented this by adding a new bean validation annotation @AwsAccountId in the aws-plugin-api. Resolves #4398 (#4400)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Apr 2024 12:14:13 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bug where using upsert or update without routing parameter caused NoSuchElementException (#4397)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 11 Apr 2024 13:51:18 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Create new codec for each s3 group in s3 sink (#4410)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 11 Apr 2024 11:18:34 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Create unit test report as html (#4384)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 10 Apr 2024 15:48:21 -0500
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Modified GRPC exception handler return BAD_REQUEST for certain internal errors (#4387)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 9 Apr 2024 15:16:58 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Simple script to help generate the release notes file as a starting point (#4322)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 9 Apr 2024 12:43:46 -0700
    
    
    This PR adds a small script that I&#39;ve used to create release notes recently. It
    can help us create release notes going forward.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __MAINT: float up exception (#4391)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 9 Apr 2024 14:24:25 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Updates the S3/file codecs to use the EventFactory over JacksonLog/JacksonEvent builder(). (#4355)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 9 Apr 2024 11:47:14 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add developer guide/integration test section (#4330)__

    [Jürgen Walter](mailto:juergen.walter@sap.com) - Fri, 5 Apr 2024 14:08:33 -0700
    
    
    * Add developer guide/integration test section
     Aligned with Kafka example
    
    https://github.com/opensearch-project/data-prepper/blob/51ee0df595aaad1b921c888394bf3e110ffc74e9/data-prepper-plugins/kafka-plugins/README.md#developer-guide
    
     Signed-off-by: Jürgen Walter &lt;juergen.walter@sap.com&gt;
    
    * Use latest OpenSearch for testing
     Version 2.12.0 onwards requires to use non-default admin password
    
    https://opensearch.org/blog/replacing-default-admin-credentials/
     Signed-off-by: Jürgen Walter &lt;juergen.walter@sap.com&gt;
    
    ---------
     Signed-off-by: Jürgen Walter &lt;juergen.walter@sap.com&gt;

* __Corrects the release date for 2.7.0 (#4349)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 5 Apr 2024 08:42:11 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Cache geolocation data within any given batch of Data Prepper events to avoid extra calls to the MaxMind GeoIP code. (#4357)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 5 Apr 2024 07:24:16 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds KarstenSchnitter as a maintainer to the Data Prepper project (#4389)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 5 Apr 2024 07:23:06 -0700
    
    
    Adds KarstenSchnitter as a maintainer to the Data Prepper project.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Remove unexpected event handle message (#4388)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 4 Apr 2024 15:09:03 -0700
    
    
    * Remove unexpected event handle message
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add aggregate_threshold with maximum_size to s3 sink (#4385)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 4 Apr 2024 16:54:05 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add server connections metric to http and otel sources (#4393)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 4 Apr 2024 10:53:26 -0500
    
    
    * Add metric to http source
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add to Otel logs source
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add to Otel metrics source
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add to Otel trace source
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Optimize imports
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added &#39;long&#39; as a target type for &#39;convert_entry_type&#39; processor (#4359)__

    [Utkarsh-Aga](mailto:126544832+Utkarsh-Aga@users.noreply.github.com) - Thu, 4 Apr 2024 08:14:59 -0700
    
    
    Added &#39;long&#39; as a target type for &#39;convert_entry_type&#39; processor
     Signed-off-by: Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;
    
    ---------
     Signed-off-by: Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;
    Co-authored-by:
    Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;

* __Log the User-Agent when Data Prepper shuts down from the POST /shutdown request. (#4390)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 4 Apr 2024 07:12:33 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __ENH: automatic credential refresh in kafka source (#4258)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 3 Apr 2024 14:49:20 -0500
    
    
    * ADD: basic credentials and dynamic refreshment
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fix flaky PipelineConfigurationFileReaderTest (#4386)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 3 Apr 2024 12:16:16 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Do not require field_split_characters is not empty for key_value processor (#4358)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 3 Apr 2024 12:15:52 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Update Stream Ack Manager unit test and code refactor (#4383)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 2 Apr 2024 15:17:16 -0500
    
    
    * Update Stream Ack Manager unit test
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Move CheckpointStatus to stream sub package and make it package protected
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add AcknowledgementSet support to DocumentDB/MongoDB streams (#4379)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 2 Apr 2024 13:20:59 -0500
    
    
    * Add AcknowledgementSet support to DocumentDB/MongoDB streams
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update StreamAcknowledgementManagerTest
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add BackgroundThreadFactory that adds thread name prefix for debugging
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update unit test
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add creation and aggregation of dynamic S3 groups based on events (#4346)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 1 Apr 2024 17:43:12 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Aggregate Processor: local mode should work when there is no when condition (#4380)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 1 Apr 2024 12:22:09 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Fix count aggregation exemplar data (#4341)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 1 Apr 2024 12:12:39 -0700
    
    
    * Fix count aggregation exemplar data
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-15df884d.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-15df884d.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Signed-off-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-15df884d.us-west-2.amazon.com&gt;
    
    Co-authored-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-15df884d.us-west-2.amazon.com&gt;

* __Bump org.jetbrains.kotlin:kotlin-stdlib-common (#4363)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Apr 2024 09:29:19 -0700
    
    
    Bumps
    [org.jetbrains.kotlin:kotlin-stdlib-common](https://github.com/JetBrains/kotlin)
    from 1.9.22 to 1.9.23.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.9.23/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.9.22...v1.9.23)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#4362)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Apr 2024 09:25:52 -0700
    
    
    Bumps software.amazon.awssdk:auth from 2.25.0 to 2.25.21.
    
    ---
    updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Adds integration tests for the geoip processor (#4353)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 28 Mar 2024 12:06:31 -0700
    
    
    Adds integration tests for the geoip processor.
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: David
    Venable &lt;dlv@amazon.com&gt;
    Co-authored-by: Asif Sohail Mohammed
    &lt;nsifmoh@amazon.com&gt;

* __ENH: automatic credential refresher opensearch sink (#4283)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 28 Mar 2024 13:49:44 -0500
    
    
    ENH: automatic credential refresher opensearch sink
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Integration test to verify that the core HTTP server starts. (#4255)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 28 Mar 2024 09:19:08 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes the S3 source tests when running together (#4280)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 28 Mar 2024 09:18:33 -0700
    
    
    When running all the S3 source tests together, there are empty objects. This
    adds a mocked counter to avoid a NPE.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for MongoDB/DocumentDB stream processing (#4338)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 28 Mar 2024 11:05:36 -0500
    
    
    * Add support for MongoDB/DocumentDB stream processing
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add test for DataStreamPartitionCheckpoint
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Fix ExportScheduler unit test
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add ingestion type meta data and minor fixes
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update unit test
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Use the org.lz4:lz4-java project instead of the old net.jpountz.lz4 project. (#4347)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 27 Mar 2024 16:27:28 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds new metrics to help debug errors connecting to OpenSearch. Resolves #4343, #4344 (#4348)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 27 Mar 2024 15:51:00 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix the release build by only applying the Maven publish task to either projects with a name starting with data-prepper or any projects that are children of the data-prepper-plugins project. (#4337)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 26 Mar 2024 15:28:44 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Handle errors from OpenSearch by checking status field as well as error (#4335)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 26 Mar 2024 14:27:44 -0700
    
    
    Handle errors from OpenSearch by checking both the status field and the error
    body for each bulk response item.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes the release flag from the release GitHub Action. It is no longer needed and it conflicts with the release task. (#4332)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 26 Mar 2024 12:14:51 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Revert HTTP data chunking changes for kafka buffer done in PR 4266 (#4329)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 26 Mar 2024 11:14:08 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Adds the release notes for Data Prepper 2.7.0. (#4321)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 26 Mar 2024 10:44:24 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix Router performance issue (#4327)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 25 Mar 2024 16:17:02 -0700
    
    
    * Fix Router performance issue
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Truncate Processor: Add support to truncate all fields in an event (#4317)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 25 Mar 2024 11:09:01 -0700
    
    
    Truncate Processor: Add support to truncate all fields in an event
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Split up the Maven groupIds for core, plugins, and test to match package names. (#4324)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 25 Mar 2024 10:44:36 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the next Data Prepper version to 2.8. (#4320)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 22 Mar 2024 14:05:51 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Configure the MaxMind database by default and update it so that uses the correct defaults when loading. Resolves #3942 (#4310)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 21 Mar 2024 14:05:36 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.jetbrains.kotlin:kotlin-stdlib-common (#3877)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 14:05:03 -0700
    
    
    Bumps
    [org.jetbrains.kotlin:kotlin-stdlib-common](https://github.com/JetBrains/kotlin)
    from 1.8.21 to 1.9.22.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.8.21...v1.9.22)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.jetbrains.kotlin:kotlin-stdlib-common (#3886)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 14:04:51 -0700
    
    
    Bumps
    [org.jetbrains.kotlin:kotlin-stdlib-common](https://github.com/JetBrains/kotlin)
    from 1.8.21 to 1.9.22.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.8.21...v1.9.22)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.mock-server:mockserver-junit-jupiter-no-dependencies (#3785)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 14:03:53 -0700
    
    
    Bumps org.mock-server:mockserver-junit-jupiter-no-dependencies from 5.14.0 to
    5.15.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.mock-server:mockserver-junit-jupiter-no-dependencies
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.httpcomponents.client5:httpclient5 (#4056)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 14:03:02 -0700
    
    
    Bumps
    [org.apache.httpcomponents.client5:httpclient5](https://github.com/apache/httpcomponents-client)
    from 5.2 to 5.3.1.
    -
    [Changelog](https://github.com/apache/httpcomponents-client/blob/rel/v5.3.1/RELEASE_NOTES.txt)
    
    -
    [Commits](https://github.com/apache/httpcomponents-client/compare/rel/v5.2...rel/v5.3.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.httpcomponents.client5:httpclient5
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.apptasticsoftware:rssreader in /data-prepper-plugins/rss-source (#4206)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 14:01:46 -0700
    
    
    Bumps [com.apptasticsoftware:rssreader](https://github.com/w3stling/rssreader)
    from 3.5.0 to 3.6.0.
    - [Release notes](https://github.com/w3stling/rssreader/releases)
    - [Commits](https://github.com/w3stling/rssreader/compare/v3.5.0...v3.6.0)
    
    ---
    updated-dependencies:
    - dependency-name: com.apptasticsoftware:rssreader
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#4216)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 14:01:19 -0700
    
    
    Bumps software.amazon.awssdk:auth from 2.24.5 to 2.25.0.
    
    ---
    updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.maven:maven-artifact from 3.0.3 to 3.9.6 (#4226)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 14:00:28 -0700
    
    
    Bumps [org.apache.maven:maven-artifact](https://github.com/apache/maven) from
    3.0.3 to 3.9.6.
    - [Release notes](https://github.com/apache/maven/releases)
    - [Commits](https://github.com/apache/maven/compare/maven-3.0.3...maven-3.9.6)
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.maven:maven-artifact
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.maven:maven-artifact in /data-prepper-plugins/opensearch (#3775)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 14:00:05 -0700
    
    
    Bumps [org.apache.maven:maven-artifact](https://github.com/apache/maven) from
    3.0.3 to 3.9.6.
    - [Release notes](https://github.com/apache/maven/releases)
    - [Commits](https://github.com/apache/maven/compare/maven-3.0.3...maven-3.9.6)
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.maven:maven-artifact
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.mapdb:mapdb in /data-prepper-plugins/mapdb-processor-state (#4059)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 13:57:12 -0700
    
    
    Bumps [org.mapdb:mapdb](https://github.com/jankotek/mapdb) from 3.0.10 to
    3.1.0.
    -
    [Commits](https://github.com/jankotek/mapdb/compare/mapdb-3.0.10...mapdb-3.1.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.mapdb:mapdb
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump io.gatling.gradle from 3.10.3.1 to 3.10.4 in /performance-test (#4215)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 13:56:08 -0700
    
    
    Bumps io.gatling.gradle from 3.10.3.1 to 3.10.4.
    
    ---
    updated-dependencies:
    - dependency-name: io.gatling.gradle
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/s3-sink (#4217)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 13:55:21 -0700
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.12.5
    to 2.12.7.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.12.5...v2.12.7)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/s3-source (#4207)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 13:55:07 -0700
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.12.6
    to 2.12.7.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.12.6...v2.12.7)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/rss-source (#4205)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 Mar 2024 13:54:47 -0700
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.12.6
    to 2.12.7.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.12.6...v2.12.7)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Adds a new tags_on_no_valid_ip configuration to geoip. Tag these events differently than the tags_on_ip_not_found condition. (#4307)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 21 Mar 2024 13:46:29 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Resolves transitive dependencies for 2.7.0 (#4308)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 21 Mar 2024 13:34:04 -0700
    
    
    Updates transitive dependencies to resolve CVE-2023-51775, CVE-2024-23944,
    CVE-2023-52428. Move some constraints such that they are only in the projects
    needing them. Resolves #4282, #4290, #4296.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Send acknowledgements to source when events are forwarded to remote peer (#4305)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 21 Mar 2024 11:10:44 -0700
    
    
    Send acknowledgements to source when events are forwarded to remote peer
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Provide a config option to do node local aggregation (#4306)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 20 Mar 2024 22:50:53 -0700
    
    
    * Provide a config option to do node local aggregation
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified config option to be local_mode
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Refactor PipelinesDataFlowModelParser to take in an InputStream instead of a file path (#4289)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 20 Mar 2024 16:35:41 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    

* __Fix the default behavior of geoip such that all fields from the databases are used. (#4303)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 20 Mar 2024 09:42:58 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix flaky test in ExportPartitionWorkerTest (#4300)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 19 Mar 2024 15:40:34 -0500
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-core (#4284)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 13:06:30 -0700
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.22.1 to 2.23.1.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.22.1...rel/2.23.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Improve geoip performance by reducing database lookups (#4297)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 19 Mar 2024 11:10:06 -0700
    
    
    Use the city database instead of the country database if city fields are
    needed. This prevents duplicate reads. Optimize the databases to use Country if
    all the fields are available in that database. It looks up locations faster
    than the city database.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support to use old ddb stream image for REMOVE events (#4275)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 19 Mar 2024 10:22:37 -0700
    
    
    Add suport to use old ddb stream image for REMOVE events
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bump org.apache.logging.log4j:log4j-jpl in /data-prepper-core (#4202)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 10:08:14 -0700
    
    
    Bumps org.apache.logging.log4j:log4j-jpl from 2.22.1 to 2.23.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-jpl
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#4209)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 09:44:28 -0700
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.14.11 to 1.14.12.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.11...byte-buddy-1.14.12)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-proto-common (#4219)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 09:43:17 -0700
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.25.2 to 3.25.3.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.25.2...assertj-build-3.25.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-logs-source (#4221)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 09:39:13 -0700
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.25.2 to 3.25.3.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.25.2...assertj-build-3.25.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#4220)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 09:38:26 -0700
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.25.2 to 3.25.3.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.25.2...assertj-build-3.25.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-trace-source (#4214)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 09:36:37 -0700
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.25.2 to 3.25.3.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.25.2...assertj-build-3.25.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#4212)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 09:36:09 -0700
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.25.2 to 3.25.3.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.25.2...assertj-build-3.25.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/http-source (#4201)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 09:35:52 -0700
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.25.2 to 3.25.3.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.25.2...assertj-build-3.25.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#4204)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 19 Mar 2024 09:35:22 -0700
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.25.2 to 3.25.3.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.25.2...assertj-build-3.25.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __GeoIP performance: database expiration and InetAddress optimization (#4286)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 19 Mar 2024 07:07:40 -0700
    
    
    Performance improvements for the geoip processor: Only parse the IP address
    once and track the database expiration for each batch of the geoip processor.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Random Apache log data from performance test (#4243)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 19 Mar 2024 06:54:35 -0700
    
    
    Updates the performance test tool to generate random data for Common Apache
    Logs. This includes predefined and random IPv4 addresses as well as predefined
    IPv6 addresses. By default, only IPv4 addresses are supplied from the
    predefined list. When sending the single log, use a randomly generated log.
    Adds a StaticRequestSimulation to send the exact static log instead.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __DocumentDB/MongoDB source initial checkpoint progress and other improvemnts (#4293)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 18 Mar 2024 16:47:26 -0500
    
    
    * DocumentDB/MongoDB source initial checkpoint progress and other improvemnts
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Undo typo
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Use Awaitility in two of the mongodb unit tests instead of depending on Thread.sleep(). (#4288)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 15 Mar 2024 13:22:02 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Refactors the geoip code packages by moving more classes into the extension package. Also make quite a few projects package protected as they are specific to the extension. (#4269)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 15 Mar 2024 07:41:48 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Name the S3 source&#39;s SQS worker threads (#4279)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 15 Mar 2024 07:41:04 -0700
    
    
    Creates a new common class for creating background threads. Name the threads
    for the S3 source&#39;s SQS worker threads.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Unit test fix for MongoDB LeaderScheduler (#4287)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 15 Mar 2024 07:39:53 -0700
    
    
    Unit test fix for MongoDB LeaderScheduler
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add DocumentDB/MongoDB source for initial load (#4285)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 14 Mar 2024 17:41:35 -0500
    
    
    * Add DocumentDB/MongoDB source for initial load
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update unit test parameter
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Extract the data-prepper-plugin-framework from data-prepper-core (#4260)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 14 Mar 2024 16:02:54 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add ExportScheduler and LeaderScheduler for MongoDB/DocumentDB source (#4277)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 14 Mar 2024 13:07:28 -0500
    
    
    * Add ExportScheduler and LeaderScheduler for MongoDB/DocumentDB source
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update access modifier for static field
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Unit test updates
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add MongoDB Export Partition supplier
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Logging collection when creating scheduler
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Split HTTP source data to multiple chunks before writing to byte buffer (#4266)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 13 Mar 2024 17:18:57 -0700
    
    
    * Split HTTP source data to multiple chunks before writing to byte buffer
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified JsonDecoder to parse objects in addition to array of objects
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added a test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed json to array conversion
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to add new JsonObjDecoder for decoding single json objects
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed changes to JsonDecoder
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Renamed JsonObjDecoder to JsonObjectDecoder
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Fix ip check and add more tests (#4278)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 13 Mar 2024 16:54:33 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add source coordinator partition and partition state (#4276)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 13 Mar 2024 14:33:17 -0500
    
    
    * Add source coordinator partition and partition state
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Remove unused import
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add support for mongodb/documentdb initial load record export (#4271)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 13 Mar 2024 09:48:39 -0500
    
    
    * Initial Mongo/DocumentDB source Configuration and Client
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Initial Mongo/DocumentDB source Configuration and Client
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Initial Mongo/DocumentDB source Configuration and Client
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update setting gradle file with mongodb module
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Renamed Configuration parameters and refactored the MongoDBHelper class
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add MongoDB Connection helper class
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add support for mongodb/documentdb initial load record export
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Updated mongodb event metadata attribute name
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Refactored record conversion into functional class
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Remove the S3 transfer manager in favor of using the S3 client directly to download files. All download scenarios are straightforward and don&#39;t need the transfer manager. This removes some unnecessary dependencies and reduces the overall file size. (#4257)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 12 Mar 2024 14:55:28 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support large files in file source by using a thread (#4256)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 12 Mar 2024 10:39:51 -0700
    
    
    Run the file source in its own thread so that Data Prepper can read large
    files.
     Also adds stop calls to RandomStringSourceTests because these were continuing
    to run and using up memory.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Initial Mongo/DocumentDB source Configuration and Client (#4265)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 12 Mar 2024 11:09:27 -0500
    
    
    * Initial Mongo/DocumentDB source Configuration and Client
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Initial Mongo/DocumentDB source Configuration and Client
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Initial Mongo/DocumentDB source Configuration and Client
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update setting gradle file with mongodb module
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Renamed Configuration parameters and refactored the MongoDBHelper class
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add MongoDB Connection helper class
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix bug where process worker would shut down if a processor drops all… (#4262)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 12 Mar 2024 10:48:38 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Adds a GitHub Action to verify the aws-testing-cdk project. It runs the linter and tests. Adds a unit test for this project. Correct the formatting to pass the linter. (#4263)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 12 Mar 2024 08:40:21 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for creating SSLContext for trustStore file path (#4264)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 11 Mar 2024 17:42:52 -0500
    
    
    * Add support for creating SSLContext for trustStore file path
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add support for creating SSLContext for trustStore file path
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-expression (#4213)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 11 Mar 2024 15:03:34 -0700
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.22.1 to 2.23.0.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.22.1...rel/2.23.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __MNT: system property on confluent IT (#4259)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 11 Mar 2024 14:57:16 -0700
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Creates a simple example of using Fluentd to send data from a file to Data Prepper. (#4248)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 11 Mar 2024 12:13:15 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Rename first and second database directories to blue and green to avoid indicating order. (#4249)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 11 Mar 2024 09:05:42 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add all subprojects as core projects. (#4254)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Fri, 8 Mar 2024 12:20:01 -0800
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Fix release task after broken by #4247. (#4253)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Fri, 8 Mar 2024 11:31:42 -0600
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Set up serverless network policy before setting up index (#4250)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 8 Mar 2024 10:02:50 -0600
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Extract parsing in data-prepper-core to data-prepper-pipeline-parser module (#4247)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 7 Mar 2024 17:34:18 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add key_value_when conditional to key_value processor (#4246)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 7 Mar 2024 15:47:30 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add release task to publish all jars. (#4238)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Thu, 7 Mar 2024 13:09:40 -0800
    
    
    This commit adds a task called release which will publish all of the
    data
    prepper jars individually.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Make using the EventFactory easier by adding a test package and class to get a test EventFactory. It uses the actual EventFactory for now, but could vary in the future. Creates two now Gradle projects - one of the event factory and another for getting a test EventFactory. This also updates the file source to create Events using the EventFactory as the initial source. (#4110)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 7 Mar 2024 12:13:40 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Various fixes for the geoip processor: avoid DNS lookups on IP addresses, better User-Agent, configuration constraints, use correct database_destination. Updates MaxMind dependencies to the latest version. Remove unused project. (#4244)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 7 Mar 2024 11:54:56 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add Buffer Latency Metric (#4237)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 7 Mar 2024 08:25:24 -0800
    
    
    * Add Buffer Latency Metric
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing code verification test by adding new test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added null check before calling updateLatency
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Bump org.wiremock:wiremock in /data-prepper-plugins/s3-source (#4208)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 7 Mar 2024 07:59:35 -0800
    
    
    Bumps [org.wiremock:wiremock](https://github.com/wiremock/wiremock) from 3.3.1
    to 3.4.2.
    - [Release notes](https://github.com/wiremock/wiremock/releases)
    - [Commits](https://github.com/wiremock/wiremock/compare/3.3.1...3.4.2)
    
    ---
    updated-dependencies:
    - dependency-name: org.wiremock:wiremock
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Fix CVEs CVE-2024-22201  and CVE-2023-3635 (#4192)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 7 Mar 2024 07:38:24 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Modify S3 Source to create multiple SqsWorkers (#4239)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 6 Mar 2024 12:54:22 -0800
    
    
    * Modify S3 Source to create multiple SqsWorkers
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments and added integration test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Organizes the GeoIP processor Java packages. Use plugins.geoip as the root package and then move processor-specific classes into plugins.geoip.processor. (#4240)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 Mar 2024 09:57:40 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __ENH: use timer for latency (#4174)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 6 Mar 2024 09:41:01 -0800
    
    
    ENH: use timer for latency
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add exclude_keys option to flatten processor (#4231)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 5 Mar 2024 14:55:27 -0600
    
    
    * Add exclude_keys option to flatten processor
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address comments - refactor excludeKeySet
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add check for if performance_metadata is enabled before adding metadata for grok processing time to Events (#4236)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 5 Mar 2024 14:52:43 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Change grok performance_metadata to be disabled by default (#4235)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 5 Mar 2024 13:18:05 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Geoip config enhancements (#4167)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 5 Mar 2024 09:14:03 -0800
    
    
    Add when condition to each entry. Reverted when condition to processor level.
    Updated metric logic and Added new test. Geoip database readers update
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Removes the type-conversion-processor project. The actual processor is part of the mutate-event-processors project. (#4234)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 5 Mar 2024 08:03:52 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Create parse xml processor (#4191)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 4 Mar 2024 15:37:22 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add support for metadata in Events for the total time spent in grok (#4230)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 4 Mar 2024 15:36:58 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix pipeline latency to calculate correct latency when persistent buffer is used (#4187)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 29 Feb 2024 16:44:27 -0600
    
    
    * Fix pipeline latency to calculate correct latency when persistent buffer is
    used
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkstyle error and addressed comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Fix KafkaBuffer isEmpty (#4200)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 29 Feb 2024 16:40:36 -0600
    
    
    * Fix KafkaBuffer isEmpty
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing test
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add include_performance_metadata parameter and track total grok patterns attempted in the grok processor (#4197)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 29 Feb 2024 12:10:02 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix to set kafka source truststore path and password only when they are not null value (#4199)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 29 Feb 2024 10:29:11 -0600
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add flatten processor (#4138)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 28 Feb 2024 15:20:09 -0600
    
    
    * Initial commit
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add complete functionality and tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add test cases with dots in keys
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update JacksonEvent tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Rename project to flatten-processor
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Rename flattenjson to flatten
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix commons-compress CVE (#4172)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Feb 2024 15:06:21 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __This updates Jackson to 2.16.1. This version does include a change to redact input data in exceptions. So a couple tests needed modifications. Additionally, this uses ion-java 1.10.5 which has a fix for  CVE-2024-21634, so this PR will resolve #3926. (#4134)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Feb 2024 15:05:53 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for spilt event processor (#4166)__

    [Srikanth Govindarajan](mailto:srikanthjg123@gmail.com) - Wed, 28 Feb 2024 14:28:25 -0600
    
    
    * Add support for spilt event processor
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Add support for spilt event processor(#4089)
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Add support for spilt event processor(#4089)
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Add support for spilt event processor(#4089)
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    ---------
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    Signed-off-by: Srikanth
    Govindarajan &lt;srikanthjg123@gmail.com&gt;

* __Missed setting the default value in PR #4190 (#4193)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 27 Feb 2024 15:28:43 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Set default value of flattenAttributes to true in Otel metrics source (#4190)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 27 Feb 2024 12:06:32 -0800
    
    
    * Set default value of flattenAttributes to true in Otel metrics source
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added test for get/setFlattenAttributes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkstyle errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Modify shutdown listeners to support a List of listeners (#4189)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 27 Feb 2024 12:43:19 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Support certificate content in Opensearch Source configuration to sup… (#4184)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 26 Feb 2024 16:44:33 -0600
    
    
    * Support certificate content in Opensearch Source configuration to support TLS
    and truststore on the client.
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update unit test try block coverage for staic mocked object
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add select_entries processor (#4147)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Sat, 24 Feb 2024 21:36:39 -0800
    
    
    * Add select_entries processor
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add new OTEL Metrics source that creates events (#4183)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Sat, 24 Feb 2024 21:35:56 -0800
    
    
    * Add new OTEL Metrics source that creates events
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to replace existing processor with new functionality where new
    events are created in the source
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Allow format expression in keys in add_entries processor (#4182)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 23 Feb 2024 17:30:50 -0600
    
    
    * Allow format expression in keys
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments: use format expression
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add csv_when parameter to the csv processor (#4179)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 23 Feb 2024 16:56:36 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Support Kafka SASL_SSL/SSL security protocol for self signed certificate in Kafka Consumer (#4181)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 23 Feb 2024 14:41:11 -0800
    
    
    * Support Kafka SASL_SSL/SSL security protocol for self signed certificate in
    Kafka Consumer
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Support Kafka SASL_SSL/SSL security protocol for self signed certificate in
    Kafka Consumer
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Support Kafka SASL_SSL/SSL security protocol for self signed certifice
    
    - Renamed the field names for certificate, truststore and password.
    - Removed redundant information from build.gradle
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Support Kafka SASL_SSL/SSL security protocol for self signed certificate in
    Kafka Consumer
    
    - Add the copyright block to build.gradle
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update unit test to include actual random values
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix OpenSearchSink upsert operation (#4178)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 22 Feb 2024 16:38:23 -0800
    
    
    * Fix opensearch upsert operation in Opensearch Sink
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to to Stringutils.equals
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Update dissect and user_agent readme (#4100)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 22 Feb 2024 10:21:23 -0600
    
    
    * Update dissect and user_agent readme
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Fix format issue
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix kafka plugin dependencies. (#4169)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Wed, 21 Feb 2024 12:53:51 -0800
    
    
    1. Integration test dependencies were being pulled in when compiling
      source code and unit tests.
    2. The wrong namespace for json-schema-validator
    was being used.
    3. Remove catching BrokerEndPointNotAvailableException because
    that
      exception will not be thrown by Kafka clients.3. Remove catching
    
    BrokerEndPointNotAvailableException because that exception will not be
    thrown
    by Kafka clients.3. Remove catching
    BrokerEndPointNotAvailableException
    because that exception will not be
    thrown by Kafka clients.
    Signed-off-by:
    Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Moves the repository declarations to the settings.gradle file to help maintain in one place. Also limit the groups used by the JitPack Maven repository to the one dependency that is needed. This way, we use Maven Central over it. (#4161)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 21 Feb 2024 12:35:19 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Catch processor exceptions instead of shutting down (#4155) (#4162)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 21 Feb 2024 11:18:47 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Geoip database update implementation (#4105)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 20 Feb 2024 20:02:46 -0600
    
    
    * Geoip processor implementation
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Delay reading from the Kafka buffer as long as the circuit breaker is open (#4135)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 20 Feb 2024 14:06:08 -0800
    
    
    Hold off on consuming from the Kafka topic as long as a pause-consume predicate
    is in place. This will allow the Kafka buffer to wait for the circuit breaker
    to close before reading. Also pause the topic while the circuit breaker is
    open.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Catch processor exceptions instead of shutting down (#4155)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 20 Feb 2024 11:36:24 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add append option to add_entries processor (#4143)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 20 Feb 2024 11:32:51 -0600
    
    
    * Add append option
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address comments: combine two mergeValue methods
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address comments: update assertion message
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Adds the CHANGELOG for Data Prepper 2.6.2. (#4156)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 20 Feb 2024 06:26:27 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#4153)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 19 Feb 2024 12:48:22 -0800
    
    
    Bumps software.amazon.awssdk:auth from 2.23.13 to 2.24.5.
    
    ---
    updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Registers some common producer metrics in the Kafka buffer. (#4139)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Feb 2024 11:52:13 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Make expressions evaluating on keys with List or Map values not throw an exception (#4142)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 19 Feb 2024 13:06:16 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Replace try-catch with if check (#4144)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 19 Feb 2024 11:17:22 -0600
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Removes unnecessary mavenCentral() declarations in Gradle sub-projects. (#4152)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Feb 2024 09:14:42 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds release notes for Data Prepper 2.6.2 (#4149)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Feb 2024 08:28:15 -0800
    
    
    * Adds release notes for Data Prepper 2.6.2
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump grpcio in /release/smoke-tests/otel-span-exporter (#4104)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 19 Feb 2024 06:39:25 -0800
    
    
    Bumps [grpcio](https://github.com/grpc/grpc) from 1.53.0 to 1.53.2.
    - [Release notes](https://github.com/grpc/grpc/releases)
    -
    [Changelog](https://github.com/grpc/grpc/blob/master/doc/grpc_release_schedule.md)
    
    - [Commits](https://github.com/grpc/grpc/compare/v1.53.0...v1.53.2)
    
    ---
    updated-dependencies:
    - dependency-name: grpcio
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __KafkaConsumer should continue to poll while waiting for buffer (#4023)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 16 Feb 2024 12:36:32 -0800
    
    
    * KafkaConsumer should continue to poll while waiting for buffer
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to call pause() whenever parititon assignment changes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Updates the JDK version of the release to jdk-17.0.10+7. (#4136)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Feb 2024 12:01:21 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update README.md (#4093)__

    [Travis Benedict](mailto:benedtra@amazon.com) - Fri, 16 Feb 2024 09:56:06 -0600
    
    
    Signed-off-by: Travis Benedict &lt;benedtra@amazon.com&gt;

* __Support for logging MDC in the Kafka buffer (#4131)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 15 Feb 2024 14:31:49 -0800
    
    
    Uses logging MDC within the KafkaBuffer entry points. Create the Kafka Buffer
    consumer threads with MDC. Name the consumer threads to help when tracking down
    thread dumps. First part of #4126
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.hibernate.validator:hibernate-validator (#3791)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 15 Feb 2024 08:56:45 -0800
    
    
    Bumps
    [org.hibernate.validator:hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 7.0.5.Final to 8.0.1.Final.
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/main/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/7.0.5.Final...8.0.1.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Require json-path 2.9.0 to fix CVE-2023-51074. Resolves #3919. (#4132)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 15 Feb 2024 08:54:45 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.hibernate.validator:hibernate-validator (#3799)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 15 Feb 2024 08:28:34 -0800
    
    
    Bumps
    [org.hibernate.validator:hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 7.0.5.Final to 8.0.1.Final.
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/main/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/7.0.5.Final...8.0.1.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Allow . and @ characters to be part of json pointer in expressions (#4130)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 15 Feb 2024 08:19:57 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Removes the Kafka Connect plugins project. (#4090)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 15 Feb 2024 08:12:58 -0800
    
    
    Removes the Kafka Connect plugins project. Deletes the source. Updates the
    README.md.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Create decrompress processor to decompress gzipped keys (#4118)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 14 Feb 2024 14:08:36 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Enhancements to map_to_list processor (#4033)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 14 Feb 2024 12:44:23 -0600
    
    
    * Add convert-field-to-list option
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix bug where s3 scan could skip when lastModifiedTimestamps are the same (#4124)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 14 Feb 2024 11:07:22 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Corrections to Kafka integration tests in GitHub (#4115)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 13 Feb 2024 09:16:21 -0800
    
    
    Replaces missing tests in the Kafka integration tests. Corrected the Kafka
    tests to run against the PR target so that secrets are available.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes a bug where the Kafka buffer inverted the relationship for the create_topic configuration. Supports better unit testing through some refactoring. Resolves #4111 (#4114)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 13 Feb 2024 09:14:48 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the S3 source README.md to link to the user documentation and retain the developer guide. (#4094)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 13 Feb 2024 09:13:26 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added support for &#39;epoch_micro&#39; in date processor (#4098)__

    [Utkarsh-Aga](mailto:126544832+Utkarsh-Aga@users.noreply.github.com) - Tue, 13 Feb 2024 08:45:06 -0800
    
    
    * Added support for &#39;epoch_micro&#39; in date processor
     Signed-off-by: Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;
    
    * Added support for &#39;epoch_micro&#39; in date processor
     Signed-off-by: Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;
    
    ---------
     Signed-off-by: Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;
    Co-authored-by:
    Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;

* __Updates the dynamodb source README.md to refer to the user documentation (#4095)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 12 Feb 2024 16:57:30 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Catch exception instead of shutting down in date processor (#4108)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 12 Feb 2024 12:07:01 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add when condition to each geoip entry (#4034)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 12 Feb 2024 10:13:45 -0600
    
    
    * Add when condition
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Runs the Kafka buffer KMS tests as part of the GitHub Actions (#4041)__

    [David Venable](mailto:dlv@amazon.com) - Sat, 10 Feb 2024 08:24:36 -0800
    
    
    Runs the Kafka buffer KMS tests using the DataPrepperTesting KMS key. Resolves
    #4040
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support input codecs in the file source. Resolves #4018. (#4019)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 9 Feb 2024 15:16:46 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Enhance copy_values processor to selectively copy entries from lists (#4085)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 9 Feb 2024 15:09:53 -0600
    
    
    * initial experiment
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add list copy options
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Remove unused imports
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Extract shouldCopyEntry method
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Bump org.jetbrains.kotlin:kotlin-stdlib in /data-prepper-plugins/s3-sink (#3876)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Feb 2024 10:50:37 -0800
    
    
    Bumps [org.jetbrains.kotlin:kotlin-stdlib](https://github.com/JetBrains/kotlin)
    from 1.8.21 to 1.9.22.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.8.21...v1.9.22)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/rss-source (#4058)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Feb 2024 10:48:49 -0800
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.12.5
    to 2.12.6.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.12.5...v2.12.6)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/s3-source (#4063)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Feb 2024 10:48:21 -0800
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.12.5
    to 2.12.6.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.12.5...v2.12.6)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump io.gatling.gradle from 3.10.3 to 3.10.3.1 in /performance-test (#4055)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Feb 2024 10:47:27 -0800
    
    
    Bumps io.gatling.gradle from 3.10.3 to 3.10.3.1.
    
    ---
    updated-dependencies:
    - dependency-name: io.gatling.gradle
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-expression (#3896)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Feb 2024 10:46:52 -0800
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.22.0 to 2.22.1.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.22.0...rel/2.22.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __When writing Kafka buffer events, save additional information about the encryption in the protobuf record. Contributes toward #3655. (#3976)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 8 Feb 2024 13:54:14 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.json:json in /data-prepper-plugins/avro-codecs (#4087)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 8 Feb 2024 11:13:29 -0800
    
    
    Bumps [org.json:json](https://github.com/douglascrockford/JSON-java) from
    20230227 to 20240205.
    - [Release notes](https://github.com/douglascrockford/JSON-java/releases)
    -
    [Changelog](https://github.com/stleary/JSON-java/blob/master/docs/RELEASES.md)
    - [Commits](https://github.com/douglascrockford/JSON-java/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.json:json
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#4042)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 6 Feb 2024 14:00:26 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.24.2 to 3.25.2.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.24.2...assertj-build-3.25.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/http-source (#4051)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 6 Feb 2024 14:00:03 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.24.2 to 3.25.2.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.24.2...assertj-build-3.25.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#4053)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 6 Feb 2024 13:59:32 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.24.2 to 3.25.2.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.24.2...assertj-build-3.25.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-logs-source (#4043)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 6 Feb 2024 13:34:01 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.24.2 to 3.25.2.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.24.2...assertj-build-3.25.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#4057)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 6 Feb 2024 13:31:57 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.24.2 to 3.25.2.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.24.2...assertj-build-3.25.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.xerial.snappy:snappy-java in /data-prepper-plugins/s3-source (#4061)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 6 Feb 2024 13:31:30 -0800
    
    
    Bumps [org.xerial.snappy:snappy-java](https://github.com/xerial/snappy-java)
    from 1.1.10.1 to 1.1.10.5.
    - [Release notes](https://github.com/xerial/snappy-java/releases)
    -
    [Commits](https://github.com/xerial/snappy-java/compare/v1.1.10.1...v1.1.10.5)
    
    ---
    updated-dependencies:
    - dependency-name: org.xerial.snappy:snappy-java
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-trace-source (#4064)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 6 Feb 2024 13:30:59 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.24.2 to 3.25.2.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.24.2...assertj-build-3.25.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-proto-common (#4065)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 6 Feb 2024 13:29:59 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.24.2 to 3.25.2.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.24.2...assertj-build-3.25.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump dash in /examples/trace-analytics-sample-app/sample-app (#4078)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 5 Feb 2024 18:41:33 -0800
    
    
    Bumps [dash](https://github.com/plotly/dash) from 2.14.1 to 2.15.0.
    - [Release notes](https://github.com/plotly/dash/releases)
    - [Changelog](https://github.com/plotly/dash/blob/dev/CHANGELOG.md)
    - [Commits](https://github.com/plotly/dash/compare/v2.14.1...v2.15.0)
    
    ---
    updated-dependencies:
    - dependency-name: dash
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Lower min duration validation for scheduled s3 scan interval from 30 seconds to 1 second (#4082)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 5 Feb 2024 15:57:40 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Enhancements to list_to_map processor (#4038)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 5 Feb 2024 14:36:52 -0600
    
    
    * Test key as optional
    * Add new options; simplify existing code
    * Add options to use source key as key in result map
    * Add tags_on_failure option
    * Remove restrictions on  option
    * Address review comments
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add join function (#4075)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 5 Feb 2024 14:36:18 -0600
    
    
    * Add join function
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __FIX: plugin callback not loaded for secret refreshment (#4079)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 5 Feb 2024 09:38:52 -0600
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __ENH: error handling in opensearch client refreshment and metrics (#4039)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 2 Feb 2024 16:17:40 -0600
    
    
    * ENH: error handling in client refreshment and metrics
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Bump com.github.seancfoley:ipaddress in /data-prepper-expression (#4060)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 2 Feb 2024 09:34:17 -0800
    
    
    Bumps
    [com.github.seancfoley:ipaddress](https://github.com/seancfoley/IPAddress) from
    5.4.0 to 5.4.2.
    - [Release notes](https://github.com/seancfoley/IPAddress/releases)
    - [Commits](https://github.com/seancfoley/IPAddress/compare/v5.4.0...v5.4.2)
    
    ---
    updated-dependencies:
    - dependency-name: com.github.seancfoley:ipaddress
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates the Kafka client libraries to the latest - Apache Kafka clients to 3.6.1 and the MSK IAM authentication library to 2.0.3. Removes the AWS SDK v1 Glue package. (#4048)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 1 Feb 2024 11:05:50 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __ENH: allow disable secret refreshment (#3990)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 31 Jan 2024 15:41:13 -0600
    
    
    * ENH: allow disable secret refreshment
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Tag events that fail for all exceptions in the grok processor. Resolves #4031 (#4032)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 30 Jan 2024 12:04:34 -0800
    
    
    Tags events that fail for all exceptions. Resolves #4031
     Adds a tags_on_timeout configuration which tags events that timeout
    differently from those that fail for other reasons. Configure the default
    behavior of tags_on_timeout to take on the value of tags_on_match_failure.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Throw a more useful error when the S3 source is unable to determine the bucket ownership to use. (#4021)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 30 Jan 2024 10:58:36 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Integration tests for using the Kafka buffer with KMS encryption (#3982)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 30 Jan 2024 08:15:09 -0800
    
    
    Adds a new integration test for using the Kafka buffer with KMS encryption.
    Includes a new KMS CDK stack for any projects that need KMS to use. Some
    improvements to the CDK stack. Resolves #3980
     Change the GitHub tests to include only the Kafka buffer tests which current
    run in GitHub.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#4030)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 30 Jan 2024 07:48:28 -0800
    
    
    Bumps software.amazon.awssdk:auth from 2.20.67 to 2.23.13.
    
    ---
    updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates jline to 3.25.0 to resolve CVE-2023-50572. (#4020)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 29 Jan 2024 16:29:59 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Changing version AWSSDK V1 to V2 (#4025)__

    [Prathyusha Vangala](mailto:157630736+shaavanga@users.noreply.github.com) - Mon, 29 Jan 2024 14:45:32 -0800
    
    
    Changing AWSSDK v1 to v2
     Signed-off-by: shaavanga &lt;prathyuvanga@gmail.com&gt;
    Co-authored-by: Prathyusha
    Vangala &lt;shavanga@amazon.com&gt;

* __Cancel the existing grok task when a timeout occurs. Resolves #4026 (#4027)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 29 Jan 2024 14:44:54 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added when condition and fixed building reader on each event (#4002)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 29 Jan 2024 14:01:11 -0600
    
    
    * Added when condition and fixed building reader on each event
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Aggregator processor should evaluate aggregate_when condition before forwarding events to remote peer (#4004)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 24 Jan 2024 17:26:31 -0800
    
    
    Aggregator processor should evaluate aggregate_when condition before forwarding
    events to remote peer
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Allow flexible DateTime pattern location in index name (#4000)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 24 Jan 2024 16:18:07 -0600
    
    
    * Allow datetime pattern anywhere in index name
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add integ tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Remove leading dash in indexPrefix
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update index patterns
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add integ tests for verifying index template creation
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update readme
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update unit tests to address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Use regex Pattern for replaceAll
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added ByteCount to plugin parser (#4003)__

    [Guian Gumpac](mailto:guian.gumpac@improving.com) - Wed, 24 Jan 2024 12:30:46 -0800
    
    
    Signed-off-by: Guian Gumpac &lt;guian.gumpac@improving.com&gt;

* __Fixes a build failure related to geoip. (#4005)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Jan 2024 14:38:44 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates our OTel plugin projects to use JUnit 5 from JUnit 4. Use package protected access for the tests and methods. (#3970)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Jan 2024 13:59:16 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Rebased to latest and addressed review comments (#3998)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 23 Jan 2024 09:46:49 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Check that System.out.println or Exception::printStackTrace() are not used. Removed usage of those two in our main code and tests. (#3991)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Jan 2024 09:11:05 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the common plugin and mapdb-processor-state projects to JUnit 5. (#3949)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Jan 2024 08:11:00 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Log when Data Prepper is shutdown via the HTTP shutdown endpoint. (#4001)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Jan 2024 07:59:30 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Change SQS visibility timeout change message log level to debug level (#3997)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 22 Jan 2024 12:50:07 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Update geoip config and use extensions (#3975)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 22 Jan 2024 12:54:37 -0600
    
    
    * Update geoip config and use extensions
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Use the official unit string for the &#39;b&#39; when creating the ByteCount string. (#3993)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 19 Jan 2024 12:06:30 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Improves usability of the ByteCount class by implementing equals()/hashCode()/toString(). (#3960)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 19 Jan 2024 11:43:44 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Kafka error handling in KafkaBuffer (#3974)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 19 Jan 2024 11:35:53 -0800
    
    
    * Rebased to latest
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing spotless check
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Mark the EventHandle as transient in the JacksonEvent to fix a serialization error with peer forwarding. Resolves #3981. (#3983)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Jan 2024 14:41:38 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix typo in truncate processor documentation (#3985)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 18 Jan 2024 14:02:44 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __ENH: metrics in secret extension (#3922)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 18 Jan 2024 14:50:19 -0600
    
    
    * ENH: add metrics in aws secrets extension
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    

* __ENH: support secret binary value (#3923)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 18 Jan 2024 11:37:36 -0600
    
    
    * ENH: support secret binary value
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Updates the Gatling version in the performance-test to 3.10.3. The 3.10 release includes a breaking change that required updating a Consumer to a Function. Added some additional instructions for running the performance tests against Amazon OpenSearch Ingestion. (#3955)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Jan 2024 08:46:45 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds a new integration test for the Kafka buffer which verifies that data written is correctly read and decrypted. This work will be used to verify upcoming changes to the Protobuf model. (#3973)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 17 Jan 2024 15:08:08 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Greatly reduce the time that the KafkaSinkTest takes by mocking the constructor. Update the unit tests to JUnit 5. Only the integration tests need JUnit 4 now, but these use quite a few JUnit 4 features. (#3972)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 17 Jan 2024 13:07:35 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Remove references to default admin creds (#3869)__

    [Derek Ho](mailto:derek01778@gmail.com) - Wed, 17 Jan 2024 12:45:40 -0800
    
    
    Signed-off-by: Derek Ho &lt;dxho@amazon.com&gt;

* __Updates wiremock to 3.3.1. This also involves changing the groupId to org.wiremock which is the new groupId as of 3.0.0. (#3969)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 17 Jan 2024 10:25:32 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add geoip service extension (#3944)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 17 Jan 2024 11:56:20 -0600
    
    
    * Add geoip service extension
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Release events that are not routed to any sinks (#3959)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 16 Jan 2024 11:08:33 -0800
    
    
    * Release events that are not routed to any sinks
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed a bug in the code that&#39;s causing the test failures
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to determine unrouted events after all routing is done
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Add test yaml files
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __GitHub-issue#1994 : Implementation Of Cloudwatch metrics source plugin c… (#3128)__

    [venkataraopasyavula](mailto:126578319+venkataraopasyavula@users.noreply.github.com) - Tue, 16 Jan 2024 08:23:48 -0800
    
    
    GitHub-issue#1994 : Implementation Of Cloudwatch metrics source plugin
    configuration Junit test cases and source coordinator.
    Signed-off-by:
    venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    Co-authored-by:
    rajeshLovesToCode &lt;131366272+rajeshLovesToCode@users.noreply.github.com&gt;
    
    Co-authored-by: rajeshLovesToCode &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds max_request_length as a configuration for the http and OTel sources (#3958)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 12 Jan 2024 09:06:58 -0800
    
    
    Adds max_request_length as a configuration for the http, otel_trace_source,
    otel_metrics_source, and otel_logs_source sources. Resolves #3931
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add map_to_list processor (#3945)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 11 Jan 2024 12:59:02 -0800
    
    
    Add map to list processor basic functionality and unit tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Bump com.github.tomakehurst:wiremock in /data-prepper-plugins/s3-source (#3777)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 11 Jan 2024 11:59:11 -0800
    
    
    Bumps [com.github.tomakehurst:wiremock](https://github.com/wiremock/wiremock)
    from 3.0.0-beta-8 to 3.0.1.
    - [Release notes](https://github.com/wiremock/wiremock/releases)
    - [Commits](https://github.com/wiremock/wiremock/compare/3.0.0-beta-8...3.0.1)
    
    ---
    updated-dependencies:
    - dependency-name: com.github.tomakehurst:wiremock
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add your public modifier back to one of the AbstractBuffer constructors to attempt to fix the build. (#3947)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Jan 2024 10:38:22 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support format expressions for routing in the opensearch sink (#3863)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 11 Jan 2024 09:15:06 -0800
    
    
    * Support format expressions for routing in the opensearch sink
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add truncate string processor (#3924)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 11 Jan 2024 09:14:43 -0800
    
    
    * Add truncate string processor
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added check for negative numbers in the config input
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkstyle error
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to make truncate processor a top level processor, not specific to
    strings
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Updated documentation with correct configuration
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed typos in the documentation
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to allow more than one source keys in the config
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to allow multiple entries under configuration
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Adds a delay processor to put a delay into the processor chain to help with debugging and testing. (#3939)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Jan 2024 08:53:57 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Corrects the bufferUsage metric by making it equal to the difference between the bufferCapacity and the available permits in the semaphore. Adds a new capacityUsed metric which tracks the actual capacity used by the semaphore which blocks. Resolves #3936. (#3937)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 10 Jan 2024 14:10:52 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Speed Up initial snapshot from MongoDB (#3675)__

    [wanghd89](mailto:wanghd89@gmail.com) - Wed, 10 Jan 2024 14:03:22 -0600
    
    
    Signed-off-by: Haidong &lt;whaidong@amazon.com&gt;
    

* __Bump org.yaml:snakeyaml in /data-prepper-plugins/kafka-plugins (#3908)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 12:26:21 -0800
    
    
    Bumps [org.yaml:snakeyaml](https://bitbucket.org/snakeyaml/snakeyaml) from 2.0
    to 2.2.
    -
    [Commits](https://bitbucket.org/snakeyaml/snakeyaml/branches/compare/snakeyaml-2.2..snakeyaml-2.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.yaml:snakeyaml
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.github.ua-parser:uap-java (#3774)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 12:12:47 -0800
    
    
    Bumps [com.github.ua-parser:uap-java](https://github.com/ua-parser/uap-java)
    from 1.5.4 to 1.6.1.
    - [Release notes](https://github.com/ua-parser/uap-java/releases)
    - [Commits](https://github.com/ua-parser/uap-java/compare/v1.5.4...v1.6.1)
    
    ---
    updated-dependencies:
    - dependency-name: com.github.ua-parser:uap-java
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-jpl in /data-prepper-core (#3879)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 12:08:54 -0800
    
    
    Bumps org.apache.logging.log4j:log4j-jpl from 2.22.0 to 2.22.1.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-jpl
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-validator:commons-validator in /data-prepper-core (#3880)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 12:08:05 -0800
    
    
    Bumps commons-validator:commons-validator from 1.7 to 1.8.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-validator:commons-validator
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.httpcomponents:httpcore (#3788)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 12:07:38 -0800
    
    
    Bumps org.apache.httpcomponents:httpcore from 4.4.15 to 4.4.16.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.httpcomponents:httpcore
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.mapdb:mapdb in /data-prepper-plugins/mapdb-processor-state (#3793)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 12:06:50 -0800
    
    
    Bumps [org.mapdb:mapdb](https://github.com/jankotek/mapdb) from 3.0.8 to
    3.0.10.
    -
    [Commits](https://github.com/jankotek/mapdb/compare/mapdb-3.0.8...mapdb-3.0.10)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.mapdb:mapdb
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-core (#3878)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 12:01:09 -0800
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.22.0 to 2.22.1.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.22.0...rel/2.22.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#3873)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 11:59:49 -0800
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.14.10 to 1.14.11.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.10...byte-buddy-1.14.11)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Fix Null Pointer Exception in KeyValue Processor (#3927)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 9 Jan 2024 09:19:18 -0800
    
    
    * Fix Null Pointer Exception in KeyValue Processor
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added a test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Support larger message sizes in Kafka Buffer (#3916)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 8 Jan 2024 16:08:15 -0800
    
    
    * Support larger message sizes in Kafka Buffer
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments and added new integration tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add 4xx aggregate metric and shard progress metric for dynamodb source (#3913)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 5 Jan 2024 11:57:06 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates Armeria to 1.26.4. This also updates io.grpc to 1.58.0 which has a slight breaking changing. This is fixed by explicitly adding io.grpc:grpc-inprocess to the build. (#3915)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 5 Jan 2024 09:33:45 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Date processor test corrections for other timezones (#3911)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 3 Jan 2024 09:58:33 -0800
    
    
    * Corrections to the date processor tests so that they run in other timezones.
    Remove an unnecessary conditional and prefer arguments() syntax.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates to Gradle 8.5 (#3910)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 3 Jan 2024 08:43:29 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for epoch timestamps and configurable output format (#3860)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 20 Dec 2023 11:58:57 -0800
    
    
    * Add support for epoch timestamps and configurable output format
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Add support for epoch timestamps and configurable output format
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Updates opensearch library to 1.3.14. And run integration test against 2.11.1 and 1.3.14 as well. Resolves #3837. (#3838)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 19 Dec 2023 14:15:58 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates commons-lang3 to 3.14.0 and commons-io to 2.15.1. Also corrects some projects to use the versions from the dependency catalog. (#3850)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 13 Dec 2023 09:11:02 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add Avro integration test to s3 source (#3852)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 13 Dec 2023 10:29:07 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add sts_header_overrides to s3 dlq configuration (#3845)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 12 Dec 2023 15:27:25 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __rebasing to latest (#3846)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 12 Dec 2023 11:45:44 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Bump org.hibernate.validator:hibernate-validator (#3765)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 12 Dec 2023 07:45:04 -0800
    
    
    Bumps
    [org.hibernate.validator:hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 8.0.0.Final to 8.0.1.Final.
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/main/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/8.0.0.Final...8.0.1.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.opencsv:opencsv in /data-prepper-plugins/csv-processor (#3750)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 12 Dec 2023 07:44:07 -0800
    
    
    Bumps com.opencsv:opencsv from 5.8 to 5.9.
    
    ---
    updated-dependencies:
    - dependency-name: com.opencsv:opencsv
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Clean-up from recent merge of PR #3103 (#3843)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 12 Dec 2023 06:51:19 -0800
    
    
    Cleaning up some unnecessary code and dependencies from the recent merge of PR
    #3103.
    Adds missing certificate and key files to fix failures from recent
    merge of PR #3103.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix for [BUG] Data Prepper is losing connections from S3 pool (#3836)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 11 Dec 2023 15:07:51 -0800
    
    
    * Fix for [BUG] Data Prepper is losing connections from S3 pool
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed CheckStyle errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Prometheus Sink draft code for issue #1744. (#3103)__

    [mallikagogoi7](mailto:mallikagogoi7@gmail.com) - Mon, 11 Dec 2023 12:27:41 -0800
    
    
    Prometheus Sink draft code for issue #1744.
    Signed-off-by: mallikagogoi7
    &lt;mallikagogoi7@gmail.com&gt;

* __Require Mozilla Rhino 1.7.12 to fix SNYK-JAVA-ORGMOZILLA-1314295. (#3839)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 11 Dec 2023 11:33:58 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds some tests to the common plugin library to increase test coverage and avoid JaCoCo coverage failures. (#3688)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 11 Dec 2023 10:19:34 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump com.bmuschko.docker-remote-api from 9.3.2 to 9.4.0 in /e2e-test (#3756)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 7 Dec 2023 09:53:59 -0800
    
    
    Bumps com.bmuschko.docker-remote-api from 9.3.2 to 9.4.0.
    
    ---
    updated-dependencies:
    - dependency-name: com.bmuschko.docker-remote-api
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Release notes for Data Prepper 2.6.1. (#3829)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 7 Dec 2023 08:59:21 -0800
    
    
    * Release notes for Data Prepper 2.6.1. From
    https://github.com/opensearch-project/data-prepper/milestone/25?closed=1.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add 2.6.1 change log (#3830)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 7 Dec 2023 10:50:09 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/rss-source (#3789)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Dec 2023 14:43:32 -0800
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.11.1
    to 2.12.5.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.11.1...v2.12.5)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-jpl in /data-prepper-core (#3742)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Dec 2023 14:42:44 -0800
    
    
    Bumps org.apache.logging.log4j:log4j-jpl from 2.21.1 to 2.22.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-jpl
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Update to Logback 1.4.14 in performance test and sample app to fix CVE-2023-6481. Resolves #3817. (#3819)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 Dec 2023 14:35:36 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/s3-sink (#3798)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Dec 2023 14:01:16 -0800
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.11.1
    to 2.12.5.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.11.1...v2.12.5)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/s3-source (#3790)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Dec 2023 14:00:25 -0800
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.11.1
    to 2.12.5.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.11.1...v2.12.5)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#3734)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Dec 2023 13:59:09 -0800
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.14.9 to 1.14.10.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.9...byte-buddy-1.14.10)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-core (#3743)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Dec 2023 13:57:11 -0800
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.21.1 to 2.22.0.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.21.1...rel/2.22.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-expression (#3738)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Dec 2023 13:56:40 -0800
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.21.1 to 2.22.0.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.21.1...rel/2.22.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.apptasticsoftware:rssreader in /data-prepper-plugins/rss-source (#3780)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Dec 2023 13:56:02 -0800
    
    
    Bumps [com.apptasticsoftware:rssreader](https://github.com/w3stling/rssreader)
    from 3.2.5 to 3.5.0.
    - [Release notes](https://github.com/w3stling/rssreader/releases)
    - [Commits](https://github.com/w3stling/rssreader/compare/v3.2.5...v3.5.0)
    
    ---
    updated-dependencies:
    - dependency-name: com.apptasticsoftware:rssreader
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add parse_ion processor (#3803)__

    [Emma](mailto:me@its-em.ma) - Wed, 6 Dec 2023 13:46:20 -0800
    
    
    Add parse_ion processor
     Signed-off-by: Emma Becar &lt;emmacb@amazon.com&gt;
    Co-authored-by: Emma Becar
    &lt;emmacb@amazon.com&gt;

* __Gradle project maintenance: Jackson 2.15.3, use the Jackson version from the BOM consistently, use the JUnit and Mockito from the version catalog consistently. (#3802)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 Dec 2023 13:27:43 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Logback 1.4.12 in performance test project to fix CVE-2023-6378 (#3746)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 Dec 2023 13:14:47 -0800
    
    
    Use Logback 1.4.12 in the performance test project to fix CVE-2023-6378.
    Resolves #3729
    Use Logback 1.4.12 in the sample analytics project to fix
    CVE-2023-6378. Resolves #3729
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix kafka buffer metrics (#3805)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 5 Dec 2023 16:54:55 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __FIX: deal with event with object size zero (#3806)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 5 Dec 2023 13:09:44 -0600
    
    
    * FIX: deal with event with object size zero
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fix bug where update/upsert bulk action did not filter exclude/include keys, document_root_key, etc (#3747)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 1 Dec 2023 14:19:58 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add missing projects to dependabot (#3717)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 1 Dec 2023 12:01:36 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump com.fasterxml.jackson.datatype:jackson-datatype-jsr310 (#3733)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 Dec 2023 11:57:08 -0800
    
    
    Bumps com.fasterxml.jackson.datatype:jackson-datatype-jsr310 from 2.15.2 to
    2.16.0.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jsr310
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates the example analytics-service to Spring Boot 3.1.6 which fixes CVE-2023-34055. (#3721)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 30 Nov 2023 14:35:27 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Require nimbus-jose-jwt 9.37.1 which fixes CVE-2021-31684 and CVE-2023-1370 by using a newer shaded version of json-smart. (#3720)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 30 Nov 2023 14:35:15 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add aggregate metrics for ddb source export and stream (#3724)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 30 Nov 2023 10:02:48 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Support append mode to file sink (#3722)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 29 Nov 2023 13:07:41 -0800
    
    
    * Support append mode to file sink
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated documentation in README
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Adds a Checkstyle validation to ensure that the Guava cache is not used. (#3631)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 29 Nov 2023 10:17:07 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates data-prepper-api fully to JUnit 5 and removes JUnit vintage from the Gradle project. (#3627)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 29 Nov 2023 10:12:55 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Pin versions for jaeger-hotrod docker-compose (#3709)__

    [Shirley](mailto:4163034+fridgepoet@users.noreply.github.com) - Wed, 29 Nov 2023 09:32:49 -0800
    
    
    * Pin versions for jaeger-hotrod docker-compose
     Signed-off-by: Shirley Fridgepoet
    &lt;4163034+fridgepoet@users.noreply.github.com&gt;
    
    * Use opensearch 2.9.0 and data-prepper 2
     Signed-off-by: Shirley Fridgepoet
    &lt;4163034+fridgepoet@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Shirley Fridgepoet
    &lt;4163034+fridgepoet@users.noreply.github.com&gt;

* __MAINT: wild card (#3719)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 29 Nov 2023 07:56:01 -0800
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add 2.6.0 change log (#3714)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 28 Nov 2023 17:09:06 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix flaky sqs it tests (#3696)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 28 Nov 2023 14:57:12 -0600
    
    
    * Fix S3 SQS flaky tests
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __MAINT: GitHub action for e2e secrets (#3704)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 28 Nov 2023 11:06:47 -0600
    
    efs/remotes/origin/gha-runner-experiments
    * ADD: initial AWS testing resources CDK
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MNT: renaming
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * E2E: basicLogWithAwsSecretsEndToEndTest
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: refactoring on build.gradle
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: use AWS_PROFILE
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: check aws credentials
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * maint: check absolute path of .aws
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: check output
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: check output
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: check step output
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: aws configure
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: set files
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MNT: format
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: check files
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * testing
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: run actual tests
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * TST: assume new role
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: add test
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: title
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: back test on branch
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: revert
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: job name
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: file name
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: us-east-2
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: manual workflow trigger
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: add back variable
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: use repo variables
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: target change
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * TST: any push
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: revert change
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    ---------
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Release notes for Data Prepper 2.6.0 (#3710)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 28 Nov 2023 08:58:20 -0800
    
    
    Adds the release notes for Data Prepper 2.6.0.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the next Data Prepper version to 2.7.0. (#3708)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 28 Nov 2023 08:51:47 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bug so GLOBAL read-only items do not expire from TTL in ddb source coordination store (#3703)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 28 Nov 2023 08:17:00 -0800
    
    
    Fix bug so GLOBAL read-only items do not expire from TTL in ddb source
    coordination store
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Gradle parallel max (#3700)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Nov 2023 13:21:51 -0800
    
    
    Set the maximum workers to 2 when running the GHA build and release tasks.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Generated THIRD-PARTY file for c88c27f (#3698)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 27 Nov 2023 13:10:27 -0800
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Check if failedDeleteCount is positive before logging (#3686)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 27 Nov 2023 14:44:00 -0600
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Address comments from PR 3625 (#3633)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 27 Nov 2023 12:19:23 -0800
    
    
    * Fix crash in Kafka consumer when negative acknowledments are received
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Rebased to latest
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed unrelated changes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Require Apache Avro 1.11.3 to fix CVE-2023-39410. Resolves #3430. (#3695)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Nov 2023 10:58:50 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates werkzeug to 3.0.1 which fixes CVE-2023-46136. This required updating to dash 2.14.1 as 2.13 does not support newer versions of werkzeug. Resolves #3552. (#3690)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Nov 2023 08:52:29 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix S3 scan failing tests (#3693)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 27 Nov 2023 08:49:02 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updates the opensearch-java client to 2.8.1 and opensearch to 1.3.13. This includes a transitive dependency update to parsson to resolve CVE-2023-4043. (#3689)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Nov 2023 08:45:36 -0800
    
    
    Update required version of org.json library to resolve CVE-2023-5072. Require a
    Zookeeper version which resolves CVE-2023-44981. Require a transitive Scala
    library to resolve CVE-2023-46122.
     Resolves #3588, #3522, #3491, #3547
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __TST: validate special data in opensearch sink (#3685)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 27 Nov 2023 10:02:33 -0600
    
    
    * TST: validate special data in opensearch sink
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fix crash in Kafka consumer when negative acknowledments are received (#3691)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 27 Nov 2023 08:00:42 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Select require_alias for OS bulk inserts from ISM Policy (#3560)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Tue, 21 Nov 2023 14:48:05 -0800
    
    
    * Select require_alias for OS bulk inserts from ISM Policy
     This change requires an alias when writing to an aliased
    index. This avoids
    creation of an index without alias, when
    a previous existing alias and index
    was deleted. It increases
    robustness of DataPrepper&#39;s trace index against OS
    user
    interactions.
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * 3342 Determine Alias Configuration from OS
     During OS sink initialization it is determined from OS, whether the
    
    configured index actually is an alias. If so, bulk request will require
    the
    index to always be an alias. The response is cached to avoid
    further requests.
    This also ensures, that the alias configuration is
    kept in the initially
    intended state. After all, this change is about to
    prevent an automatic index
    creation for a formerly existing alias.
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix imports for checkstyle
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix integration tests
     The specific user used in some tests of OpenSerachSinkIT
    needs get
    permissions on all aliases to test for their existence.
    Another bug with
    determining the alias name is fixed as well.
     As a final result, the DataPrepper OpenSearch user requires
    write access to
    the indices and now additionally read access to
    the aliases. This can be a
    change for self-managed indices.
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix Bulk Requests for older OD versions
     The `require_alias` parameter for bulk requests was only introduced
    with ES
    7.10. Since DataPrepper needs to be compatible down to 6.8,
    the parameter
    should not be used in earlier OD versions. This change
    will apply the
    parameter only when OpenSearch is detected as target.
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add Permission to get Cluster Info
     For checking the OS version, the test user needs an
    additional permission.
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    ---------
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Use Amazon Linux 2023 as the base image for the Data Prepper Docker image. This install Temurin for the Amazon Linux 2 distribution. Resolves #3505. (#3671)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Nov 2023 12:34:51 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __E2E: aws secrets tests (#3654)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 17 Nov 2023 10:34:09 -0600
    
    
    * E2E: basicLogWithAwsSecretsEndToEndTest
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __ShardId is not passed to ShardConsumer, resulting in logs saying shard is null on shutdown (#3683)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 16 Nov 2023 17:32:55 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Improve logging message for no shards found to indicate that export m… (#3681)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 16 Nov 2023 16:59:00 -0600
    
    
    Improve logging message for no shards found to indicate that export may still
    be ongoing
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add ddb source fixes/improvements (#3676)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 16 Nov 2023 10:52:55 -0600
    
    
    Add ddb source fixes/improvements
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix an NPE when the DynamoDB LeaderScheduler does not receive a leader partition. To help test this, I also allowed for a smaller lease interval in a package protected constructor. (#3672)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Nov 2023 06:37:47 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Improves the DefaultPluginFactory class design by creating a new class for providing arguments from the Application Context to plugin parameters. (#3615)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 15 Nov 2023 11:02:06 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Uses export time minus 5 minutes for export document version (#3668)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 15 Nov 2023 09:06:28 -0800
    
    
    Uses export time minus 5 minutes for export document version
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __ADD: initial AWS testing resources CDK (#3501)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 15 Nov 2023 10:46:14 -0600
    
    
    * ADD: initial AWS testing resources CDK
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Correct single quote escape character in DynamoDB [#3664] (#3667)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 15 Nov 2023 08:02:31 -0800
    
    
    Resolves a bug with escaped single quotes in the DynamoDB source by updating
    the AWS SDK to 2.21.23. Also, skip data that cannot be parsed entirely rather
    than silently send empty data. Resolves #3664.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix leader partition time out issue due to exception (#3666)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Wed, 15 Nov 2023 09:42:09 -0600
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Update gRPC and HTTP logging (#3658)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 14 Nov 2023 17:26:11 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Undo rename change done in PR 3656 (#3661)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 14 Nov 2023 17:10:33 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Do not send empty lists to the DLQ when all items share the same retryable failure. Resolves #3644 (#3660)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Nov 2023 16:58:55 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds integration tests to KafkaBufferIT to verify that raw bytes are wrapped in the Protobuf Kafka message wrapper. Adds a missing validation when reading data after a writeBytes call and some other minor test improvements. (#3645)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Nov 2023 16:24:23 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Catch failure to change visibility timeout and maintain a counter (#3657)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 14 Nov 2023 16:22:43 -0800
    
    
    Catch failure to change visibility timeout and maintain a counter
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Adds a configuration to the random string source to configure the wait delay between writes to the buffer. Resolves #3595. Also uses a single thread for this source to avoid an unnecessary thread pool and increases the code coverage. (#3602)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Nov 2023 14:53:29 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Change latency metric names (#3656)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 14 Nov 2023 14:05:31 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Go back to processing only 1 data node file at a time instead of 3 (#3652)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 14 Nov 2023 11:04:47 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Convert Number types to BigDecimal plainString for consistency between partition and sort keys for export and streams (#3650)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 14 Nov 2023 09:51:39 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix no acknowledgments for closed shard issue (#3651)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Tue, 14 Nov 2023 09:51:10 -0600
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Include the full exception and stack trace when an unexpected error occurs in the LeaderScheduler. (#3648)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Nov 2023 16:48:11 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __MAINT: add bytes metrics into opensearch source (#3646)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 13 Nov 2023 16:54:23 -0600
    
    
    * MAINT: add bytes metrics
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: add bytes metrics into dynamo source (#3647)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 13 Nov 2023 16:54:06 -0600
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fix initialization issue in DynamoDB source (#3643)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Mon, 13 Nov 2023 11:29:23 -0800
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Use a Protobuf buffer message for data in the Kafka buffer (#3635)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Nov 2023 11:12:32 -0800
    
    
    Adds a Protobuf buffer message for the Kafka buffer. Data sent to the topic is
    wrapped in this and then parsed back into this. Contributes toward #3620.
     Correct the Kafka buffer tests to test correctly as bytes, adds bytes tests,
    fixes some serialization issues with the Kafka buffer.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Refactor to use Admin client instead of second set of consumers for empty check (#3637)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Sun, 12 Nov 2023 12:20:03 -0600
    
    
    * Refactor to use Admin client instead of second set of consumers for empty
    check
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove debug log
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Pass custom metric prefix if present to AbstractBuffer when using KafkaBuffer (#3638)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Sun, 12 Nov 2023 11:31:16 -0600
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Improvements to prevent data loss in DynamoDB source (#3614)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Sun, 12 Nov 2023 11:20:58 -0600
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Remove verbose debug log from JacksonEvent (#3639)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Sun, 12 Nov 2023 11:14:04 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add distribution_version flag to opensearch source (#3636)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Sun, 12 Nov 2023 10:42:27 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add normalize_index flag to normalize invalid dynamic indices (#3634)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Sat, 11 Nov 2023 15:54:29 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bump com.fasterxml.jackson.datatype:jackson-datatype-jdk8 (#3570)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Nov 2023 16:43:31 -0800
    
    
    Bumps com.fasterxml.jackson.datatype:jackson-datatype-jdk8 from 2.15.2 to
    2.15.3.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jdk8
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#3571)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Nov 2023 16:42:56 -0800
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.14.8 to 1.14.9.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.8...byte-buddy-1.14.9)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Create network policy for aoss source. (#3613)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Fri, 10 Nov 2023 14:15:49 -0800
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Add support OTEL traces and logs with Kafka buffer (#3625)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 10 Nov 2023 14:13:26 -0800
    
    
    * Add support OTEL traces and logs with Kafka buffer
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed binary files
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Rebased and merged with latest changes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Retry on dynamic index creation when an OpenSearchException is thrown (#3541)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 10 Nov 2023 15:53:30 -0600
    
    
    * Retry on dynamic index creation when an OpenSearchException is thrown
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Use correct exception type in unit tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove older cache imports
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    Signed-off-by: Chase
    &lt;62891993+engechas@users.noreply.github.com&gt;

* __Start unit tests for the OpenSearch sink testing the document_version error cases (#3599)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 10 Nov 2023 13:27:29 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Attempting to fix the flaky tests in AwsCloudMapPeerListProviderTest. This uses a higher wait for changes, and refreshes at sub-second intervals for testing. (#3628)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 10 Nov 2023 11:15:41 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add metric for shards actively being processed, lower ownership timeo… (#3629)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 10 Nov 2023 12:56:00 -0600
    
    
    Add metric for shards actively being processed, lower ownership timeout from 10
    minutes to 5 minutes for ddb source
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updated the Router_ThreeRoutesIT test with a longer timeout. Also, use Awaitility&#39;s during() method to verify that certain data never reaches a sink that it never should reach. (#3624)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 10 Nov 2023 09:57:00 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Disable the circuit breaker for buffers that write data off-heap only… (#3619)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 10 Nov 2023 08:45:05 -0800
    
    
    Disable the circuit breaker for buffers that write data off-heap only. This is
    currently only the Kafka buffer. Resolves #3616
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add splitExportTraceServiceRequest API to OTelProtoDecoder (#3600)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 9 Nov 2023 21:09:45 -0800
    
    
    * Add splitExportTraceServiceRequest API to OTelProtoDecoder
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Renamed the API
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed code and modified test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed check style test
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add support for OTEL metrics source to use Kafka buffer (#3539)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 9 Nov 2023 15:32:41 -0800
    
    
    * Add support for OTEL metrics source to use Kafka buffer
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added tests and fixed test failures
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add Java 11/17/21 matrix for build, test and performance test checks (#3622)__

    [Andriy Redko](mailto:drreta@gmail.com) - Thu, 9 Nov 2023 15:21:03 -0800
    
    
    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

* __Fix race condition in DefaultEventHandle (#3618)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 9 Nov 2023 10:09:20 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add Kafka Producer metrics for send record failures (#3611)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 8 Nov 2023 12:46:35 -0800
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix issue where we start from checkpoint for PIT with acks to instead start from beginning (#3610)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 8 Nov 2023 12:08:42 -0800
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Pause reading from Kafka in the Kafka buffer when the circuit breaker is open (#3595)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 8 Nov 2023 11:21:20 -0800
    
    
    Use the CircuitBreaker in the Kafka buffer to stop reading data from the Kafka
    topic and putting it into the in-memory buffer. Moves the CircuitBreaker class
    into data-prepper-api. Adds a DelegatingBuffer class to data-prepper-api.
    Resolves #3578.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Move opensearch source into same module as opensearch sink. (#3607)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Wed, 8 Nov 2023 10:07:27 -0800
    
    
    This change is required to share code between the source and sink plugins.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Updated visibility protection timeout (#3608)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 8 Nov 2023 09:40:42 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Check topic for emptiness during KafkaBuffer shutdown (#3545)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 8 Nov 2023 11:13:45 -0600
    
    
    * Add shutdown method to buffer API
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Only allow single thread to check emptiness
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix logic and add 1 minute wait before requerying kafka
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests for thread safety logic
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Refactor metadata related to emptiness into own class
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Single instance per topic rather than per worker
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests for topic emptiness class
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Resolve rebase conflicts
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Some changes to the configurations for the new visibility timeout extension feature. Increase the maximum default to 2 hours, increase the maximum configurable extension to 24 hours, and rename to use the work &#34;maximum&#34; to remain consistent (e.g. maximum_messages). (#3604)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 8 Nov 2023 08:45:39 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Change Latency Metric names (#3603)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 8 Nov 2023 09:51:55 -0600
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Support ISM correctly when using composable index templates (#3590)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 8 Nov 2023 06:38:49 -0800
    
    
    Correctly support custom properties in composable index templates in the
    OpenSearch sink. This resolves #3506.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add Kafka Producer Metrics and Kafka Buffer Metrics (#3598)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 7 Nov 2023 22:45:19 -0600
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add dynamodb_item_version metadata that is derived from timestamp for… (#3596)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 7 Nov 2023 19:16:16 -0600
    
    
    Add dynamodb_item_version metadata that is derived from timestamp for stream
    events
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add document_version and document_version_type parameters to the open… (#3591)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 7 Nov 2023 18:38:46 -0600
    
    
    Add document_version and document_version_type parameters to the opensearch
    sink for conditional indexing of documents
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Optimize idle item between GetRecords call (#3593)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Tue, 7 Nov 2023 17:55:18 -0600
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Add CMK encryption support to DynamoDB export (#3592)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Tue, 7 Nov 2023 16:26:24 -0600
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Javadoc fixes (#3594)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 7 Nov 2023 12:18:43 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add Internal and external latency to OpenSearch and S3 sinks.  (#3583)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 7 Nov 2023 09:42:58 -0800
    
    
    Add Internal and external latency to OpenSearch and S3 sinks
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Enable acknowledgements for KafkaBuffer plugin (#3584)__

    [Hardeep Singh](mailto:mzhrde@amazon.com) - Mon, 6 Nov 2023 09:57:04 -0800
    
    
    Signed-off-by: Hardeep &lt;mzhrde@amazon.com&gt;

* __Add dynamodb_event_name metadata attribute, change mapping for Ddb INSERT and MODIFY to be index bulk action (#3585)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Sun, 5 Nov 2023 10:49:05 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Change s3 scan and opensearch to only save state every 5 minutes, fix… (#3581)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Sun, 5 Nov 2023 10:31:01 -0600
    
    
    Change s3 scan and opensearch to only save state every 5 minutes, fix bug where
    any action was valid in OpenSearch sink
    Signed-off-by: Taylor Gray
    &lt;tylgry@amazon.com&gt;

* __Replace Guava Cache with Caffeine for OpenSearch integration (#3586)__

    [Roman Kvasnytskyi](mailto:roman@kvasnytskyi.net) - Sat, 4 Nov 2023 13:24:12 -0700
    
    
    Signed-off-by: Roman Kvasnytskyi &lt;roman@kvasnytskyi.net&gt;

* __Add ProgressCheck callbacks to end-to-end acknowledgements (#3565)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Sat, 4 Nov 2023 08:04:22 -0700
    
    
    Add ProgressCheck callbacks to end-to-end acknowledgements
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Update end-to-end tests to use the release Docker image or a custom image with a specific Java version. Resolves #3566 (#3576)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 3 Nov 2023 11:26:53 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add acknowledgments for the ddb source (#3575)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 2 Nov 2023 11:14:41 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Modify EventHandle to be created for every event and support internal and external origination times (#3546)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 1 Nov 2023 13:52:58 -0700
    
    
    * Modify EventHandle to be created for every event and support internal and
    external origination times
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed build failures
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed build failures
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * fixed failing checkstyle error
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed build errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments by adding InternalEventHandle
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkstyle errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed build errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-expression (#3569)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Nov 2023 11:46:48 -0700
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.20.0 to 2.21.1.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.20.0...rel/2.21.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-jpl in /data-prepper-core (#3574)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Nov 2023 10:02:27 -0700
    
    
    Bumps org.apache.logging.log4j:log4j-jpl from 2.20.0 to 2.21.1.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-jpl
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-core (#3573)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Nov 2023 10:01:53 -0700
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.20.0 to 2.21.1.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.20.0...rel/2.21.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __OpenSearch Sink: Add log messages when there is no exception (#3532)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 31 Oct 2023 13:23:55 -0700
    
    
    Add log messages when there is exception
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Add bug fixes and improvements to DDB source (#3559)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Tue, 31 Oct 2023 14:48:32 -0500
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Updates Kafka configurations such that plugin has its own topic configurations (#3551)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 31 Oct 2023 10:20:09 -0700
    
    
    Updates Kafka buffer configurations to disallow configurations which are not
    needed - e.g. schema and the serde_format for the topic configuration. As part
    of this change, I also split the TopicConfig into three distinct interfaces and
    classes. This allows each plugin to either accept a configuration or provide a
    value of the plugin&#39;s own choosing. Also adds copyright headers to all files
    modified as part of this commit.
     Renamed is_topic_create to create_topic. Also made this a boolean internally
    instead of Boolean since it will have a value.
     Adds a zeroBytes() static method to ByteCount as a convenience.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix for Kafka buffer encryption with bytes serde_format by returning null for null input in EncryptionSerializer. (#3556)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 27 Oct 2023 14:39:02 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Catch exceptions and backoff and retry ddb source threads instead of shutting down on exception (#3554)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 27 Oct 2023 10:37:39 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix CVE error for Jetty version 11.0.12 (#3548)__

    [wanghd89](mailto:wanghd89@gmail.com) - Thu, 26 Oct 2023 13:12:39 -0500
    
    
    * Fix CVE error for Jetty version 11.0.12
     Signed-off-by: Haidong &lt;whaidong@amazon.com&gt;
    
    ---------
     Signed-off-by: Haidong &lt;whaidong@amazon.com&gt;
    Co-authored-by: Haidong
    &lt;whaidong@amazon.com&gt;

* __Implement writeBytes and isByteBuffer in the CircuitBreakingBuffer. Also update the Buffer to throw UnsupportedOperationException when writeBytes is called. (#3553)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 26 Oct 2023 09:50:44 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add tests to InMemoryBufferTest and LocalFileBufferTest (#3550)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 25 Oct 2023 14:34:18 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add obfuscate_when parameter and tags_on_match failure to obfuscate processor (#3544)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 25 Oct 2023 07:52:13 -0700
    
    
    Add obfuscate_when parameter to obfuscate processor
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __HttpSink plugin functionality for #874. (#3065)__

    [mallikagogoi7](mailto:mallikagogoi7@gmail.com) - Tue, 24 Oct 2023 17:10:29 -0700
    
    
    HttpSink plugin functionality for #874.
    Signed-off-by: mallikagogoi7
    &lt;mallikagogoi7@gmail.com&gt;

* __Add bug fixes and improvements to DDB source (#3534)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Mon, 23 Oct 2023 18:08:21 -0500
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Removed the deprecated annotation from Record and RecordMetadata as these are currently still very necessary. Resolves #3536. (#3540)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Oct 2023 14:52:26 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Test against newer versions of OpenSearch and use odd versions in the 2.x series to avoid testing against too many different versions. Updated to the latest 1.3 version. (#3512)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Oct 2023 14:51:48 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Perform a full upgrade on the base Docker image when building the Data Prepper Docker image to get latest patches. (#3497)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Oct 2023 14:51:22 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Run the end-to-end tests on Java 21 in the GitHub Actions. (#3523)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Oct 2023 14:50:56 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump @babel/traverse in /release/staging-resources-cdk (#3521)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 23 Oct 2023 12:13:44 -0700
    
    
    Bumps
    [@babel/traverse](https://github.com/babel/babel/tree/HEAD/packages/babel-traverse)
    from 7.22.5 to 7.23.2.
    - [Release notes](https://github.com/babel/babel/releases)
    - [Changelog](https://github.com/babel/babel/blob/main/CHANGELOG.md)
    -
    [Commits](https://github.com/babel/babel/commits/v7.23.2/packages/babel-traverse)
    
    
    ---
    updated-dependencies:
    - dependency-name: &#34;@babel/traverse&#34;
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add logic to create or update serverless network policy. (#3510)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Mon, 23 Oct 2023 12:12:59 -0700
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Bump net.bytebuddy:byte-buddy-agent in /data-prepper-plugins/opensearch (#3527)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 23 Oct 2023 06:14:52 -0700
    
    
    Bumps [net.bytebuddy:byte-buddy-agent](https://github.com/raphw/byte-buddy)
    from 1.14.8 to 1.14.9.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.8...byte-buddy-1.14.9)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Adding support for storing raw bytes in Kafka Buffer (#3519)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 20 Oct 2023 16:34:54 -0700
    
    
    * Adding support for storing raw bytes in Kafka Buffer
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to wait for the send() to finish before returning
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed unused imports
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed Kafka integration test
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed json processor check style errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments and added a new test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments and added a new tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Reorder formatString expression check for JacksonEvent (#3533)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 20 Oct 2023 15:50:17 -0500
    
    
    Reorder formatString expression check for JacksonEvent
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix broken DefaultKafkaClusterConfigSupplier get API (#3529)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 18 Oct 2023 21:04:08 -0500
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#3413)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 18 Oct 2023 13:49:34 -0700
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.14.7 to 1.14.8.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.7...byte-buddy-1.14.8)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Rebased to latest (#3476)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 18 Oct 2023 13:08:45 -0700
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Adds new AWS configurations for the KMS encryption. Resolves #3516. (#3517)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 18 Oct 2023 08:26:20 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump urllib3 in /examples/trace-analytics-sample-app/sample-app (#3518)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 18 Oct 2023 06:17:48 -0700
    
    
    Bumps [urllib3](https://github.com/urllib3/urllib3) from 2.0.6 to 2.0.7.
    - [Release notes](https://github.com/urllib3/urllib3/releases)
    - [Changelog](https://github.com/urllib3/urllib3/blob/main/CHANGES.rst)
    - [Commits](https://github.com/urllib3/urllib3/compare/2.0.6...2.0.7)
    
    ---
    updated-dependencies:
    - dependency-name: urllib3
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump urllib3 in /release/smoke-tests/otel-span-exporter (#3520)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 18 Oct 2023 06:17:13 -0700
    
    
    Bumps [urllib3](https://github.com/urllib3/urllib3) from 1.26.17 to 1.26.18.
    - [Release notes](https://github.com/urllib3/urllib3/releases)
    - [Changelog](https://github.com/urllib3/urllib3/blob/main/CHANGES.rst)
    - [Commits](https://github.com/urllib3/urllib3/compare/1.26.17...1.26.18)
    
    ---
    updated-dependencies:
    - dependency-name: urllib3
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Gradle 8.4 (#3492)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 18 Oct 2023 05:48:26 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Rename/add metrics for ddb source (#3498)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 16 Oct 2023 14:47:20 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add more verbose logging for the DynamoDb source (#3500)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Mon, 16 Oct 2023 10:41:26 -0500
    
    
    Add more verbose logging to the DynamoDB source
     Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Miscellaneous fixes/improvements to the DynamoDb source (#3489)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 13 Oct 2023 09:50:12 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Adds KMS encryption_context for KMS encryption in the Kafka buffer. Moves the kms_key_id into a new kms section along with encryption_context. Resolves #3484 (#3486)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 12 Oct 2023 11:06:49 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Data Prepper 2.5.0 change log (#3488)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 12 Oct 2023 10:22:07 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Integrate CDC data from MySQL/Postgres/MongoDb data source (#3313)__

    [wanghd89](mailto:wanghd89@gmail.com) - Wed, 11 Oct 2023 22:10:36 -0500
    
    
    Signed-off-by: Haidong &lt;whaidong@amazon.com&gt;

* __Move ddb source coordinator config to the data-prepper-config.yaml (#3466)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 11 Oct 2023 14:40:16 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix e2e acks test (#3471)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 11 Oct 2023 12:21:47 -0700
    
    
    * Disable flaky e2e acks test
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Disabled another flaky test
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added debug statements to debug the failing tests
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified to assign unique names to pipelines
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Trying with enabling the disabled test
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing checkstyle error
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Reduced sleep time in InMemorySource
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified to use log4j
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __remove WIP plugins from build (#3480)__

    [Jonah Calvo](mailto:caljonah@amazon.com) - Wed, 11 Oct 2023 13:41:37 -0500
    
    
    Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;

* __Some updates to the 2.5.0 release notes. (#3479)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 11 Oct 2023 08:30:39 -0700
    
    
    Some updates to the 2.5.0 release notes.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix #3459 Unit tests fail on Windows machine (#3461)__

    [GongYi](mailto:topikachu@163.com) - Tue, 10 Oct 2023 16:38:59 -0700
    
    
    1. SinkModelTest: Use system System.lineSeparator() instead of hardcode &#39;\n&#39;
    
    2. DataPrepperArgsTest: Covert file path separators to local system.
    3.
    DateProcessorTests: Covert time to same timezone before comparing.
    4.
    InMemorySourceCoordinationStoreTest: Use greaterThanOrEqualTo to compare time
    since they may be same.
    5. QueuedPartitionsItemTest: Use sleep to get two
    different time instances.
    6. RSSSourceTest: Use mocker server to avoid
    internet connecting.
    7. ParquetOutputCodecTest: Close all outputStream objects
    in the tests.
    8.
    org.opensearch.dataprepper.plugins.sink.s3.accumulator.InMemoryBufferTest#getDuration_provides_duration_within_expected_range:
    No solution to fix. Disable it. Please see my comments in the test file.
     Signed-off-by: Gong Yi &lt;topikachu@163.com&gt;

* __Resolve Netty to 4.1.100.Final, require Jetty 11.0.17 in Data Prepper. Use Tomcat 10.1.14 in the example project. These changes fix CVE-2023-44487 to protect against HTTP/2 reset floods. Resolves #3474. (#3475)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 10 Oct 2023 16:33:25 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Shutdown kafka buffer (#3464)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Tue, 10 Oct 2023 15:49:46 -0500
    
    
    * Add shutdown method to buffer API
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove POC code
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Revert acknowledgments default
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add test for coverage
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove unused import
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Encrypted and decrypt data in the Kafka buffer (#3468)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 10 Oct 2023 13:18:03 -0700
    
    
    Encrypt and decrypt data in the Kafka buffer when the user configures. Use a
    KMS key to decrypt the data encryption key, if one is provided. Resolves #3422
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix broken build and clean up KafkaSource class. (#3469)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 10 Oct 2023 07:56:36 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix MSK integration test fix (#3465)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 9 Oct 2023 15:01:35 -0700
    
    
    Signed-off-by: Kondaka &lt;krishkdk@bcd07441e083.ant.amazon.com&gt;
    Co-authored-by:
    Kondaka &lt;krishkdk@bcd07441e083.ant.amazon.com&gt;

* __Refactors the Kafka buffer (and Kafka sink) code related to defining the serialization and deserialization classes. This migrates from using Kafka properties to providing concrete instances into the Kafka consumer/producer. We will make use of this when encrypting or decrypting data. (#3463)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 9 Oct 2023 14:04:23 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Kafka drain timeout (#3454)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 9 Oct 2023 13:40:02 -0500
    
    
    * Add getDrainTimeout method to buffer interface. Add as configurable value for
    kafka buffer
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Move getDrainTimeout to default method in the interface, add test for it,
    disable SNS sink
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove verification from non-mock
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __ENH: support index template for serverless (#3071)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 9 Oct 2023 09:02:19 -0700
    
    
    * ENH: support index template for serverless
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __ENH: support index template for serverless (#3071)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 9 Oct 2023 09:01:47 -0700
    
    
    * ENH: support index template for serverless
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __ENH: opensearch source secrets refreshment suppport (#3437)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 9 Oct 2023 08:19:50 -0700
    
    
    ENH: opensearch source secrets refreshment suppport (#3437)
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add release-notes for 2.5.0 (#3449)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 6 Oct 2023 14:13:01 -0700
    
    
    * Add release-notes for 2.5.0
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Add AWS secrets
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added missing items
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Addressed feedback
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix CVE-2023-39410 (#3450)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sat, 7 Oct 2023 00:17:50 +0530
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fixes some issues with the Kafka buffer and sink. Adds an integration test for the Kafka buffer and run that in the GitHub Actions. (#3451)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 6 Oct 2023 10:55:51 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Awaitility in the KafkaSourceJsonTypeIT to avoid sleeps. Also consolidates logic for creating and deleting topics. (#3447)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 6 Oct 2023 09:20:37 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add inline template_content support to the opensearch sink (#3431)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 6 Oct 2023 11:17:22 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Run the Kafka integration tests as a GitHub Action. Created Docker compose files for starting Kafka easily and updated the README.md instructions. (#3445)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 6 Oct 2023 09:08:41 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes the GeoIP processor from the build. It doesn&#39;t work and the tests are failing consistently now. (#3448)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 6 Oct 2023 08:19:54 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for Update/Upsert/Delete operations in OpenSearch Sink (#3424)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 5 Oct 2023 17:54:37 -0700
    
    
    * Add support for Update/Upsert/Delete operations in OpenSearch Sink
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed tests and removed unused imports
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added test cases to improve code coverage
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed check style errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added another test for upsert action without prior create action
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added check for valid action strings at config time
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Refactor Kafka Source and Sink, implement basic Kafka buffer (#3354)__

    [Jonah Calvo](mailto:caljonah@amazon.com) - Thu, 5 Oct 2023 17:30:58 -0700
    
    
    * Refactor Kafka Source and Sink to make Kafka producer and consumer logic more
    reusable
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Implement requested changes + simple kafka buffer
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Cleaning up logs, add TODOs, etc.
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Add support for MSK in kafka buffer
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Change Topics to list for now
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * update config yaml names
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Fix unit tests
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    ---------
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;

* __Remove support for Enum and Duration values from secrets manager (#3433)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 5 Oct 2023 22:35:58 +0530
    
    
    * Remove support for Enum and Duration values from secrets manager
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added unit tests
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump org.xerial.snappy:snappy-java in /data-prepper-plugins/common (#3411)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 5 Oct 2023 09:50:41 -0700
    
    
    Bumps [org.xerial.snappy:snappy-java](https://github.com/xerial/snappy-java)
    from 1.1.10.3 to 1.1.10.5.
    - [Release notes](https://github.com/xerial/snappy-java/releases)
    -
    [Commits](https://github.com/xerial/snappy-java/compare/v1.1.10.3...v1.1.10.5)
    
    ---
    updated-dependencies:
    - dependency-name: org.xerial.snappy:snappy-java
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump urllib3 in /examples/trace-analytics-sample-app/sample-app (#3425)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 5 Oct 2023 09:48:34 -0700
    
    
    Bumps [urllib3](https://github.com/urllib3/urllib3) from 2.0.4 to 2.0.6.
    - [Release notes](https://github.com/urllib3/urllib3/releases)
    - [Changelog](https://github.com/urllib3/urllib3/blob/main/CHANGES.rst)
    - [Commits](https://github.com/urllib3/urllib3/compare/2.0.4...2.0.6)
    
    ---
    updated-dependencies:
    - dependency-name: urllib3
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __ENH: data-prepper-core support for secrets refreshment (#3415)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 5 Oct 2023 09:46:44 -0700
    
    
    * INIT: secrets refreshment infra
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: add interval and test validity
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: some more refactoring
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: delete unused classes
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * TST: AwsSecretsPluginConfigPublisherExtensionProviderTest
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: inject PluginConfigPublisher into PluginCreator
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: complete test cases for AwsSecretPluginIT
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: test refresh secrets
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: refactoring and documentation
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * STY: import
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: fix test cases
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: missing test case
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * MAINT: address minor comments
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    * REF: PluginConfigurationObservableRegister
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    ---------
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Set main version to 2.6 (#3439)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 5 Oct 2023 08:58:35 -0700
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump urllib3 in /release/smoke-tests/otel-span-exporter (#3427)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 3 Oct 2023 09:55:27 -0700
    
    
    Bumps [urllib3](https://github.com/urllib3/urllib3) from 1.26.7 to 1.26.17.
    - [Release notes](https://github.com/urllib3/urllib3/releases)
    - [Changelog](https://github.com/urllib3/urllib3/blob/main/CHANGES.rst)
    - [Commits](https://github.com/urllib3/urllib3/compare/1.26.7...1.26.17)
    
    ---
    updated-dependencies:
    - dependency-name: urllib3
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __-download task support for geoip (#3373)__

    [rajeshLovesToCode](mailto:131366272+rajeshLovesToCode@users.noreply.github.com) - Tue, 3 Oct 2023 09:39:32 -0700
    
    
    * -download task support for geoip
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -download task support for geoip
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -download task support for geoip
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -fix for geoip IP constant
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;

* __Add DynamoDB source plugin (#3349)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Tue, 3 Oct 2023 11:31:22 -0500
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Update release notes and change log for 2.4.1 (#3416)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 2 Oct 2023 09:58:20 -0700
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix CVE-2022-45688, CVE-2023-43642 (#3404)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 29 Sep 2023 19:14:59 +0530
    
    
    * Fix CVE-2022-45688
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Fix CVE-2023-43642
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated release notes file name (#3403)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 29 Sep 2023 16:20:20 +0530
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add retry to Kafka Consumer Create in source (#3399)__

    [Jonah Calvo](mailto:caljonah@amazon.com) - Fri, 29 Sep 2023 16:19:57 +0530
    
    
    Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;

* __2.4.1 release notes (#3398)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 29 Sep 2023 00:08:21 +0530
    
    
    * 2.4.1 release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __2.4.1 change log (#3397)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 29 Sep 2023 00:08:02 +0530
    
    
    * 2.4.1 change log
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated change log
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Improve logging for failed documents in the OpenSearch sink (#3387)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 27 Sep 2023 19:38:31 +0530
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add support for fully async acknowledgments in source coordination (#3384)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 26 Sep 2023 15:34:54 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add _id as additional sort key for point-in-time and search_after (#3374)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 26 Sep 2023 13:24:20 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __CVE fixes (#3385)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 26 Sep 2023 23:38:58 +0530
    
    
    * CVE fixes
    CVE-2022-36944, WS-2023-0116, CVE-2021-39194, CVE-2023-3635,
    CVE-2023-36479, CVE-2023-40167
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Fix WS-2023-0236
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Support a local ARM Docker image by using Ubuntu Jammy for the base image. Also use only the JRE to keep the image size smaller. Resolves #3352. (#3355)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 26 Sep 2023 09:10:18 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add Dissect Processor (#3363)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 25 Sep 2023 22:30:48 -0500
    
    
    * Added Dissect Processor Functionality
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Fixed checkstyle issue
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Tweak readme and a unit test
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Fix build failures
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments - separate unit tests for dissector from processor;
    add delimiter and fieldhelper tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    Signed-off-by:
    Hai Yan &lt;oeyh@amazon.com&gt;
    Co-authored-by: Vishal Boinapalli
    &lt;vishalboinapalli3@gmail.com&gt;

* __Add tagging on failure for KeyValue processor (#3368)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 25 Sep 2023 14:32:48 -0500
    
    
    * readme, config done, main code integration in progress
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * clarify readme with example output
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * add import statement
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * Add tagging on failure
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    Signed-off-by: Hai Yan
    &lt;oeyh@amazon.com&gt;
    Co-authored-by: Kat Shen &lt;katshen@amazon.com&gt;

* __Updates commons-compress to 1.24.0 which fixes CVE-2023-42503. As part of this change, I updated the Apache commons projects to use the Gradle version catalog to keep versions in sync. Resolves #3347. (#3371)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 25 Sep 2023 08:19:54 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Write to root when destination is set to null; add overwrite option (#3380)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 22 Sep 2023 11:57:13 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Rebased to latest (#3364)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 20 Sep 2023 12:09:10 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Consolidate the end-to-end Gradle tasks which are shared in common between the different tests. (#3344)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 20 Sep 2023 11:38:26 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.apache.parquet:parquet-common in /data-prepper-api (#2966)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 20 Sep 2023 10:28:07 -0700
    
    
    Bumps [org.apache.parquet:parquet-common](https://github.com/apache/parquet-mr)
    from 1.12.3 to 1.13.1.
    - [Changelog](https://github.com/apache/parquet-mr/blob/master/CHANGES.md)
    -
    [Commits](https://github.com/apache/parquet-mr/compare/apache-parquet-1.12.3...apache-parquet-1.13.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.parquet:parquet-common
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-codec:commons-codec (#2968)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 20 Sep 2023 10:26:49 -0700
    
    
    Bumps [commons-codec:commons-codec](https://github.com/apache/commons-codec)
    from 1.15 to 1.16.0.
    -
    [Changelog](https://github.com/apache/commons-codec/blob/master/RELEASE-NOTES.txt)
    
    -
    [Commits](https://github.com/apache/commons-codec/compare/rel/commons-codec-1.15...rel/commons-codec-1.16.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: commons-codec:commons-codec
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.hibernate.validator:hibernate-validator in /data-prepper-core (#2974)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 20 Sep 2023 10:25:41 -0700
    
    
    Bumps
    [org.hibernate.validator:hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 8.0.0.Final to 8.0.1.Final.
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/main/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/8.0.0.Final...8.0.1.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Armeria 1.25.2 (#3351)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 20 Sep 2023 06:35:42 -0700
    
    
    Updates Armeria to 1.25.2. This also removes a Gradle resolution strategy which
    fixes some dependencies to specific versions. Instead, use a dependency version
    requirement which allows for using newer versions. Resolves #3069.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Rebased to latest (#3358)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 19 Sep 2023 08:00:49 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __FEAT: AWS secret extension (#3340)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 18 Sep 2023 23:48:02 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Use async client to delete scroll and pit for OpenSearch as workaroun… (#3338)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 18 Sep 2023 14:14:34 -0500
    
    
    Use async client to delete scroll and pit for OpenSearch as workaround for bug
    in client
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Recursive (#3198)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Mon, 18 Sep 2023 14:13:28 -0500
    
    
    * readme and config
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * clarify readme
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * working on recursive implementation, resolving issues
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * resolve errors
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * inner string parse logic done, working on splitter logic
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * write recursive implementation and reorganize code for clarity, fixing bugs
    with recursing
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * basic implementation done and working, cleaning code and testing edge cases
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * resolve duplicate value test failures and add basic recursive test
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * write tests and specify configs in regards to recursive
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * switch transform_key config functionality, specify that splitters have to
    have length = 1, switch bracket check logic to pattern matching
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * clean code
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * fix errors
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * fix nits
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    ---------
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    Co-authored-by: Kat Shen
    &lt;katshen@amazon.com&gt;

* __Updates Trace Analytics sample appliction to run again (#3348)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 18 Sep 2023 12:04:09 -0700
    
    
    Get the Trace Analytics sample app running again. This includes version updates
    for dependencies and some corrections from the previous PR which started using
    Temurin which brought in Ubuntu in the image. Adds GitHub Actions to verify
    that the trace-analytics example apps can still build Docker images.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Rename os source rate/job_count to interval/count, acquire UNASSIGNED partitions before CLOSED partitions (#3327)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Sat, 16 Sep 2023 12:06:18 -0700
    
    
    * Rename os source rate/job_count to interval/count, acquire UNASSIGNED
    partitions before CLOSED partitions
    Signed-off-by: Taylor Gray
    &lt;tylgry@amazon.com&gt;
    
    * Rename count to index_read_count
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    
    ---------
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates the trace analytics sample app to run using the latest Spring Boot - 3.1.3. Also updates to using JDK 17 which is required, along with moving to the Temurin Docker image as the OpenJDK Docker image is deprecated. (#3343)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 15 Sep 2023 12:02:40 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Moved the S3 source package to include s3 in the package name. (#3339)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 15 Sep 2023 12:02:10 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Gatling performance tests - round-robin host property and documentation for recent changes. (#3320)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 15 Sep 2023 12:01:39 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __BUG: Stop S3 source on InterruptedException (#3331)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 15 Sep 2023 11:26:09 -0700
    
    
    Stop S3 source on InterruptedException
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __ENH: support pipeline extensions in pipeline config (#3299)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 14 Sep 2023 19:55:07 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Adds README for the RSS Source plugin (#2350)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Thu, 14 Sep 2023 09:53:18 -0700
    
    
    Adds README for the RSS Source plugin
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;

* __Moves cmanning09 to the emeritus section. (#3337)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 14 Sep 2023 09:23:07 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds a Gradle task to generate an aggregate test report. This is not currently used by any automation, but this makes it available for a developer to use. (#3325)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 14 Sep 2023 06:55:22 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Run the Gradle builds in parallel to reduce the overall build time. (#3324)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 14 Sep 2023 06:55:07 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds missing license headers section to the CONTRIBUTING.md file. (#3292)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 12 Sep 2023 09:45:51 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Exponential backoff and jitter for opensearch source when no indices are available to process (#3321)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 12 Sep 2023 11:36:57 -0500
    
    
    Add linear backoff and jitter to opensearch source when no indices are
    available
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix issue of skipping new partitions/indices for the opensearch source (#3319)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 12 Sep 2023 10:57:31 -0500
    
    
    Fix issue where the source coordinator would skip creating partitions for new
    items for the os source
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    
    ---------
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix NPE in s3 scan partition supplier (#3317)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 12 Sep 2023 10:09:45 -0500
    
    
    Fix potential NPE in s3 scan partition supplier
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Uses mocking in the SQS Source test to simplify the unit tests and reduce build times. This knocks off close to a minute from the build. (#3303)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 11 Sep 2023 11:17:08 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Need two digits for dates in the common Apache log format in the Gatling performance tests. Formatting fixes. (#3318)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 8 Sep 2023 13:13:15 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump grpcio in /release/smoke-tests/otel-span-exporter (#2984)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 7 Sep 2023 14:56:12 -0700
    
    
    Bumps [grpcio](https://github.com/grpc/grpc) from 1.50.0 to 1.53.0.
    - [Release notes](https://github.com/grpc/grpc/releases)
    -
    [Changelog](https://github.com/grpc/grpc/blob/master/doc/grpc_release_schedule.md)
    
    - [Commits](https://github.com/grpc/grpc/compare/v1.50.0...v1.53.0)
    
    ---
    updated-dependencies:
    - dependency-name: grpcio
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump certifi in /release/smoke-tests/otel-span-exporter (#3062)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 7 Sep 2023 14:54:56 -0700
    
    
    Bumps [certifi](https://github.com/certifi/python-certifi) from 2022.12.7 to
    2023.7.22.
    -
    [Commits](https://github.com/certifi/python-certifi/compare/2022.12.07...2023.07.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: certifi
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Gatling performance test enhancements - HTTPS, path configuration, AWS SigV4 (#3312)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 7 Sep 2023 09:29:13 -0700
    
    
    Adds Gatling configurations for using HTTPS and for configuring the target
    path. Resolves #3308. 
     Increase the maximum response time for the SingleRequestSimulation to 1
    second. This is in line with other tests. 
     Adds AWS SigV4 signing in the Gatling performance tests. Also moves the
    Gatling setup into constructors rather than static initializers. Resolves
    #3308.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds sigv4 support to Elasticsearch client (#3305)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 6 Sep 2023 14:30:33 -0700
    
    
    Adds sigv4 support to Elasticsearch client. Move
    AwsRequestSigningApacheInterceptor to aws-plugin-api, use in os source and sink
    
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add metrics for the opensearch source (#3304)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 6 Sep 2023 08:25:31 -0700
    
    
    Add metrics for the opensearch source
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#3298)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Sep 2023 07:45:55 -0700
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.14.4 to 1.14.7.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.4...byte-buddy-1.14.7)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates Bouncy Castle to 1.76. This moves the dependency into the version catalog and starts using the jdk18on series as Data Prepper requires Java 11 as a minimum anyway. (#3302)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 Sep 2023 06:42:52 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump net.bytebuddy:byte-buddy-agent in /data-prepper-plugins/opensearch (#3297)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 Sep 2023 09:37:28 -0700
    
    
    Bumps [net.bytebuddy:byte-buddy-agent](https://github.com/raphw/byte-buddy)
    from 1.14.4 to 1.14.7.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.4...byte-buddy-1.14.7)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates to Gradle 8.3; fixes deprecated Gradle behavior (#3269)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 1 Sep 2023 09:36:23 -0700
    
    
    Updates to Gradle 8.3, including fixing deprecated behavior. Resolves #3267
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump tough-cookie from 4.1.2 to 4.1.3 in /release/staging-resources-cdk (#2993)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 Sep 2023 09:02:21 -0700
    
    
    Bumps [tough-cookie](https://github.com/salesforce/tough-cookie) from 4.1.2 to
    4.1.3.
    - [Release notes](https://github.com/salesforce/tough-cookie/releases)
    -
    [Changelog](https://github.com/salesforce/tough-cookie/blob/master/CHANGELOG.md)
    
    - [Commits](https://github.com/salesforce/tough-cookie/compare/v4.1.2...v4.1.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: tough-cookie
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __MAINT: merge dataflow model instead of files (#3290)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 31 Aug 2023 15:38:14 -0500
    
    
    ---------
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Bump semver and aws-cdk-lib in /release/staging-resources-cdk (#3047)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 31 Aug 2023 11:28:13 -0700
    
    
    Bumps [semver](https://github.com/npm/node-semver) to 7.5.3 and updates
    ancestor dependencies [semver](https://github.com/npm/node-semver) and
    [aws-cdk-lib](https://github.com/aws/aws-cdk/tree/HEAD/packages/aws-cdk-lib).
    These dependencies need to be updated together.
    
     Updates `semver` from 6.3.0 to 7.5.3
    - [Release notes](https://github.com/npm/node-semver/releases)
    - [Changelog](https://github.com/npm/node-semver/blob/main/CHANGELOG.md)
    - [Commits](https://github.com/npm/node-semver/compare/v6.3.0...v7.5.3)
     Updates `semver` from 5.7.1 to 7.5.3
    - [Release notes](https://github.com/npm/node-semver/releases)
    - [Changelog](https://github.com/npm/node-semver/blob/main/CHANGELOG.md)
    - [Commits](https://github.com/npm/node-semver/compare/v6.3.0...v7.5.3)
     Updates `aws-cdk-lib` from 2.80.0 to 2.88.0
    - [Release notes](https://github.com/aws/aws-cdk/releases)
    - [Changelog](https://github.com/aws/aws-cdk/blob/main/CHANGELOG.v2.md)
    -
    [Commits](https://github.com/aws/aws-cdk/commits/v2.88.0/packages/aws-cdk-lib)
    
    ---
    updated-dependencies:
    - dependency-name: semver
     dependency-type: indirect
    - dependency-name: semver
     dependency-type: indirect
    - dependency-name: aws-cdk-lib
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add e2e acknowledgments support to opensearch source (#3278)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 30 Aug 2023 21:49:42 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add support for OpenSearch Serverless collections to the opensearch source (#3288)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 30 Aug 2023 21:48:00 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add Support for OTel Log SeverityText (#3280) (#3281)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Wed, 30 Aug 2023 10:53:20 -0700
    
    
    Add Support for OTel Log SeverityText (#3280)
     The OpenTelemetry Codec lacks support for the severity text.
    This oversight
    is corrected by extracting the field from the OTLP
    source data and copying it
    to a matching field in the JSON
    document. It tightly aligns with the already
    supported SeverityNumber
    field. This closes a gap in the OTLP logs data model
    mapping.
    Unit tests of codec and JSON mapping are adjusted for the added
    
    field.
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __ENH: allow extension configuration from data prepper configuration (#2851)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 30 Aug 2023 12:31:05 -0500
    
    
    * ADD: initial implementation on injecting extension config
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Reduce sleep times in BlockingBufferTests to speed up unit tests. (#3221)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 29 Aug 2023 13:46:28 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes some unnecessary dependencies in the S3 sink and Parquet codecs (#3275)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 29 Aug 2023 08:36:14 -0700
    
    
    Removes some unnecessary dependencies in the S3 sink and Parquet codec
    projects. Updating the Parquet version to 1.13.1 consistently. Exclude HDFS
    client.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Checkstyle to the latest version - 10.12.3 - to attempt to remove Guava vulnerability. (#3276)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 29 Aug 2023 08:35:36 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add exception handling and retry to uncaught exceptions, catch IndexN… (#3250)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 28 Aug 2023 16:24:48 -0500
    
    
    Add exception handling and retry to uncaught exceptions, catch
    IndexNotFoundException for os source
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Enable publishing to all platforms in jenkins release pipeline (#3274)__

    [Sayali Gaikawad](mailto:61760125+gaiksaya@users.noreply.github.com) - Mon, 28 Aug 2023 12:41:43 -0700
    
    
    Signed-off-by: Sayali Gaikawad &lt;gaiksaya@amazon.com&gt;

* __Adds Data Prepper 2.4.0 changelog. (#3223)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 28 Aug 2023 12:01:54 -0700
    
    
    Adds Data Prepper 2.4.0 changelog.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix jenkins maven publishing stage and disable other stages for now (#3271)__

    [Sayali Gaikawad](mailto:61760125+gaiksaya@users.noreply.github.com) - Mon, 28 Aug 2023 12:01:06 -0700
    
    
    Signed-off-by: Sayali Gaikawad &lt;gaiksaya@amazon.com&gt;

* __Removes Maxmind license keys from test URLs. (#3270)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 28 Aug 2023 10:41:27 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix for kafka source issue #3264 (aws glue excetion handling) (#3265)__

    [Hardeep Singh](mailto:mzhrde@amazon.com) - Sat, 26 Aug 2023 19:34:41 -0500
    
    
    

* __Kafka sink (#3127)__

    [rajeshLovesToCode](mailto:131366272+rajeshLovesToCode@users.noreply.github.com) - Sat, 26 Aug 2023 15:59:20 -0700
    
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;

* __Fix for kafka source issue #3247 (offset commit stops on deserialization error) (#3260)__

    [Hardeep Singh](mailto:mzhrde@amazon.com) - Fri, 25 Aug 2023 16:57:59 -0700
    
    
    Signed-off-by: Hardeep Singh &lt;mzhrde@amazon.com&gt;

* __Disallow the combination of a user-defined schema and include/exclude keys (#3254)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 25 Aug 2023 15:42:16 -0700
    
    
    Disallow the combination of a user-defined schema and include/exclude keys in
    the Parquet/Avro sink codecs. Resolves #3253.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes build broken by RELEASING.md spotless check. (#3258)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 25 Aug 2023 14:59:11 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds a RELEASING.md file to the root of the project (#3251)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 25 Aug 2023 14:04:43 -0700
    
    
    Adds a RELEASING.md file to the root of the project. This has updated
    instructions for the new release workflow. Resolves #3108.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes a bug with the S3 parquet codec which was not calculating size correctly. Require the parquet codec only with in_memory which is how it is buffering data. Some debugging help. (#3249)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 25 Aug 2023 13:43:31 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Behavioral change to Avro codecs and schema handling (#3238)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 24 Aug 2023 14:15:53 -0700
    
    
    Change the behavior of Avro-based codecs. When a schema is defined, rely on the
    schema rather than the incoming event. If the schema is auto-generated, then
    the incoming event data must continue to match. Fix Avro arrays which were only
    supporting arrays of strings previously.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Batch the errors writing to the S3 sink to reduce the number of errors reported. (#3242)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 24 Aug 2023 14:14:24 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Catch when no object exists and mark as completed in s3 scan (#3241)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 24 Aug 2023 13:52:20 -0700
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix for kafka source not committing offsets issue #3231 (#3232)__

    [Hardeep Singh](mailto:mzhrde@amazon.com) - Thu, 24 Aug 2023 12:42:55 -0700
    
    
    Signed-off-by: Hardeep Singh &lt;mzhrde@amazon.com&gt;

* __Removes @cmanning09 from the CODEOWNERS file to allow the release build to proceed. (#3225)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 23 Aug 2023 15:45:03 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Improvements in the release.yml GitHub Action: Better conditional to fail the promote if the build fails, increased the timeout, added the issues write permissions, string literal correction. (#3224)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 23 Aug 2023 12:41:19 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Improve the S3 sink integration tests combinations. The tests are now more consistent and avoid some redundant tests, thus also running faster. Sets up to have fewer combinations while testing all codecs. (#3199)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 23 Aug 2023 12:16:02 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add 2.4 release notes (#3220)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 23 Aug 2023 06:43:44 -0700
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updates documentation for the Avro codec and S3 sink. Resolves #3162. (#3211)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Aug 2023 15:17:11 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Set main version to 2.5.0 (#3215)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 22 Aug 2023 15:16:45 -0700
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Kafka source fixes: commit offsets, consumer group mutations, consumer shutdown (#3207)__

    [Hardeep Singh](mailto:mzhrde@amazon.com) - Tue, 22 Aug 2023 15:12:26 -0700
    
    
    Removed acknowledgments_timeout config from kafka source
     Signed-off-by: Hardeep Singh &lt;mzhrde@amazon.com&gt;

* __Catch exceptions when writing to the output codec and drop the event. (#3210)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Aug 2023 14:40:53 -0700
    
    
    Catch exceptions when writing to the output codec and drop the event. Correctly
    release failed events in the S3 sink.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Generated THIRD-PARTY file for fecb842 (#3212)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 22 Aug 2023 14:09:53 -0700
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Normalize the include/exclude keys in the JacksonEvent implementation in order to fix a problem where the ndjson codec was not correctly including/excluding keys. (#3209)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Aug 2023 13:47:29 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bug where enum for s3 select was using NotBlank annotation (#3208)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 22 Aug 2023 14:04:46 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Adds an auto_schema flag to require a user to be explicit in using auto-schema generation. (#3206)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Aug 2023 10:25:03 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add jenkins release workflow and refactor GHA workflow (#3204)__

    [Sayali Gaikawad](mailto:61760125+gaiksaya@users.noreply.github.com) - Tue, 22 Aug 2023 09:31:00 -0700
    
    
    Signed-off-by: Sayali Gaikawad &lt;gaiksaya@amazon.com&gt;

* __Removes code which isn&#39;t used for the Avro and Parquet codecs. This will keep untested and errant code paths out of the project. Resolves #3201. (#3205)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Aug 2023 07:40:32 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates to the S3 sink to speed up the unit test time. There are a few major changes - use the Duration class instead of a nebulous long to have millisecond options and clarity; inject the retry sleep time so that the tests can sleep for shorter time; using mocking where possible to avoid unnecessary sleeps. (#3203)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Aug 2023 07:05:30 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix: IllegalArgument Exception in String converter (#3172)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 21 Aug 2023 12:06:13 -0700
    
    
    * Fix: IllegalArgument Exception in String converter
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added tags_on_failure
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Addressed feedback
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Corrects the output codecs for Avro/Parquet to use the include/exclude keys. Also adds a shouldNotIncludeKey method to OutputCodecContext. (#3197)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 21 Aug 2023 08:56:19 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Correctly add compression extensions to the generated S3 sink keys. If compression is internal, does not utilize. Resolves #3158. (#3196)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 21 Aug 2023 08:47:04 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix uncaught exception causing pipeline shutdown  (#3189)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 21 Aug 2023 10:15:19 -0500
    
    
    * Catch ClassCastException in JacksonOtelLog.toJsonString()
    * Add overwrite option to parse-json processor
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updates the Data Prepper tar.gz artifact to include JDK 17.0.8_7 which is the current latest version available. (#3136)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 18 Aug 2023 18:17:15 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated scan start_time ,end_time and range combinations (#3188)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 18 Aug 2023 16:56:54 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Corrects the JSON output codec to write Events as provided rather than convert to string. Also fixes the include/exclude keys. Adds a boolean check in OutputCodecContext so that this can be used by other codecs. (#3195)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 18 Aug 2023 16:44:40 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Suppport null values in Avro and Parquet output codecs when the schema is null. Auto-generate schemas that are nullable so that null values can be included in these schemas. Resolves part of #3158. (#3194)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 18 Aug 2023 16:15:56 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added validations in include and exclude keys (#3181)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 18 Aug 2023 13:46:27 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __FIX: set default value for enable_compression (#3190)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 18 Aug 2023 10:47:14 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Automatically promote the releases in the release workflow once the release issue has been approved by two maintainers. Resolves #2122. (#3149)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 18 Aug 2023 08:30:16 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Continue calling S3SinkService::output even if records is empty to flush stale batches (#3187)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 18 Aug 2023 08:29:49 -0500
    
    
    Continue calling S3SinkService::output even if records is empty to flush stale
    batches (#3187)
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __merged with latest (#3182)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 17 Aug 2023 16:23:06 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Correct Parquet support for the S3 sink and a new multipart buffer type (#3186)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 17 Aug 2023 16:35:25 -0500
    
    
    Correct the ParquetOutputCodec and moved into the S3 sink project for now. It
    has a few corrections including support for compression and avoiding multiple
    S3 copies. This PR also adds a new buffer type to the S3 sink - Multipart
    uploads. This PR also includes a number of refactorings to the project and the
    integration tests.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Kafka source/sink to use ByteCount (#3183)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 17 Aug 2023 14:24:30 -0700
    
    
    * Retry without seeking incase of AWSSchemaRegistryException
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Updated to link usage and config to DataPrepper documentation
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Update Kafka source/sink to use ByteCount
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add Exemplars to metrics generated in aggregate processor (#3165)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 17 Aug 2023 09:43:58 -0700
    
    
    * Add Exemplars to metrics generated in aggregate processor
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add limit to cardinality key, and metric for cardinality overflow (#3173)__

    [Jonah Calvo](mailto:caljonah@amazon.com) - Thu, 17 Aug 2023 08:40:39 -0700
    
    
    * Add limit to cardinality key, and metric for cardinality overflow
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Change cardinality overflow warning from once to every five minutes
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    ---------
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;

* __Adds a new integration test to the S3 sink which can test different scenarios. This currently is testing against ndjson since this codec generally works. (#3179)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 16 Aug 2023 14:50:36 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Only log that the scan is complete one time for s3 scan (#3168)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 16 Aug 2023 13:33:10 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix NPE on s3 source stopping without sqs, stop s3 scan worker thread on stopping of the s3 source (#3178)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 16 Aug 2023 13:09:41 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Support other scan time configs= combinations (#3151)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 16 Aug 2023 12:53:02 -0500
    
    
    * Added support for additional time comibinations in s3 scan
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Deprecate document_id_field and add support for document_id with formatting (#3153)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 16 Aug 2023 12:27:29 -0500
    
    
    Deprecate document_id_field and add support for document_id with formatting
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Minor fixes to Kafka Source (#3174)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 16 Aug 2023 10:23:15 -0700
    
    
    * Minor fixes to Kafka Source
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed unused configs
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Reword circuit breaker configuration log message (#3175)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 16 Aug 2023 12:18:00 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix timestamp format (#3171)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 16 Aug 2023 11:52:53 -0500
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix S3 sink writing to closed stream exception (#3170)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 16 Aug 2023 11:22:07 -0500
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Snappy as a compression option in the S3 sink: adds new option and engine, adds missing unit test for the CompressionOption class, make other compression engine classes package private. (#3155)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 16 Aug 2023 10:39:35 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add catching and logging of exceptions for s3 scan worker (#3159)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 15 Aug 2023 15:36:04 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Support s3:// prefix (#3156)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 15 Aug 2023 10:39:01 -0500
    
    
    Support s3:// prefix
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fixes two flaky unit tests that have failed recently (#3150)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 14 Aug 2023 16:02:18 -0500
    
    
    Fixes two unit tests that have failed and are probably flaky. The
    ParseTreeEvaluatorListenerTest appears to be using negative values sometimes,
    which seems to be unsupported. The OTelLogsSourceTest test failed as well, but
    it appears this code may not always be executed because it was mixing Hamcrest
    and Mockito.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix flaky integration test by wrapping a list in a new list to avoid a ConcurrentModificationException. Resolves #3139. (#3152)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 14 Aug 2023 16:02:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix flakey test caused by RCF variance. Update metric for RCF Instances (#3145)__

    [Jonah Calvo](mailto:caljonah@amazon.com) - Mon, 14 Aug 2023 13:10:21 -0500
    
    
    * Fix flakey test caused by RCF variance
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Change metric name and type. Update test readability
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Fix unit test to account for metric change
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    ---------
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;

* __Fix Null pointer exception when schema registry not specified (#3147)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 11 Aug 2023 18:35:17 -0700
    
    
    * Fix Null pointer exception when schema registry not specified
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fix failing test cases
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Glue registry fixes. Fixed a bug in getMSKBootstrapServers (#3142)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 11 Aug 2023 10:28:08 -0700
    
    
    * Glue registry fixes. Fixed a bug in getMSKBootstrapServers
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Changed default auto commit reset to earliest
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add default value to cardinality keys (#3144)__

    [Jonah Calvo](mailto:caljonah@amazon.com) - Fri, 11 Aug 2023 11:14:43 -0500
    
    
    Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;

* __Adds S3 sink compression. Resolves #3130. (#3138)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 11 Aug 2023 10:58:22 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for scheduled scan to s3 scan (#3140)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 11 Aug 2023 10:54:42 -0500
    
    
    Add support for scheduled scan to s3 scan
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __The rss-source is defined twice in the settings.gradle and this removes the extra one. (#3134)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 10 Aug 2023 20:02:37 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding cardinality key support for AD processor (#3073)__

    [Jonah Calvo](mailto:jonah.calvo@gmail.com) - Thu, 10 Aug 2023 13:33:20 -0700
    
    
    * Adding cardinality key support for AD processor
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    * Refactor hash function to common package. Add metrics for RCF instances.
    Implement optional verbose mode for RCF
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    
    ---------
     Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;

* __S3 single scan improvements (#3124)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 10 Aug 2023 15:30:28 -0500
    
    
    * S3 single scan improvements
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Correct the behavior of the JSON output codec to write a JSON object first. Adds a configurable keyName for the array. (#3132)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 10 Aug 2023 15:13:27 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add metrics to Kafka Source (#3118)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 10 Aug 2023 11:38:43 -0700
    
    
    * Add metrics to Kafka Source
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed debug print statement
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added total committed metric and fixed tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed number of committed records stat. Also fixed bug when acknowledgements
    enabled
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments. Fixed acknowledgements related bug
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed to use counters for records/bytes consumed metrics
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed unused code
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added a metric for keeping track of number of consumers without any
    partitions assigned
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added unit test for KafkaTopicMetrics
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Update the developer_guide.md with updated instructions for user documentation. Updated the PR template to include documentation, and updated the Issues Resolved section to encourage use of &#34;Resolves #&#34;. (#2772)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 10 Aug 2023 11:51:16 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Improved Avro error reporting related to schemas (#3110)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 10 Aug 2023 11:22:33 -0500
    
    
    Check the Avro schema when starting Data Prepper and throw a clearer exception
    when an Avro field is missing from the schema. Some code clean-up.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates to the Avro codec README.md to include an example of using an Avro schema for VPC Flow Logs. Updates the YAML to make the string easier to handle. (#3111)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 10 Aug 2023 10:21:05 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __MAINT: add integ test coverage for ODFE 0.10.0 (#3131)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 9 Aug 2023 19:29:13 -0500
    
    
    * MAINT: add integ test coverage for ODFE 0.10.0
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Adds Apache commons-lang3 to the Gradle version catalog and updates it to version 3.13.0. (#3120)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 9 Aug 2023 17:23:16 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixed issue with CloudWatchLogMetrics not updating counters when no event handles are present (#3114)__

    [Marcos Gonzalez Mayedo](mailto:95880281+MaGonzalMayedo@users.noreply.github.com) - Wed, 9 Aug 2023 16:20:10 -0500
    
    
    Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    Co-authored-by:
    Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;

* __Add ttl to all dynamo source coordination store items on creation, not just when they are COMPLETED (#3121)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 9 Aug 2023 13:26:55 -0500
    
    
    Add ttl to all dynamo source coordination store items on creation, not just
    when they are COMPLETED
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add include_keys and exclude_keys to S3 sink (#3122)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Wed, 9 Aug 2023 13:15:39 -0500
    
    
    * Add validation and update document
     Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;
    
    * Add OutputCodecContext for output codecs.
     Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;
    
    * Add OutputCodecContext for output codecs.
     Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;
    
    ---------
     Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Default keys (#3075)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Tue, 8 Aug 2023 12:19:36 -0500
    
    
    * readme and config
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * skeleton logic written
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * default keys impl and tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * finish tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * rerun checks
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * change impl to have parity with logstash
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * add clarifying example to readme, fix edge cases, add tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * rename test for clarity
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * change logging statements from string.format()
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * fix default key check error
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * change default config name to default_values, fix to have parity with
    logstash, enhance relevant tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * rerun checks
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * fix nits
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * remove extraneous test
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * clean up illegal argument statements, parameterize tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    ---------
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    Co-authored-by: Kat Shen
    &lt;katshen@amazon.com&gt;

* __S3 scan enhancements (#3049)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 7 Aug 2023 16:50:20 -0500
    
    
    * S3 scan enhancements
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __-Support for kafka-sink (#2999)__

    [rajeshLovesToCode](mailto:131366272+rajeshLovesToCode@users.noreply.github.com) - Mon, 7 Aug 2023 09:07:18 -0700
    
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    * -Support for kafka-sink
    Signed-off-by: rajeshLovesToCode
    &lt;rajesh.dharamdasani3021@gmail.com&gt;

* __Centralize exception handling and fix behavior for RequestTimeoutException (#3063)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 4 Aug 2023 16:05:54 -0500
    
    
    * Centralize exception handling and fix behavior for RequestTimeoutException
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix existing tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests for exception handlers
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add copyright headers
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add better default messages
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix code to set max poll interval and fetch min bytes config (#3115)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 4 Aug 2023 12:50:56 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Sns Sink Plugin with junit test cases (#2995)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Fri, 4 Aug 2023 12:58:24 -0500
    
    
    Sns Sink Plugin with junit test cases
    
    ---------
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    Signed-off-by: Uday
    Chintala &lt;udaych20@gmail.com&gt;

* __Prometheus Sink Boiler plate code for issue #1744. (#3078)__

    [mallikagogoi7](mailto:mallikagogoi7@gmail.com) - Fri, 4 Aug 2023 10:10:49 -0500
    
    
    * Prometheus Sink boiler plate code for issue #1744.
    Signed-off-by:
    mallikagogoi7 &lt;mallikagogoi7@gmail.com&gt;
    
    * Prometheus Sink Fix for issue #1744.
    Signed-off-by: mallikagogoi7
    &lt;mallikagogoi7@gmail.com&gt;
    
    * Prometheus Sink review comment rsolved for issue #1744.
    Signed-off-by:
    mallikagogoi7 &lt;mallikagogoi7@gmail.com&gt;

* __ENH: support custom index template for ES6 in opensearch sink (#3061)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 3 Aug 2023 11:59:06 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __GitHub-Issue#2778: Added README for CloudWatch Logs Sink (#3101)__

    [Marcos Gonzalez Mayedo](mailto:95880281+MaGonzalMayedo@users.noreply.github.com) - Wed, 2 Aug 2023 17:19:06 -0500
    
    
    * Adding README
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added README to sink
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added PluginFunctionality to README
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Update data-prepper-plugins/cloudwatch-logs/README.md
     Co-authored-by: Mark Kuhn &lt;kuhnmar@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez
    Mayedo &lt;95880281+MaGonzalMayedo@users.noreply.github.com&gt;
    
    * Update data-prepper-plugins/cloudwatch-logs/README.md
     Co-authored-by: Mark Kuhn &lt;kuhnmar@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez
    Mayedo &lt;95880281+MaGonzalMayedo@users.noreply.github.com&gt;
    
    * Added fixes to configuration in README and example id
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added extra info in plugin functionality
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    ---------
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    Signed-off-by:
    Marcos Gonzalez Mayedo &lt;95880281+MaGonzalMayedo@users.noreply.github.com&gt;
    
    Co-authored-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    Co-authored-by:
    Mark Kuhn &lt;kuhnmar@amazon.com&gt;

* __Bump com.opencsv:opencsv from 5.7.1 to 5.8 (#3097)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 2 Aug 2023 17:13:11 -0500
    
    
    Bumps com.opencsv:opencsv from 5.7.1 to 5.8.
    
    ---
    updated-dependencies:
    - dependency-name: com.opencsv:opencsv
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __GitHub-Issue#2778: Refactored Config to include Duration and ByteCount types (#3099)__

    [Marcos Gonzalez Mayedo](mailto:95880281+MaGonzalMayedo@users.noreply.github.com) - Wed, 2 Aug 2023 16:43:33 -0500
    
    
    * Converted data types in the configuration to Data-Prepper types
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added Duration to backOffTime
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Attempting to fix unused imports
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    ---------
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    Co-authored-by:
    Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;

* __Updated GitHub Actions to use &#34;Data Prepper&#34; in the job titles for consistency and aligning with the project name. (#3104)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 2 Aug 2023 13:59:04 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix consumer synchronization. Fix consumer to use user-specified groupId (#3100)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 2 Aug 2023 10:43:37 -0700
    
    
    * Fix consumer synchronization. Fix consumer to use user-specified groupId
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fix check style error
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed to retry if consume records encounters an exception
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Bump org.xerial.snappy:snappy-java in /data-prepper-plugins/common (#3095)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 2 Aug 2023 09:00:13 -0500
    
    
    Bumps [org.xerial.snappy:snappy-java](https://github.com/xerial/snappy-java)
    from 1.1.10.1 to 1.1.10.3.
    - [Release notes](https://github.com/xerial/snappy-java/releases)
    -
    [Commits](https://github.com/xerial/snappy-java/compare/v1.1.10.1...v1.1.10.3)
    
    ---
    updated-dependencies:
    - dependency-name: org.xerial.snappy:snappy-java
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __GitHub-Issue#2778: Added CloudWatchLogsSink (#3084)__

    [Marcos Gonzalez Mayedo](mailto:95880281+MaGonzalMayedo@users.noreply.github.com) - Tue, 1 Aug 2023 15:58:09 -0500
    
    
    GitHub-Issue#2778: Refactoring config files for CloudWatchLogs Sink (#4)
    
    
    ---------
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez
    Mayedo &lt;alemayed@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez Mayedo
    &lt;95880281+MaGonzalMayedo@users.noreply.github.com&gt;
    Co-authored-by: Taylor Gray
    &lt;tylgry@amazon.com&gt;
    Co-authored-by: Marcos &lt;alemayed@amazon.com&gt;

* __Fix Negative acknowledgement handling and other minor issues (#3082)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 1 Aug 2023 10:10:22 -0700
    
    
    * Fix Negative acknowledgement handling and other minor issues
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed check style errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Cleanup of unused files and config
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __HttpSink Plugin Functionality for #874. (#3036)__

    [mallikagogoi7](mailto:mallikagogoi7@gmail.com) - Tue, 1 Aug 2023 08:57:06 -0700
    
    
    * HttpSink Plugin Functionality for #874.
    Signed-off-by: mallikagogoi7
    &lt;mallikagogoi7@gmail.com&gt;
    
    * Fixed review comments for #874.
    Signed-off-by: mallikagogoi7
    &lt;mallikagogoi7@gmail.com&gt;
    
    * Fixes for #874.
    Signed-off-by: mallikagogoi7 &lt;mallikagogoi7@gmail.com&gt;

* __Added Translate Processor README.md file (#3033)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Mon, 31 Jul 2023 16:19:08 -0700
    
    
    Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __GitHub-Issue#2778: Added CouldWatchLogsService, Tests and RetransmissionException (#3023)__

    [Marcos Gonzalez Mayedo](mailto:95880281+MaGonzalMayedo@users.noreply.github.com) - Mon, 31 Jul 2023 13:03:33 -0500
    
    
    * Elasticsearch client implementation with pit and no context search (#2910)
     Create Elasticsearch client, implement search and pit apis for
    ElasticsearchAccessor
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez
    Mayedo &lt;alemayed@amazon.com&gt;
    
    * GitHub-Issue#2778: Refactoring config files for CloudWatchLogs Sink (#4)
     Added Config Files for CloudWatchLogs Sink.
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added fixes from comments to code (including pathing and nomenclature syntax)
    
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Refactoring config (#5)
     Added default params for back_off and log_send_interval alongside test cases
    for ThresholdConfig.
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed deleted AwsConfig file
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Removed the s3 dependency from build.gradle, replaced the AwsAuth.. with
    AwsConfig.
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added modifiable back_off_timer, added threshold test for back_off_timer and
    params to AwsConfig
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added fixes to gradle file, added tests to AwsConfig, and used Reflective
    mapping to tests CwlSink
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added default value test to ThresholdConfig and renamed getter for
    maxRequestSize
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Removed unnecessary imports
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added cloudwatch-logs to settings.gradle
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added a quick fix to the back_off_time range
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added Buffer classes, ClientFactory similar to S3, and ThresholdCheck
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Removed unnecessary default method from ClientFactory
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added comments in Buffer Interface, change some default values to suit the
    plugin use case more
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Removed unused imports
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Changed the unused imports, made parameters final in the ThresholdCheck
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Made changes to the tests and the method signatures in ThresholdCheck, made
    fixes to gradle file to include catalog
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Removed unused methods/comments
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added CloudWatchLogsService, CloudWatchLogsServiceTest and
    RetransmissionLimitException
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed retransmission logging fixed value
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed unused imports
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed making ThresholdCheck public
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added fixes to ThresholdCheck and CloudWatchLogsService to decouple methods
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed syntax start import in CloudWatchLogsServiceTest
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Extracted LogPusher and SinkStopWatch classes for code cleanup. Addded fixes
    to variables and retry logic for InterruptExceptions
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Changed method uses in CloudWatchLogsService and removed logging the batch
    size in LogPusher
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added Multithreaded CloudWatchLogsDispatcher for handling various async calls
    to perform PLE&#39;s
     and added tests
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added fixesto test and defaulted the parameters in the config to
    CloudWatchLogs limits, customer can change this in config file
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added exponential backofftime
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed unused imports
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed up deepcopy of arraylist for service workers in CloudWatchLogsService,
    and fixed Log calling methods
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added CloudWatchLogsDispatcher builder pattern, fixed tests for Service and
    Dispatcher and modified backOffTimeBase
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Removed unused imports
     Signed-off-by:Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    Signed-off-by:
    Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added resetBuffer method, removed unnecessary RetransmissionException, and
    added logString pass in parameter for staging log events.
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Started making changes to the tests to implement the new class structure
    (performance enhancement)
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Refactored the CloudWatchLogsDispatcher into two classes with the addition of
    Uploader, introduced simple multithread tests for CloudWatchLogsService
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed issues with locking in try block and added final multithreaded tests to
    the CloudWatchLogsService class
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added CloudWatchLogsMetricsTest, changed upper back off time bound and scale,
    and refactoring changes for better code syntax (renaming, refactoring methods
    for conciseness, etc...)
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added changes to javadoc
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Update
    data-prepper-plugins/cloudwatch-logs/src/main/java/org/opensearch/dataprepper/plugins/sink/client/CloudWatchLogsDispatcher.java
    
     Co-authored-by: Mark Kuhn &lt;kuhnmar@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez
    Mayedo &lt;95880281+MaGonzalMayedo@users.noreply.github.com&gt;
    
    * Fixed comment on CloudWatchLogsDispatcher
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    ---------
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez
    Mayedo &lt;alemayed@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez Mayedo
    &lt;95880281+MaGonzalMayedo@users.noreply.github.com&gt;
    Co-authored-by: Taylor Gray
    &lt;tylgry@amazon.com&gt;
    Co-authored-by: Marcos &lt;alemayed@amazon.com&gt;
    
    Co-authored-by: Mark Kuhn &lt;kuhnmar@amazon.com&gt;

* __Config changes and support for adding different modes to put kafka key in the event (#3076)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Sun, 30 Jul 2023 22:54:47 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add include_keys and exclude_keys to sink (#2989)__

    [Aiden Dai](mailto:68811299+daixba@users.noreply.github.com) - Fri, 28 Jul 2023 16:43:20 -0700
    
    
    Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Removed IterateOn otpion (#3050)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Fri, 28 Jul 2023 09:33:08 -0700
    
    
    Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __Changed non-exact mathching logic (#3046)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Fri, 28 Jul 2023 09:31:02 -0700
    
    
    Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __Translate Processor : Added support for S3 file mappings (#3054)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Fri, 28 Jul 2023 09:30:43 -0700
    
    
    * Added support for S3 file
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Changed logic for retrieving mappings from S3 file
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    ---------
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __Moves the S3 sink and HTTP sink into their own packages. This fixes an issue where there are class conflicts at runtime. (#3067)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 27 Jul 2023 19:37:18 -0500
    
    
    

* __Exclude keys (#3055)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Thu, 27 Jul 2023 11:35:48 -0500
    
    
    * Add exclude keys
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    ---------
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    Co-authored-by: Kat Shen
    &lt;katshen@amazon.com&gt;

* __Parquet Sink Codec  (#2928)__

    [umayr-codes](mailto:130935051+umayr-codes@users.noreply.github.com) - Thu, 27 Jul 2023 08:35:31 -0700
    
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    ---------
     Co-authored-by: umairofficial &lt;umairhusain1010@gmail.com&gt;

* __Adds new configurations to the S3 source to better define bucket ownership. Resolves #2012. (#3012)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 26 Jul 2023 14:41:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __-Support for Sink Codecs (#2986)__

    [umayr-codes](mailto:130935051+umayr-codes@users.noreply.github.com) - Wed, 26 Jul 2023 11:52:00 -0700
    
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    ---------
     Co-authored-by: umairofficial &lt;umairhusain1010@gmail.com&gt;

* __Add support for Glue registry (#3056)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 25 Jul 2023 15:32:30 -0700
    
    
    * Add Support for Glue registry
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed MskGlueRegistryMultiTypeIT test
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed integration test failures
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified change Encryption:PLAINTEXT to Encryption:NONE
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Moved serdeFormat to TopicConfig
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Remove * imports from MskGlue test
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Improves some of the timeouts for the peer forwarder tests to reduce testing time. (#3020)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 25 Jul 2023 12:49:46 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Improves some of the test timing in Data Prepper core tests which are showing somewhat high test times. (#3021)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 25 Jul 2023 12:49:32 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Work to reduce test time by reducing some repeated tests, using Awaitility, and reducing delays (#3019)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 25 Jul 2023 12:49:18 -0500
    
    
    Work to reduce test time by reducing some repeated tests, using Awaitility, and
    reducing some wait times.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __FIX: missing request index (#3058)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 25 Jul 2023 12:03:25 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fix bug where exception is thrown when csv source key does not exist or is null (#3053)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 25 Jul 2023 12:01:25 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __ENH: support es 6 in sink (#3045)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 24 Jul 2023 15:52:17 -0500
    
    
    * ENH: support es 6 for bulk API
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    

* __Kafka source Confluent schema registry connectivity and OAuth implementation (#3037)__

    [Ajeesh Gopalakrishnakurup](mailto:61016936+ajeeshakd@users.noreply.github.com) - Mon, 24 Jul 2023 11:27:48 -0700
    
    
    * Schema registry connectivity with the oauth configurations
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Junit fixes
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Defect fixes
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Updated the review comments for the PR3037
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    ---------
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;

* __Remove brackets feature option (#3035)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Mon, 24 Jul 2023 13:17:14 -0500
    
    
    *add remove brackets feature option
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    ---------
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    Co-authored-by: Kat Shen
    &lt;katshen@amazon.com&gt;

* __Add Support for Auth/NoAuth with/without Encryption in Kafka with integration tests (#3042)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 20 Jul 2023 09:20:15 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Bump word-wrap from 1.2.3 to 1.2.4 in /release/staging-resources-cdk (#3044)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 20 Jul 2023 10:26:17 -0500
    
    
    Bumps [word-wrap](https://github.com/jonschlinkert/word-wrap) from 1.2.3 to
    1.2.4.
    - [Release notes](https://github.com/jonschlinkert/word-wrap/releases)
    - [Commits](https://github.com/jonschlinkert/word-wrap/compare/1.2.3...1.2.4)
    
    ---
    updated-dependencies:
    - dependency-name: word-wrap
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Translate Processor -  Added file_path functionality for local files (#3034)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Wed, 19 Jul 2023 14:20:24 -0700
    
    
    * Changed target_type option name to type
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Added file_path functionality for local file
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Fixed file parsing issue and changed error msgs
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Added IOException to log, made testcase change for mappings validation
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    ---------
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __Consolidate logic related to extracting data from a BulkOperation (#3041)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 19 Jul 2023 13:15:41 -0500
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Kafka Source - Cleanup and Enhancements for MSK (#3029)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 18 Jul 2023 15:54:21 -0700
    
    
    * Kafka Source - Cleanup and Enhancements for MSK
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkstyle error
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Fix race condition in data prepper sources using e2e acknowledgements (#3039)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 18 Jul 2023 15:53:41 -0700
    
    
    * Fix race condition in data prepper sources using e2e acknowledgements
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkStyle error
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add support for using expressions with formatString in JacksonEvent, use for index in OpenSearch sink (#3032)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 18 Jul 2023 12:10:24 -0500
    
    
    Add support for using expressions with formatString in JacksonEvent, use for
    index in OpenSearch sink
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    
    ---------
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __-Support for Sink Codecs (#3030)__

    [Omkar](mailto:133762828+omkarmmore95@users.noreply.github.com) - Mon, 17 Jul 2023 13:28:58 -0700
    
    
    * -Support for Sink Codecs
    Signed-off-by: omkarmmore95
    &lt;omkar.m.more95@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: omkarmmore95
    &lt;omkar.m.more95@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: omkarmmore95
    &lt;omkar.m.more95@gmail.com&gt;

* __Duplicate values (#3026)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Mon, 17 Jul 2023 09:35:57 -0700
    
    
    * implement transform_key feature
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * fix unit tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * fix unit tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * remove bin files
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * add static final variable for string comparison
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * add whitespace description to readme, add configs
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * writing whitespace impl
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * add whitespace impl
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * fix impl, writing tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * write whitespace test
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * fix formatting, whitespace() -&gt; trimWhitespace()
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * edit readme, add config
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * update logic to valid values set
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * correct return value
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * update variables to static
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * correct convention for private variables
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * impl allow duplicate values, writing tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * allow duplicate values impl + tests
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * modify regex portion to final variables, remove some whitespace
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * rerun checks
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * rename methods/variables for more clarity, change default bool value to be
    false
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * rerun checks
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * change logic to reflect skip_duplicate_values
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * modify tests according to changed logic
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    * remove include keys content (accidentally included it oops)
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    ---------
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    Signed-off-by: Katherine Shen
    &lt;40495707+shenkw1@users.noreply.github.com&gt;
    Co-authored-by: Kat Shen
    &lt;katshen@amazon.com&gt;

* __Connection code of HttpSink Plugin for #874. (#2987)__

    [mallikagogoi7](mailto:mallikagogoi7@gmail.com) - Mon, 17 Jul 2023 09:49:32 -0500
    
    
    Connection code of HttpSink Plugin.
    Signed-off-by: mallikagogoi7
    &lt;mallikagogoi7@gmail.com&gt;

* __GitHub-issue#253 : Implemented GeoIP processor integration test (#2927)__

    [venkataraopasyavula](mailto:126578319+venkataraopasyavula@users.noreply.github.com) - Fri, 14 Jul 2023 14:54:33 -0700
    
    
    * GitHub-issue#253 : Implemented GeoIP processor integration test
    
    Signed-off-by: venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor integration test
    
    Signed-off-by: venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor integration test
    
    Signed-off-by: venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor integration test
    
    Signed-off-by: venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor integration test
    
    Signed-off-by: venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;

* __Add support for Data Prepper expressions in the document_id_field of the OpenSearch sink, add opensearch prefix to opensearch source metadata keys (#3025)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 14 Jul 2023 10:51:25 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Translate Plugin: Simplified Config.  (#3022)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Thu, 13 Jul 2023 22:18:06 -0700
    
    
    * Translate Plugin: Simplified Config. Added functionality for multiple sources
    and multiple targets
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Moved helper methods out of config file
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    ---------
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __Adds the Data Prepper 2.3.2 change log. (#3024)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 13 Jul 2023 12:02:45 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated Kafka security configuration (#2994)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 13 Jul 2023 08:20:50 -0700
    
    
    * Add Kafka Security Configurations
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified kafka security config. Added new fields to AwsConfig
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified AwsConfig to have msk option that can take multiple options
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __GitHub-Issue#2778: Added CloudWatchLogs Buffer, ThresholdCheck, and ClientFactory utilities. (#2982)__

    [Marcos Gonzalez Mayedo](mailto:95880281+MaGonzalMayedo@users.noreply.github.com) - Wed, 12 Jul 2023 16:35:47 -0500
    
    
    Added CloudWatchLogs Buffer, ThresholdCheck, and ClientFactory utilities.
    
    ---------
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    Signed-off-by:
    Marcos Gonzalez Mayedo &lt;95880281+MaGonzalMayedo@users.noreply.github.com&gt;
    
    Co-authored-by: Marcos &lt;alemayed@amazon.com&gt;

* __Whitespace (#3004)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Wed, 12 Jul 2023 13:28:59 -0500
    
    
    implement transform_key feature
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    
    ---------
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;
    Co-authored-by: Kat Shen
    &lt;katshen@amazon.com&gt;

* __Release notes for Data Prepper 2.3.2 (#3016)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 12 Jul 2023 13:13:11 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __GitHub-issue#253 : Implemented GeoIP processor functionality (#2925)__

    [venkataraopasyavula](mailto:126578319+venkataraopasyavula@users.noreply.github.com) - Wed, 12 Jul 2023 08:58:22 -0700
    
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality
    Signed-off-by:
    venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality
    Signed-off-by:
    venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality
    Signed-off-by:
    venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality
    Signed-off-by:
    venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality
    Signed-off-by:
    venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality. Addressed Code
    review comments
    Signed-off-by: venkataraopasyavula
    &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality. Addressed Code
    review comments
    Signed-off-by: venkataraopasyavula
    &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Fixed the test-case-failed issue.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality. Addressed Code
    review comments
    Signed-off-by: venkataraopasyavula
    &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality. Addressed Code
    review comments
    Signed-off-by: venkataraopasyavula
    &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality. Addressed Code
    review comments
    Signed-off-by: venkataraopasyavula
    &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality. Addressed Code
    review comments
    Signed-off-by: venkataraopasyavula
    &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Implemented GeoIP processor functionality. Addressed Code
    review comments
    Signed-off-by: venkataraopasyavula
    &lt;venkataraopasyavula@gmail.com&gt;
    
    ---------
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    Co-authored-by: Deepak
    Sahu &lt;deepak.sahu562@gmail.com&gt;

* __Fix bucket ownership validation. Resolves #3005 (#3009)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 11 Jul 2023 21:30:00 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Introduce option to measure bulk sizes with or without compression (#2985)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Tue, 11 Jul 2023 18:17:33 -0500
    
    
    * Initial bulk estimation improvements
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add setting to enable/disable estimation with compression
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Make max local compressions configurable
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add documentation for new settings
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove debug comment
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove another debug log
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix race condition in SqsWorker when acknowledgements are enabled (#3001)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 11 Jul 2023 15:09:26 -0700
    
    
    * Fix race condition in SqsWorker when acknowledgements are enabled
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to do the synchronization in the acknowledgement set framework
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed unused variable
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comment and fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkStyle failure
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Retry s3 reads on socket exceptions. (#2992)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Tue, 11 Jul 2023 12:25:20 -0500
    
    
    * Retry s3 reads on socket exceptions.
     S3 will reset the conenction on their end frequently. To not lose data,
    data
    prepper should retry all socket exceptions by attempting to re-open
    the
    stream.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    
    * Bubble up parquet exceptions.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    
    ---------
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Fix SqsWorker error messages (#2991)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 7 Jul 2023 10:25:47 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Translate Plugin - Target Type implementation (#2979)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Thu, 6 Jul 2023 16:29:02 -0700
    
    
    * Translate Plugin - Target Type implementation
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * addressed review comments
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    ---------
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __Implement transform_key feature (#2977)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Thu, 6 Jul 2023 11:37:55 -0500
    
    
    implement transform_key feature
     Signed-off-by: Kat Shen &lt;katshen@amazon.com&gt;

* __Fix S3 errors around end of file behavior. (#2983)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Wed, 5 Jul 2023 16:20:12 -0500
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Fix Stdout and File sink (#2978)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 5 Jul 2023 10:15:15 -0700
    
    
    * Fix Stdout and File sink
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed javadoc warnings and errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Boiler plate code of HttpSink for #874. (#2916)__

    [mallikagogoi7](mailto:mallikagogoi7@gmail.com) - Wed, 5 Jul 2023 09:27:57 -0700
    
    
    * Boiler plate code of HttpSink for #874.
    Signed-off-by: mallikagogoi7
    &lt;mallikagogoi7@gmail.com&gt;
    
    * Added copyright on classes of HttpSink for #874.
    Signed-off-by:
    mallikagogoi7 &lt;mallikagogoi7@gmail.com&gt;
    
    * Moved Accumulator package to common for #874.
    Signed-off-by: mallikagogoi7
    &lt;mallikagogoi7@gmail.com&gt;
    
    * Test cases added for accumulator related classes for #874.
    Signed-off-by:
    mallikagogoi7 &lt;mallikagogoi7@gmail.com&gt;
    
    * Added HttpSink related methos in accumulator for #874.
    Signed-off-by:
    mallikagogoi7 &lt;mallikagogoi7@gmail.com&gt;
    
    * Removed plugin specific methods from common for #874.
    Signed-off-by:
    mallikagogoi7 &lt;mallikagogoi7@gmail.com&gt;

* __GitHub-Issue#2778: Added CloudWatchLogs Sink Config Files (#2922)__

    [Marcos Gonzalez Mayedo](mailto:95880281+MaGonzalMayedo@users.noreply.github.com) - Wed, 5 Jul 2023 09:12:03 -0700
    
    
    * Elasticsearch client implementation with pit and no context search (#2910)
     Create Elasticsearch client, implement search and pit apis for
    ElasticsearchAccessor
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez
    Mayedo &lt;alemayed@amazon.com&gt;
    
    * GitHub-Issue#2778: Refactoring config files for CloudWatchLogs Sink (#4)
     Added Config Files for CloudWatchLogs Sink.
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added fixes from comments to code (including pathing and nomenclature syntax)
    
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Refactoring config (#5)
     Added default params for back_off and log_send_interval alongside test cases
    for ThresholdConfig.
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Fixed deleted AwsConfig file
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Removed the s3 dependency from build.gradle, replaced the AwsAuth.. with
    AwsConfig.
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added modifiable back_off_timer, added threshold test for back_off_timer and
    params to AwsConfig
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added fixes to gradle file, added tests to AwsConfig, and used Reflective
    mapping to tests CwlSink
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added default value test to ThresholdConfig and renamed getter for
    maxRequestSize
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Removed unnecessary imports
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added cloudwatch-logs to settings.gradle
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    * Added a quick fix to the back_off_time range
     Signed-off-by: Marcos Gonzalez Mayedo &lt;alemayed@amazon.com&gt;
    
    ---------
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez
    Mayedo &lt;alemayed@amazon.com&gt;
    Signed-off-by: Marcos Gonzalez Mayedo
    &lt;95880281+MaGonzalMayedo@users.noreply.github.com&gt;
    Co-authored-by: Taylor Gray
    &lt;tylgry@amazon.com&gt;
    Co-authored-by: Marcos &lt;alemayed@amazon.com&gt;

* __Bump org.apache.commons:commons-compress in /data-prepper-plugins/common (#2960)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Jul 2023 10:59:41 -0500
    
    
    Bumps org.apache.commons:commons-compress from 1.21 to 1.23.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.commons:commons-compress
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-io:commons-io from 2.11.0 to 2.13.0 in /data-prepper-api (#2900)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Jul 2023 10:57:43 -0500
    
    
    Bumps commons-io:commons-io from 2.11.0 to 2.13.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.fasterxml.jackson.datatype:jackson-datatype-jsr310 (#2796)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Jul 2023 10:55:11 -0500
    
    
    Bumps com.fasterxml.jackson.datatype:jackson-datatype-jsr310 from 2.14.2 to
    2.15.2.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jsr310
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __-Support for Sink Codecs (#2881)__

    [Omkar](mailto:133762828+omkarmmore95@users.noreply.github.com) - Wed, 5 Jul 2023 10:53:44 -0500
    
    
    -Support for Sink Codecs
    Signed-off-by: omkarmmore95
    &lt;omkar.m.more95@gmail.com&gt;

* __Added Kafka config to support acknowledgments and MSK arn (#2976)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 4 Jul 2023 21:25:09 -0700
    
    
    * Added Kafka config to support acknowledgments and MSK arn
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to use data-prepper-core in testImplementation
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed failing test
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Define multiple keys for type conversion (#2934)__

    [wanghd89](mailto:wanghd89@gmail.com) - Tue, 4 Jul 2023 21:42:07 -0500
    
    
    * feat: add include_key options to KeyValueProcessor
     Signed-off-by: Haidong &lt;whaidong@amazon.com&gt;
    
    ---------
     Signed-off-by: Haidong &lt;whaidong@amazon.com&gt;
    Co-authored-by: Haidong
    &lt;whaidong@amazon.com&gt;

* __Translate Plugin - Added functionality for iterate_on, default, exact options (#2953)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Mon, 3 Jul 2023 14:32:32 -0500
    
    
    Added IterateOn functionality, default, exact and testcases for translate
    processor
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __Kafka Source code refactoring (#2951)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 30 Jun 2023 13:01:29 -0700
    
    
    * Kafka Source code refactoring
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixes for failing build/tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments. Cleaned up code
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add disable_authentication flag to the opensearch source (#2942)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 29 Jun 2023 11:37:34 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix CVE-2023-35165, CVE-2023-34455, CVE-2023-34453, CVE-2023-34454, C… (#2948)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 29 Jun 2023 09:59:03 -0500
    
    
    * Fix CVE-2023-35165, CVE-2023-34455, CVE-2023-34453, CVE-2023-34454,
    CVE-2023-2976
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated snappy version in build.gradle files
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update simple_pipelines.md (#2947)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Thu, 29 Jun 2023 09:29:49 -0500
    
    
    Removed a space between the second ${PWD} and the rest of the line
     Signed-off-by: Katherine Shen &lt;40495707+shenkw1@users.noreply.github.com&gt;

* __Adding Translate Processor functionality and config files (#2913)__

    [Vishal Boinapalli](mailto:vishalboinapalli3@gmail.com) - Wed, 28 Jun 2023 13:45:02 -0700
    
    
    * Adding MapValues Processor functionality and config file
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Changed logic for splitting of Keys, Added config file for Regex option
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Added testcases, made changes addressing previous review comments, Changed
    the processor name from map_values to translate
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Added regexConfigTests, made code structure changes, added check for patterns
    under regex
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    * Removed * imports
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;
    
    ---------
     Signed-off-by: Vishal Boinapalli &lt;vishalboinapalli3@gmail.com&gt;

* __Sqs Source Integration Tests (#2874)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Wed, 28 Jun 2023 10:02:25 -0500
    
    
    Sqs Source Integration Tests
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;

* __Remove validation that made keys starting or ending with . - or _ invalid, catch all exceptions in the parse json processor (#2945)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 27 Jun 2023 16:25:19 -0500
    
    
    Remove validation that made keys starting or ending with . - or _ invalid,
    catch all exceptions in the parse json processor
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Kafka source integration test (#2891)__

    [Ajeesh Gopalakrishnakurup](mailto:61016936+ajeeshakd@users.noreply.github.com) - Tue, 27 Jun 2023 10:11:52 -0700
    
    
    * Integration testcases
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Fix for the Integration testcases
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Fix for the white source issue
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Fixes for the merge conflicts
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    ---------
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;

* __Add basic opensearch source documentation for config (#2940)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 27 Jun 2023 12:02:36 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add support for writing tags along with events to Sink (#2850)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 27 Jun 2023 08:42:27 -0700
    
    
    * Updated to pass SinkContext to Sink constructors as suggested in the previous
    comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed check style errors and renamed RoutedPluginSetting to
    SinkContextPluginSetting
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed s3-sink integration test
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added javadoc for SinkContext
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Temporarily reduce coverage minimum (#2937)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 26 Jun 2023 16:50:03 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Implement scroll apis for ElasticSearch Accessor (#2930)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 26 Jun 2023 14:58:36 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Sqs Source initial changes (#2786)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Mon, 26 Jun 2023 13:12:28 -0500
    
    
    * Sqs Source implementation
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    ---------
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    Signed-off-by: Uday
    Chintala &lt;udaych20@gmail.com&gt;

* __Fix DLQ writer writing empty list (#2931)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 26 Jun 2023 09:48:31 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __S3 Scan time range improvements (#2883)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 26 Jun 2023 11:11:40 -0500
    
    
    * When no time range set, default to scan all objects; allow setting time range
    for specific bucket
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Implement searching with scroll contexts for OpenSearch (#2923)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 23 Jun 2023 15:26:52 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix addTags API in EventMetadata (#2926)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 23 Jun 2023 10:07:09 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __GitHub-issue#253 : Implemented GeoIP Plugin Configuration Code JUnit test cases (#2909)__

    [venkataraopasyavula](mailto:126578319+venkataraopasyavula@users.noreply.github.com) - Thu, 22 Jun 2023 09:11:48 -0700
    
    
    Signed-off-by: venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;

* __Elasticsearch client implementation with pit and no context search (#2910)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 21 Jun 2023 13:59:00 -0500
    
    
    Create Elasticsearch client, implement search and pit apis for
    ElasticsearchAccessor
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Added Readme.md (#2714)__

    [Ajeesh Gopalakrishnakurup](mailto:61016936+ajeeshakd@users.noreply.github.com) - Wed, 21 Jun 2023 11:49:55 -0700
    
    
    * Added Readme.md
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Updated Readme.md
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Updated Readme.md
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    ---------
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;

* __OpenSearch Sink Optimizations (#2908)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 21 Jun 2023 10:12:26 -0500
    
    
    * Fix size estimation for compression. Maintain requests across iterations for
    better packing. Limit bulk response size
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests, slight refactors
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add null handling
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Increase gradle heap
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Set flush timeout in IT
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Set flush timeout to 0 in ITs
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add documentation for flush_timeout and fix OpenSearchSinkITs
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add default to documentation
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Set flush_timeout to 5s in e2e tests to fall within timeouts
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Updated the release date (#2911)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 20 Jun 2023 13:45:17 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Kafka source retry (#2708)__

    [Ajeesh Gopalakrishnakurup](mailto:61016936+ajeeshakd@users.noreply.github.com) - Tue, 20 Jun 2023 09:50:26 -0700
    
    
    * Code rebase for the PR2708
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Fixes the code rebase issue for the PR2708
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Fixes the code rebase issue and code refactoring
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Fixes the build issue
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Refactored the consumer code for plaintext,json and avro
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Updated the review comments for the PR2708
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    ---------
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;

* __Configuration PR for HttpSink for #874. (#2890)__

    [mallikagogoi7](mailto:mallikagogoi7@gmail.com) - Mon, 19 Jun 2023 11:40:49 -0700
    
    
    Signed-off-by: mallikagogoi7 &lt;mallikagogoi7@gmail.com&gt;

* __Added 2.3.1 release notes (#2871)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 19 Jun 2023 12:44:40 -0500
    
    
    * Added 2.3.1 release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added 2.3.1 change log (#2872)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 19 Jun 2023 12:44:25 -0500
    
    
    * Added 2.3.1 change log
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated change log
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __GitHub-issue#253 : Implemented GeoIP Plugin boiler plate Code Structure (#2840)__

    [venkataraopasyavula](mailto:126578319+venkataraopasyavula@users.noreply.github.com) - Mon, 19 Jun 2023 10:01:15 -0700
    
    
    Signed-off-by: venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;

* __Change the title to be consistent with configuration (#2899)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 19 Jun 2023 09:13:06 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Do not suppress logs when there are exception in s3 source. (#2896)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Sat, 17 Jun 2023 10:04:23 -0500
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Change log for index name format failure in opensearch sink (#2894)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 16 Jun 2023 16:09:38 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updated s3 sink metrics (#2888)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Jun 2023 08:48:44 -0500
    
    
    

* __Implement NoSearchContextWorker to search with search_after and not use pit or scroll, allow override with search_context_type parameter (#2873)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 15 Jun 2023 16:14:33 -0500
    
    
    * Implement NoSearchContextWorker to search with search_after and not use pit
    or scroll, allow override with search_context_type parameter
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix silent dropping of data when index format has null keys, write to dlq if configured (#2885)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 15 Jun 2023 15:12:30 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __GitHub-issue#253 : Implemented GeoIP Plugin Configuration Code (#2811)__

    [venkataraopasyavula](mailto:126578319+venkataraopasyavula@users.noreply.github.com) - Thu, 15 Jun 2023 12:31:04 -0700
    
    
    * GitHub-issue#253 : Implemented GeoIP Plugin Configuration Code
    
    Signed-off-by: venkataraopasyavula &lt;venkataraopasyavula@gmail.com&gt;
    
    * GitHub-issue#253 : Incorporated code review comments of GeoIP Plugin
    Configuration Code
    Signed-off-by: venkataraopasyavula
    &lt;venkataraopasyavula@gmail.com&gt;

* __Update README.md for S3 sink (#2878)__

    [Travis Benedict](mailto:benedtra@amazon.com) - Thu, 15 Jun 2023 10:39:53 -0500
    
    
    Signed-off-by: Travis Benedict &lt;benedtra@amazon.com&gt;

* __Add exception when gzip input stream not have magic header. (#2879)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Thu, 15 Jun 2023 09:28:17 -0500
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __FIX: concurrentModification (#2876)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 14 Jun 2023 21:24:07 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __S3 EventBridge and security lake support (#2861)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 13 Jun 2023 17:53:18 -0500
    
    
    * EventBridge initial working draft
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add STS external ID to all STS configurations. (#2862)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Tue, 13 Jun 2023 16:08:30 -0500
    
    
    STS external ID is required by some AWS services when making an STS
    AssumeRole
    call.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Save searchAfter to state so pagination can continue where it left off when using PIT on opensearch source (#2856)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 12 Jun 2023 15:56:57 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Consolidate BufferAccumulator to buffer-api module (#2857)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 12 Jun 2023 13:29:56 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add end-to-end acknowledgement support to Stdout and File Sinks (#2860)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 12 Jun 2023 11:18:18 -0700
    
    
    * Add end-to-end acknowledgement support to Stdout and File Sinks
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed code to check for object is an instance of event
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Sqs Source configuration code changes for #2679 (#2801)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Mon, 12 Jun 2023 12:04:53 -0500
    
    
    Sqs Source configuration code changes for #2679
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;

* __Os source buffer backoff retry (#2849)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 12 Jun 2023 09:40:10 -0500
    
    
    Use buffer accumulator in opensearch source to backoff and retry
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __-Support for Sink Codecs (#2845)__

    [Omkar](mailto:133762828+omkarmmore95@users.noreply.github.com) - Fri, 9 Jun 2023 13:40:05 -0500
    
    
    Support for Sink Codecs
    Signed-off-by: omkarmmore95 &lt;omkar.m.more95@gmail.com&gt;

* __Implement basic search with point in time and search after (#2847)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 9 Jun 2023 10:06:50 -0500
    
    
    Implement basic search with point in time and search after
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix CVE in maven-artifact by excluding that dependency (#2848)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 8 Jun 2023 15:52:39 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __-Support for Sink Codecs (#2842)__

    [Omkar](mailto:133762828+omkarmmore95@users.noreply.github.com) - Thu, 8 Jun 2023 14:47:08 -0500
    
    
    Signed-off-by: omkarmmore95 &lt;omkar.m.more95@gmail.com&gt;

* __Create and delete point in time for processing an index (#2839)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 8 Jun 2023 11:32:22 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __GitHub-issue#2822: Define null characters in convert processor (#2844)__

    [saydar31](mailto:43093669+saydar31@users.noreply.github.com) - Thu, 8 Jun 2023 11:28:37 -0500
    
    
    GitHub-issue#2822: Define null characters in convert processor
    Signed-off-by:
    Aidar Shaidullin &lt;ajdarshaydullin@gmail.com&gt;
     Signed-off-by: saydar31 &lt;ajdarshaydullin@gmail.com&gt;
    
    ---------
     Signed-off-by: saydar31 &lt;ajdarshaydullin@gmail.com&gt;
    Co-authored-by: saydar31
    &lt;ajdarshaydullin@gmail.com&gt;

* __Implement opensearch index partition creation supplier and PitWorker without processing indices (#2821)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 6 Jun 2023 17:59:59 -0500
    
    
    Implement opensearch index partition creation supplier and PitWorker without
    processing indices
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Added change log for 2.3 (#2836)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 6 Jun 2023 12:32:55 -0500
    
    
    * Added change log for 2.3
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Generated change log in 2.3 branch
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Adds release notes for Data Prepper 2.3.0. (#2833)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 6 Jun 2023 10:59:57 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates main to Data Prepper 2.4. (#2832)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 6 Jun 2023 10:44:47 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bug where s3 stream was closing too early. (#2830)__

    [Adi Suresh](mailto:adsuresh@amazon.com) - Mon, 5 Jun 2023 20:21:40 -0500
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Generated THIRD-PARTY file for 3a70e73 (#2828)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 17:06:11 -0500
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Fixes a bug in the S3 sink where events without handles throw NPE (#2814)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 5 Jun 2023 16:34:31 -0500
    
    
    Fixes a bug in the S3 sink where events without handles are throwing NPEs by
    skipping any such handles.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add include_keys as a new option to KeyValueProcessor (#2776)__

    [wanghd89](mailto:wanghd89@gmail.com) - Mon, 5 Jun 2023 16:33:57 -0500
    
    
    feat: add include_key options to KeyValueProcessor
     Signed-off-by: Haidong &lt;whaidong@amazon.com&gt;
    
    ---------
     Signed-off-by: Haidong &lt;whaidong@amazon.com&gt;
    Co-authored-by: Haidong
    &lt;whaidong@amazon.com&gt;

* __Add a doc for end to end acknowledgements (#2487)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 5 Jun 2023 15:26:00 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __addressing missing metrics in README (#2812)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Mon, 5 Jun 2023 15:25:14 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Adds support for composable index templates (#2808)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 5 Jun 2023 15:20:32 -0500
    
    
    Adds support for composable index templates. Resolves #1275. Update the
    OpenSearch sink integration test to skip the composable index template tests on
    older versions of OpenDistro. Updated the README.md with the new template_type
    feature.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump net.bytebuddy:byte-buddy-agent in /data-prepper-plugins/opensearch (#2608)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 15:18:01 -0500
    
    
    Bumps [net.bytebuddy:byte-buddy-agent](https://github.com/raphw/byte-buddy)
    from 1.14.3 to 1.14.4.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.3...byte-buddy-1.14.4)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.fasterxml.jackson.datatype:jackson-datatype-jdk8 (#2792)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 15:16:30 -0500
    
    
    Bumps com.fasterxml.jackson.datatype:jackson-datatype-jdk8 from 2.15.1 to
    2.15.2.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jdk8
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/common (#2790)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 15:15:49 -0500
    
    
    Bumps commons-io:commons-io from 2.11.0 to 2.12.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/otel-trace-source (#2793)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 15:15:16 -0500
    
    
    Bumps commons-io:commons-io from 2.11.0 to 2.12.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add new InputCodec interface to support seek-able input and corresponding implementation and tests for S3 objects (#2727)__

    [Adi Suresh](mailto:suresh.aditya@gmail.com) - Mon, 5 Jun 2023 14:37:51 -0500
    
    
    Add new InputCodec interface to support seek-able input and corresponding
    implementation and tests for S3 objects (#2727)
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    
    ---------
     Signed-off-by: umairofficial &lt;umairhusain1010@gmail.com&gt;
    Signed-off-by: Adi
    Suresh &lt;adsuresh@amazon.com&gt;
    Co-authored-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/opensearch (#2794)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 13:31:08 -0500
    
    
    Bumps commons-io:commons-io from 2.11.0 to 2.12.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/otel-metrics-source (#2798)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 11:44:30 -0500
    
    
    Bumps commons-io:commons-io from 2.11.0 to 2.12.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add containSubstring expression function to check for substring in a string (#2805)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 5 Jun 2023 09:13:28 -0700
    
    
    * Add containSubstring expression function to check for substring in a string
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed comments. Renamed containsSubstring() to contains()
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed comments.
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Updates the current Data Prepper version in the DataPrepperVersion class to 2.3. (#2815)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 5 Jun 2023 10:52:54 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/http-source (#2795)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 10:18:38 -0500
    
    
    Bumps commons-io:commons-io from 2.11.0 to 2.12.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __FIX: bump opensearch-java version to fix unhelpful log message (#2813)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 5 Jun 2023 10:16:44 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Source Codecs | Avro Codec follow-on PR (#2715)__

    [umayr-codes](mailto:130935051+umayr-codes@users.noreply.github.com) - Fri, 2 Jun 2023 17:57:47 -0700
    
    
    * -Support for Source Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Source Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    * -Support for Sink Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    ---------
     Co-authored-by: umairofficial &lt;umairhusain1010@gmail.com&gt;

* __Create OpenSearch source client with auth and lookup version to detect search strategy (#2806)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 2 Jun 2023 19:39:24 -0500
    
    
    Create OpenSearch source client with auth and lookup version to detect search
    strategy
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates the S3 sink to use the AWS Plugin for loading AWS credentials (#2787)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 2 Jun 2023 14:30:47 -0500
    
    
    Updates the S3 sink to use the AWS Plugin for loading AWS credentials. Resolves
    #2767
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update the OpenSearch sink and the OTel Trace Group processor to use the AWS Plugin for loading AWS credentials. Resolves #2765 (#2782)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 2 Jun 2023 12:49:08 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Functionality added for Plaintext,Json and Avro consumers (#2717)__

    [Ajeesh Gopalakrishnakurup](mailto:61016936+ajeeshakd@users.noreply.github.com) - Fri, 2 Jun 2023 10:22:15 -0700
    
    
    * Functionality added for Plaintext,Json and Avro consumers
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Updated the review comments for the PR#2717
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    ---------
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;

* __Support global state items in the in memory source coordination store (#2803)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 2 Jun 2023 10:45:00 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Acquire Global State Item to create partitions, pass globalStateMap to partition creation supplier function (#2785)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 1 Jun 2023 14:51:23 -0500
    
    
    Acquire global state item to create partitions, pass globalStateMap to
    partition creation supplier
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates the S3 source to use the aws-plugin for loading AWS credentials. Resolves #2766. (#2773)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 1 Jun 2023 10:07:39 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Enable TTL for the ddb source coordination store, add option to skip store creation to source coordination config (#2777)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 31 May 2023 18:05:26 -0500
    
    
    Enable TTL for the ddb source coordination store, add option to skip store
    creation to source coordination config
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Update RCF Maven version to reduce noise (#2784)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 31 May 2023 12:22:35 -0700
    
    
    * Move to RCF 3.7 version
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added testcase of outputAfter
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed unnecessary print message
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Update the AWS Plugin to provide a consistent retry policy and backoff strategy for STS credentials. (#2781)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 31 May 2023 13:52:03 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __OpenSearch initialization fix to retry after any exception (#2770)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 31 May 2023 10:27:27 -0700
    
    
    * rebasing
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed to open search init to fail on IllegalArgumentException
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Moves the S3 sink bucket configuration to the root configuration (#2759)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 31 May 2023 10:37:21 -0500
    
    
    Moves the S3 sink bucket configuration up a level to simplify the YAML.
    Addressing PR comments for non-empty validation and to improve tests.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Tail Sampler action in Aggregate processor broken (#2761)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 30 May 2023 13:25:44 -0700
    
    
    * Tail Sampler action in Aggregate processor broken
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed comments. Changed config option errorCondition to condition
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __adding metric and logs in the event an S3 object does not contain records or no records were parsed from the object (#2748)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 30 May 2023 13:34:53 -0500
    
    
    * adding metric and logs in the event an S3 object does not contain records or
    no records were parsed from the object
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;
    
    * addressing build issue
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Fix failing S3ScanObjectWorkerIT tests by creating a source coordinator for these tests to use (#2774)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 30 May 2023 08:31:00 -0700
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add obfuscation processor (#2752)__

    [daixba](mailto:68811299+daixba@users.noreply.github.com) - Fri, 26 May 2023 17:41:17 -0500
    
    
    Add obfuscation processor
     Signed-off-by: Aiden Dai &lt;daixb@amazon.com&gt;

* __Add date_when option to date processor (#2762)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 26 May 2023 15:17:15 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Allow explicit setting of null STS header overrides in AwsCredentialsOptions to make this easier for clients to use. (#2768)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 26 May 2023 13:30:08 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.jetbrains.kotlin:kotlin-stdlib from 1.8.20 to 1.8.21 (#2610)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 26 May 2023 11:51:07 -0500
    
    
    Bumps [org.jetbrains.kotlin:kotlin-stdlib](https://github.com/JetBrains/kotlin)
    from 1.8.20 to 1.8.21.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.8.20...v1.8.21)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.fasterxml.jackson.datatype:jackson-datatype-jdk8 (#2763)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 26 May 2023 11:45:27 -0500
    
    
    Bumps com.fasterxml.jackson.datatype:jackson-datatype-jdk8 from 2.14.2 to
    2.15.1.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jdk8
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Removes JUnit Vintage from the root project (#2742)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 26 May 2023 11:15:18 -0500
    
    
    Removes JUnit Vintage from the root project. Requires projeccts to explicitly
    use JUnit Vintage. Updates some easy tests to JUnit Jupiter.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Rearrange and validate opensearch source configuration (#2746)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 26 May 2023 10:25:08 -0500
    
    
    Rearrange and validate opensearch source configuration
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updated the data prepper log ingestion demo guide documentation (opensearch-project#2756) (#2758)__

    [Thomas Montfort](mailto:61255722+tmonty12@users.noreply.github.com) - Thu, 25 May 2023 14:52:22 -0700
    
    
    Signed-off-by: Thomas Montfort &lt;tjmontfo@amazon.com&gt;
    Co-authored-by: Thomas
    Montfort &lt;tjmontfo@amazon.com&gt;

* __Add basic operator support to arithmetic and string expressions (#2726)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 25 May 2023 14:23:27 -0700
    
    
    * Add basic operator support to arithmetic and string expressions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed grammar to make unary and binary subtract operator to work correctly
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed unused files
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added tests to increase code coverage
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments - Updated expression documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified names in the grammar as per comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __creating boilerplate for OpenSearch Source (#2750)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 25 May 2023 16:10:10 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Adds support for end-to-end acknowledgements in the S3 Sink. Resolves #2732 (#2755)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 25 May 2023 15:04:11 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds the new AWS Extension Plugin for Data Prepper (#2754)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 25 May 2023 13:45:27 -0500
    
    
    Adds the new AWS Extension Plugin for Data Prepper with support for
    standardizing how we load AWS credentials. #2751
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __eliminates in built slash in s3 dlq key and resolves 2581 (#2676)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 25 May 2023 13:08:20 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Use the same Log4j configuration for integration tests as used for unit testing in data-prepper-core. (#2728)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 25 May 2023 12:30:26 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Sets JAVA_OPTS after DATA_PREPPER_JAVA_OPTS to allow Data Prepper admins to override the Log4j configuration file setting. Resolves #2720. (#2721)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 25 May 2023 11:20:46 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the name of the metrics for the new S3 sink to match the names in the S3 source for consistency. Some test clean-up, and updated the README.md with development instructions. (#2741)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 25 May 2023 10:18:19 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Data Prepper Extensions #2636, #2637 (#2730)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 24 May 2023 16:22:53 -0500
    
    
    Data Prepper Extensions #2636, #2637
     Initial work supports the basic model and the ability to inject shared objects
    across plugins.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support to tag events when parse_json fails to parse (#2745)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 24 May 2023 14:21:49 -0700
    
    
    * Add support to tag events when parse_json fails to parse
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump requests in /release/smoke-tests/otel-span-exporter (#2733)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 24 May 2023 16:09:35 -0500
    
    
    Bumps [requests](https://github.com/psf/requests) from 2.26.0 to 2.31.0.
    - [Release notes](https://github.com/psf/requests/releases)
    - [Changelog](https://github.com/psf/requests/blob/main/HISTORY.md)
    - [Commits](https://github.com/psf/requests/compare/v2.26.0...v2.31.0)
    
    ---
    updated-dependencies:
    - dependency-name: requests
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add isIpInCidr function (#2684)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 24 May 2023 10:56:15 -0500
    
    
    * Add isIpInCidr function expression
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Implement InMemorySourceCoordinationStore for use with single node instances of data prepper (#2693)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 23 May 2023 17:34:19 -0500
    
    
    Implement InMemorySourceCoordinationStore for use with single node instances of
    data prepper
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Refactors AbstractIndexManager by extracting template interactions (#2454)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 May 2023 17:33:59 -0500
    
    
    Refactors AbstractIndexManager by removing the template interactions with
    OpenSearch into a new TemplateStrategy interface. This supports #1275 by
    allowing a new strategy for composable index templates later.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for expressions in add_entries processor (#2722)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 23 May 2023 13:31:01 -0700
    
    
    * Rebased to latest. Addressed comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed value option check
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkStyleMain error
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments to make tests simpler
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Log all plugin classes found when DEBUG logging is enabled. (#2729)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 May 2023 13:48:20 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Instrument metrics in LeaseBasedSourceCoordinator (#2723)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 22 May 2023 17:57:48 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Incorporated review comments changes for #1985,#2264.  (#2683)__

    [rajeshLovesToCode](mailto:131366272+rajeshLovesToCode@users.noreply.github.com) - Mon, 22 May 2023 15:38:25 -0500
    
    
    Resolves #1985,#2264
     Signed-off-by: rajeshLovesToCode &lt;rajesh.dharamdasani3021@gmail.com&gt;
     Signed-off-by: rajeshLovesToCode &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    ---------
     Signed-off-by: rajeshLovesToCode &lt;rajesh.dharamdasani3021@gmail.com&gt;
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    Co-authored-by: Taylor Gray
    &lt;tylgry@amazon.com&gt;

* __Use spring-test from testLibs (#2724)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 22 May 2023 12:34:51 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Pipeline creation should succeed even when sink(s) are not ready (#2652)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 22 May 2023 10:03:17 -0700
    
    
    * Rebased to latest
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed code coverage issue
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed code to pass failing tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Use evaluateConditional to fix unit test (#2725)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Sun, 21 May 2023 13:12:54 -0700
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add a processor to parse user agent string (#2696)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 19 May 2023 15:22:58 -0500
    
    
    * Add user_agent processor
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add support for adding metadata entries (#2707)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 19 May 2023 10:38:59 -0700
    
    
    * Add support for adding metadata entries
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added more unit tests for metadata key set
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add support for basic arithmetic and string returning expressions to DataPrepper Expression (#2697)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 19 May 2023 12:02:56 -0500
    
    
    Modified to create GenericExpressionEvaluator that can be used for all types of
    expressions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Rework ddb source coordination store to support multi-source, remove scan for queries on global secondary index (#2710)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 19 May 2023 09:41:15 -0500
    
    
    Rework ddb source coordination store to support multi-source, remove scan for
    queries on global secondary index
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Kafka source multithreading (#2673)__

    [Ajeesh Gopalakrishnakurup](mailto:61016936+ajeeshakd@users.noreply.github.com) - Thu, 18 May 2023 08:43:56 -0700
    
    
    * Added kafka consumer multithreaded logic and it&#39;s junit
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Applied file formatting
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Fixed the build issue
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Incorporated the review comments
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Removed the topic config files
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Incorporated the review comments
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    ---------
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;

* __Add backoff when flushing on add and change condition to &gt;= (#2701)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 18 May 2023 10:14:24 -0500
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __ENH: support gzip compression for armeria sources (#2702)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 17 May 2023 11:24:31 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add support for getMetadata() function in data prepper expressions (#2690)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 17 May 2023 09:09:19 -0700
    
    
    * Addressed comments and rebased to latest
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed unintended file
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Grok processor:  add support to set tags when grok fails to match an event (#2682)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 16 May 2023 13:18:24 -0700
    
    
    * Rebased to latest. Addressed review comments.
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified to accept a list of tags in grok processor
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Signed-off-by: kkondaka
    &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Github-issue#1048 : s3-sink integration test implementation. (#2624)__

    [Deepak Sahu](mailto:deepak.sahu562@gmail.com) - Tue, 16 May 2023 13:41:25 -0500
    
    
    Github-issue#1048 : s3-sink integration test implementation.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    ---------
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;

* __Github-issue#1048 : s3-sink with local-file buffer implementation. (#2645)__

    [Deepak Sahu](mailto:deepak.sahu562@gmail.com) - Tue, 16 May 2023 11:27:38 -0700
    
    
    * GitHub-issue#1048 : Rebase the code from DP main branch.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048 : Incorporated review comments.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048 : Incorporated review comments.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048 : Incorporated review comments.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    ---------
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;

* __S3 scan with source coordination (#2689)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 16 May 2023 12:14:58 -0500
    
    
    Implement S3 Scan using SourceCoordinator
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __addressing copy and paste error (#2678)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Mon, 15 May 2023 13:29:08 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Fix float point number grammar in DataPrepperExpression (#2692)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 15 May 2023 10:59:26 -0700
    
    
    * Fix float point number grammar in DataPrepperExpression
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments.
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix space between function args and add a test (#2688)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 15 May 2023 12:03:40 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Implement dynamo db source coordination store (#2647)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 12 May 2023 16:24:35 -0500
    
    
    Implement dynamo db source coordination store
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Github-issue#1048 : s3-sink with in_memory buffer implementation.  (#2623)__

    [Deepak Sahu](mailto:deepak.sahu562@gmail.com) - Fri, 12 May 2023 14:15:31 -0700
    
    
    * Github-issue#1048 : s3-sink with in-memory buffer implementation.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : s3-sink with in-memory buffer implementation.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : s3-sink with in-memory buffer implementation.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : s3-sink - added JUnit test classes.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : s3-sink - incorporated review comment.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : s3-sink - incorporated review comment.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : s3-sink - local-file buffer implementation.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : s3-sink - in-memory buffer implementation.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : resolved -  checkstyle error.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : incorporated review comment.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 : incorporated review comment.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048 : Incorporated review comments.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048 : Incorporated review comments.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048 : Incorporated review comments.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * GitHub-issue#1048 : Resolved javadoc issues.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    ---------
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;

* __Add hasTags() function to dataprepper expressions (#2680)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 12 May 2023 09:11:02 -0700
    
    
    * Add hasTags() function to dataprepper expressions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed code coverage build failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified to make sure that the arguments passed to hasTags is string literals
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __updating documentation and providing tests which demonstrate json pointers can be used to reference nested elements (#2675)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Fri, 12 May 2023 09:53:51 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Support functions in Data Prepper expressions #2626 (#2644)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 11 May 2023 12:47:03 -0700
    
    
    * Support functions in Data Prepper expressions #2626
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Made ExpressionFunction a interface with provider
    and implementation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added newly created files
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed zero string size issue in LengthExpressionFunction
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified to pass Event to ExpressionFunction
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified to do argument resolution inside the functions instead of the common
    infra
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed support for literal strings in length() function in dataprepper
    expressions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated the document
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Change JsonStringBuilder in JacksonEvent to be non static for ease-of-use (#2666)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 9 May 2023 15:45:20 -0700
    
    
    * Change JsonStringBuilder in JacksonEvent to be non static for ease-of-use
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed to pass code coverage test
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Made JsonStringBuilder constructor private
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Added Kafka-source configurations (#2653)__

    [Ajeesh Gopalakrishnakurup](mailto:61016936+ajeeshakd@users.noreply.github.com) - Tue, 9 May 2023 14:07:44 -0700
    
    
    * Added Kafka-source configurations
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    * Updated build.gradle
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;
    
    ---------
     Signed-off-by: Ajeesh Gopalakrishnakurup &lt;ajeesh.akd@gmail.com&gt;

* __Added 2.2.1 release notes (#2664)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 9 May 2023 12:28:09 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added 2.2.1 change log (#2660)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 9 May 2023 10:59:21 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update to Snakeyaml 2.0 in the Trace Analytics sample app. (#2651)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 8 May 2023 14:38:43 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support reading S3 Event messages from SNS fan-out (#2622)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 8 May 2023 13:27:37 -0500
    
    
    Support reading S3 Event messages which can from SNS to SQS if the message is
    wrapped in the Message key.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump com.palantir.docker from 0.33.0 to 0.35.0 (#2611)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Sat, 6 May 2023 09:09:00 -0500
    
    
    Bumps com.palantir.docker from 0.33.0 to 0.35.0.
    
    ---
    updated-dependencies:
    - dependency-name: com.palantir.docker
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Tagging Events in Data Prepper. Issue #629 (#2629)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 5 May 2023 11:53:24 -0700
    
    
    * Tagging Events in Data Prepper. Issue #629
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Introduced JsonStringBuilder in JacksonEvent to
    return event with additinal info (like tags) as json string
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix OpenSearch Retry mechanism (#2643)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 5 May 2023 13:40:38 -0500
    
    
    Fix OpenSearch Retry mechanism
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Lease based source coordinator (#2460)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 4 May 2023 16:46:27 -0500
    
    
    Implement LeaseBasedSourceCoordinator for source coordination
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Update to use Jetty 11.0.14 in the s3-source project to fix CVE-2023-26048. Also, use wiremock 3.0.0-beta-8, even though this did not update the Jetty version. (#2635)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 4 May 2023 16:33:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the example Spring Boot application to Spring Boot 2.7.11 and Java 11. Should resolve CVE-2023-20863, CVE-2022-45143. (#2634)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 4 May 2023 16:33:16 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates Jackson to 2.15 and Snakeyaml to 2.0. This should resolve security warnings on CVE-2022-1471, though according to the Jackson team, Jackson was already not vulnerable to this CVE. (#2632)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 4 May 2023 10:33:14 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update java-json to address CVE-2022-45688 (#2631)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 4 May 2023 10:18:14 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Followup to 2497. Addressing comments from PR 2497 (#2628)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 3 May 2023 14:51:52 -0700
    
    
    * Followup to 2497. Addressing comments from PR 2497
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed check style failures
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Integration Tests for S3 Source related to Issue #1970,#1971 (#2398)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Wed, 3 May 2023 11:35:44 -0700
    
    
    * Integration Tests for S3 Source related to Issue #1970,#1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Modified S3 Scan in Readme.md file
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Incorporating new yaml changes in IT for Issue#1970,#1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * updated Readme file as per the new yaml configuration #1970
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Incorporated review comments for #1970 and #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    ---------
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    Signed-off-by: Uday
    Chintala &lt;udaych20@gmail.com&gt;

* __Add when conditions to commonly used processors (#2619)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 3 May 2023 10:50:02 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Log clear messages when OpenSearch Sink fails to push. Modify retries to be iterative instead of recursive (#2605)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 2 May 2023 16:05:27 -0700
    
    
    * Log clear messages when OpenSearch Sink fails to push. Modify retries to be
    iterative instead of recursive
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comment. Fixed off-by-one error in the retry count
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed code to address failing integration tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed code to address failing tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add Tail Sampler action to aggregate processor (#2497)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 2 May 2023 15:22:18 -0700
    
    
    * Add Tail Sampler action to aggregate processor
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added documentation and made change to cleanup state after wait period
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Added AggregateActionOutput class
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Introduced customShouldConclude check for adding custom conclusion checks
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add AggregateActionOutput
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix javadoc errors
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __S3 Scan Functionality including S3 Select feature Issue#1970 and #1971 (#2389)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Tue, 2 May 2023 10:32:56 -0500
    
    
    S3 Scan Functionality including S3 Select feature Issue#1970 and #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    ---------
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;

* __Adds Krishna (kkondaka) as a maintainer. (#2617)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 1 May 2023 22:12:00 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Increase timeout values in in-memory source PipelinesWithAcksIT to fix occasional test failures (#2606)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 1 May 2023 12:39:05 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Consolidates use of OpenSearch clients using the Gradle version catalog. Removes some unnecessary Gradle configurations. (#2569)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 1 May 2023 11:11:13 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#2603)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 28 Apr 2023 15:07:04 -0500
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.14.2 to 1.14.4.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.2...byte-buddy-1.14.4)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.mgd.core.gradle.s3 from 1.1.4 to 1.2.1 (#2218)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 28 Apr 2023 09:09:47 -0500
    
    
    Bumps com.mgd.core.gradle.s3 from 1.1.4 to 1.2.1.
    
    ---
    updated-dependencies:
    - dependency-name: com.mgd.core.gradle.s3
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy-agent in /data-prepper-plugins/opensearch (#2432)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 28 Apr 2023 09:08:45 -0500
    
    
    Bumps [net.bytebuddy:byte-buddy-agent](https://github.com/raphw/byte-buddy)
    from 1.14.2 to 1.14.3.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.2...byte-buddy-1.14.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Initial commit for the S3 Sink #1048 (#2585)__

    [Deepak Sahu](mailto:deepak.sahu562@gmail.com) - Fri, 28 Apr 2023 08:54:19 -0500
    
    
    Initial commit for the S3 Sink #1048
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    ---------
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;

* __Github-issue#1048 : s3 object index. (#2586)__

    [Deepak Sahu](mailto:deepak.sahu562@gmail.com) - Thu, 27 Apr 2023 11:22:46 -0500
    
    
    * Github-issue#1048 : s3 object index.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 Incorporate review comments.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    * Github-issue#1048 Incorporate review comments.
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;
    
    ---------
     Signed-off-by: Deepak Sahu &lt;deepak.sahu562@gmail.com&gt;

* __Add null check for bulkRetryCountMap in opensearch sink (#2600)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 27 Apr 2023 10:12:24 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates the instructions for the log-ingestion example with better copy-and-paste support, explicit Data Prepper 2 usage, and removes Data Prepper 1.x. (#2591)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 25 Apr 2023 16:13:13 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.jetbrains.kotlin:kotlin-stdlib from 1.7.10 to 1.8.20 (#2434)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 24 Apr 2023 19:56:33 -0500
    
    
    Bumps [org.jetbrains.kotlin:kotlin-stdlib](https://github.com/JetBrains/kotlin)
    from 1.7.10 to 1.8.20.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-jpl in /data-prepper-core (#2430)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 24 Apr 2023 19:55:44 -0500
    
    
    Bumps
    [org.apache.logging.log4j:log4j-jpl](https://github.com/apache/logging-log4j2)
    from 2.17.0 to 2.20.0.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    - [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/CHANGELOG.adoc)
    
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.17.0...rel/2.20.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-jpl
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Avro codecs (#2527)__

    [umayr-codes](mailto:130935051+umayr-codes@users.noreply.github.com) - Mon, 24 Apr 2023 13:39:17 -0500
    
    
    -Support for Source Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    ---------
     Co-authored-by: umairofficial &lt;umairhusain1010@gmail.com&gt;

* __Adds object filter patterns for core peer-forwarder&#39;s Java deserialization to put restrictions on the maximum array length and the maximum object depth. (#2576)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 24 Apr 2023 09:46:57 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates main branch to 2.3.0-SNAPSHOT (#2578)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 24 Apr 2023 09:18:12 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Apply exponential backoff for exceptions when reading from S3 (#2580)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 24 Apr 2023 09:16:53 -0500
    
    
    Apply exponential backoff for exceptions when reading from S3 in the S3 source.
    Apply exponential backoff for SQS DeleteMessage requests as well. #2568
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Increase the backoff delays in the S3 source polling thread to run in the range of 20 seconds to 5 minutes. The current behavior still produces too many logs. Fixes #2568. (#2574)__

    [David Venable](mailto:dlv@amazon.com) - Sat, 22 Apr 2023 16:07:32 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Log full errors when the OpenSearch sink fails to start (#2565)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 20 Apr 2023 19:28:57 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Baselines the MAINTAINERS.md and CODEOWNERS file. Resolves #2275 (#2564)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 20 Apr 2023 13:45:33 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added 2.2 release notes (#2560)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 20 Apr 2023 12:24:00 -0500
    
    
    * Added 2.2 release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added 2.2 change log (#2561)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 20 Apr 2023 12:03:57 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated plugin names for otel plugins (#2526)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 20 Apr 2023 10:54:43 -0500
    
    
    * Updated plugin names for otel plugins
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update dependency versions to fix CVEs (#2546)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 20 Apr 2023 09:45:59 -0500
    
    
    * Update dependency versions to fix CVEs
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Opensearch serverless change (#2542)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 20 Apr 2023 09:28:52 -0500
    
    
    * Remove aws_serverless option
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix STS logging (#2552)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 20 Apr 2023 09:25:41 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add Metrics to end-to-end acknowledgement core framework (#2506)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 19 Apr 2023 23:33:39 -0500
    
    
    * Add Metrics to end-to-end acknowledgement core framework
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Added routes as an alias to route (#2535)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 19 Apr 2023 21:00:57 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Start directory for built-in grok patterns for the grok processor, as well as some common patterns (#2514)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 19 Apr 2023 19:22:27 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Adds branching on supported configurations for ScanRange (#2539)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 19 Apr 2023 18:26:19 -0500
    
    
    * Branch on batching based on support
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Small formatting fixes
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Bump io.micrometer:micrometer-bom from 1.9.4 to 1.10.5 (#2433)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Apr 2023 17:13:07 -0500
    
    
    Bumps
    [io.micrometer:micrometer-bom](https://github.com/micrometer-metrics/micrometer)
    from 1.9.4 to 1.10.5.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.9.4...v1.10.5)
    
    
    ---
    updated-dependencies:
    - dependency-name: io.micrometer:micrometer-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated package name for otel logs source (#2518)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 19 Apr 2023 16:59:21 -0500
    
    
    * Updated package name for otel logs source
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add documentation for list_to_map processor (#2474)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 19 Apr 2023 16:14:45 -0500
    
    
    * Add docs for list_to_map processor
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Shutdown Data Prepper when any pipeline fails by default, but allow configuration so that only it can remain running as long as one pipeline is still running. #2441 (#2524)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 19 Apr 2023 15:44:31 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Document acknowledgements option to s3 source (#2530)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 19 Apr 2023 13:55:49 -0500
    
    
    Modified to use only one spelling - acknowledgment
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __address build failure in 2511 (#2534)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 19 Apr 2023 12:42:46 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __addressing metrics publishing bug for DLQ (#2523)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 19 Apr 2023 12:05:47 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __[2511] adding support for document_root_key (#2516)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 19 Apr 2023 11:02:48 -0500
    
    
    * [2511] adding support for document_root_key
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __adding wrapper dlq object, dlq file xtensions and improving dlq README (#2509)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 19 Apr 2023 10:24:35 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Shut down Data Prepper after all pipelines have shutdown. Also close the Application Context so that it can close other dependencies. #2441 (#2495)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 18 Apr 2023 17:04:24 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __-Support for Source Codecs (#2519)__

    [umayr-codes](mailto:130935051+umayr-codes@users.noreply.github.com) - Tue, 18 Apr 2023 12:02:01 -0500
    
    
    -Support for Source Codecs
    Signed-off-by: umairofficial
    &lt;umairhusain1010@gmail.com&gt;
    
    ---------
     Co-authored-by: umairofficial &lt;umairhusain1010@gmail.com&gt;

* __Generated THIRD-PARTY file for bc75494 (#2517)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 18 Apr 2023 09:28:46 -0500
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: asifsmohammed
    &lt;asifsmohammed@users.noreply.github.com&gt;

* __Change the behavior of the CSV codec in the S3 source to fail when it is unable to parse CSV rows. Resolves #2512. (#2513)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 17 Apr 2023 21:10:26 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Allow deprecated plugin names  (#2508)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 17 Apr 2023 21:10:05 -0500
    
    
    Allow deprecated plugin names and update otel plugin names
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __removing event handle from dlq object (#2510)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Mon, 17 Apr 2023 17:59:24 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __end_to_end_acknowledgements option name change (#2486)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 17 Apr 2023 15:51:31 -0500
    
    
    Rename end_to_end_acknowledgements to acknowledgements and support alias
    acknowledgments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Update RCF  maven repository to latest (#2507)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 17 Apr 2023 14:26:12 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __FileSink updates: Remove call to initialize(), adds FileSinkConfig, test updates (#2475)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 17 Apr 2023 09:42:12 -0500
    
    
    Removes an unnecessary call to initialize() in the FileSink constructor.
    Updates FileSink to use a FileSinkConfig. Updates FileSink tests.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Properly shutdown the log_generator plugin correctly. It was preventing Data Prepper from shutting down. (#2494)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 14 Apr 2023 13:25:36 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding iam role arn validation to s3 source and open search sink configs (#2472)__

    [roshan-dongre](mailto:roshan-dongre@users.noreply.github.com) - Fri, 14 Apr 2023 09:49:57 -0500
    
    
    * adding iam role arn validation to s3 source and open search sink configs
     Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;

* __Move backoff strategy to BufferAccumulator (#2481)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 13 Apr 2023 20:21:51 -0500
    
    
    * Move backoff strategy to BufferAccumulator
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix metric reporting for batching
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix unit test
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix warning message when Open Search Sink fails to initialize (#2482)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 13 Apr 2023 17:11:11 -0500
    
    
    Fix warning message when Open Search Sink fails to initialize
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Fixes an issue with the end-to-end acknowledgements where the the scheduled monitor thread holds a user thread and prevents Data Prepper from shutting down correctly. The monitor now runs in a dedicated daemon thread and the callback methods are submitted to a distinct executor service with a lower bound of available threads. Includes various test improvements as well. (#2483)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 13 Apr 2023 15:23:47 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add End to End acknowledgement support for S3 source (#2465)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 12 Apr 2023 18:30:13 -0500
    
    
    Add End to End acknowledgement support for S3 source
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix cannot start databaseService.py with Trace Analytics Sample App (#2477) (#2478)__

    [Toby Lam](mailto:me@livekn.com) - Wed, 12 Apr 2023 18:26:05 -0500
    
    
    Signed-off-by: Toby Lam &lt;me@livekn.com&gt;

* __OpenSearchSink: add support for sending end-to-end acknowledgements (#2458)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 12 Apr 2023 16:33:59 -0500
    
    
    OpenSearchSink: add support for sending end-to-end acknowledgements
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __ENH: batching metrics with same tags in EMFLoggingMeterRegistry (#2467)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 12 Apr 2023 15:32:01 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add format string option to add_entries processor (#2464)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 12 Apr 2023 14:08:13 -0500
    
    
    * Add format string option to add_entries processor
    * Update config validations
    * Update README
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix: CVE-2023-20861 (#2473)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 11 Apr 2023 18:37:35 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Retry flushing of buffer on buffer TimeoutException (#2470)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 11 Apr 2023 14:24:44 -0500
    
    
    Retry flushing of buffer on buffer TimeoutException
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Address review comments from PR 2436 (#2459)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 11 Apr 2023 12:11:09 -0500
    
    
    Address review comments from PR 2436
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Added integration tests for end-to-end acknowledgements (#2442)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 10 Apr 2023 08:06:34 -0500
    
    
    * Changed the source config to end-to-end-acknowledgements and key name
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed check style errors
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add list_to_map processor (#2453)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 7 Apr 2023 11:56:03 -0500
    
    
    * Add list_to_map processor plugin
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add support for acknowledgements to source, process worker and router strategy (#2436)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 6 Apr 2023 16:24:42 -0500
    
    
    Add support for acknowledgements to source, process worker and router strategy
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Support running tests on OpenSearch 2.6.0 (#2455)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 6 Apr 2023 15:22:35 -0500
    
    
    Fixes an issue with the tests that prevented them from running against
    OpenSearch 2.6.0. This also tries to get ahead of other possible new system
    indexes. Adds more OpenSearch versions to the list of versions to test.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __S3 select dev (#2447)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Thu, 6 Apr 2023 14:04:44 -0500
    
    
    Incorporated new yaml changes for s3 select #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;

* __Updated S3 worker logging (#2438)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 5 Apr 2023 16:39:34 -0500
    
    
    * Updated S3 worker logging
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __adding data-time patterns to key-path-prefix and creating readme for s3 dlq (#2451)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 5 Apr 2023 16:28:58 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Fix imports for AppendAggregateAction (#2452)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 5 Apr 2023 12:55:05 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Count SQS message delete failures in the S3 source (#2450)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Apr 2023 12:29:51 -0500
    
    
    Provide a new metric for when the S3 source is unable to delete an SQS message.
    Resolves #2449
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __merge aggregation (#2230)__

    [Krishnanand Singh](mailto:krishnanand91@gmail.com) - Wed, 5 Apr 2023 10:32:51 -0500
    
    
    Add new aggregate action to create aggregated Event by appending values over
    time
     Signed-off-by: KrishnanandSingh &lt;krishnanand_singh@cargill.com&gt;
    
    Signed-off-by: Krishnanand Singh &lt;krishnanand91@gmail.com&gt;

* __adding support for dlq plugins in opensearch (#2429)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 5 Apr 2023 09:28:32 -0500
    
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Refactor source coordination to split interfaces between SourceCoordinator and SourceCoordinationStore (#2444)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 5 Apr 2023 09:24:33 -0500
    
    
    Refactor source coordination to split interfaces between SourceCoordinator and
    SourceCoordinationStore
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Logging improvements to the csv codec, role arn, and span messages (#2448)__

    [roshan-dongre](mailto:roshan-dongre@users.noreply.github.com) - Wed, 5 Apr 2023 08:26:47 -0500
    
    
    * Logging improvements to the csv codec, role arn, and span messages
     Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;
    
    * improving the log messaging of the otel trace raw processor
     Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;
    
    ---------
     Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;

* __implementing s3 dlq writer (#2419)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 4 Apr 2023 09:32:18 -0500
    
    
    * implementing s3 dlq writer
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Addresses S3 Select YAML code review suggestions (#2439)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 3 Apr 2023 11:35:32 -0500
    
    
    * Rebased. Fixed test cases and addressed review comments (#2399)
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;
     Support for Snappy PR# 2420 (#2421)
     Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     MAINT: replace grok debugger (#2425)
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
     Wire in source_coordination config and SourceCoordinator interface to Source
    plugins (#2395)
     Wire in source_coordination config and SourceCoordinator inteface to Source
    plugins
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
     Add EventFactory and AcknowledgementSetManager instantiations and make them
    available  (#2426)
     Add EventFactory and AcknowledgementSetManager instantiations and make them
    available
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Updated SQS logging (#2417)
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Address code review comment of changing yaml parameters to expression,
    input_serialization
    PR#1971
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
    Signed-off-by: Ashok Telukuntla
    &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
    Signed-off-by: Ashok Telukuntla
    &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     Fix test broken by rename
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix spacing
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    Co-authored-by:
    kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Updated SQS logging (#2417)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sat, 1 Apr 2023 22:20:08 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add EventFactory and AcknowledgementSetManager instantiations and make them available  (#2426)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 31 Mar 2023 15:04:33 -0500
    
    
    Add EventFactory and AcknowledgementSetManager instantiations and make them
    available
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Wire in source_coordination config and SourceCoordinator interface to Source plugins (#2395)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 31 Mar 2023 12:45:43 -0500
    
    
    Wire in source_coordination config and SourceCoordinator inteface to Source
    plugins
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __MAINT: replace grok debugger (#2425)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 30 Mar 2023 23:24:39 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Support for Snappy PR# 2420 (#2421)__

    [Ashok Telukuntla](mailto:55903152+ashoktelukuntla@users.noreply.github.com) - Thu, 30 Mar 2023 16:13:54 -0500
    
    
    Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;

* __Rebased. Fixed test cases and addressed review comments (#2399)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 30 Mar 2023 15:05:35 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __S3 select dev (#2353)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Thu, 30 Mar 2023 09:44:20 -0500
    
    
    * Support S3 Select when loading objects from S3 via the S3 source #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Support S3 Select when loading objects from S3 via the S3 source #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Incorporated review comments for Issue #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Indentation issue fix.
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * EventMetadataModifier used in S3 Select, Review Comment Changes
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Modified S3 Select Readme.md file
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Incorporated S3 Select review changes #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Removed as this is related to s3 select integration tests
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Incorporated review comments for Issue#1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    ---------
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;

* __updating opensearch sink constructor to support loading dlq plugins (#2415)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 29 Mar 2023 14:57:14 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __adding dlqObject and dlqWriter interface to (#2392)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 29 Mar 2023 14:56:42 -0500
    
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Add acknowledgementSet and acknowledgementSetManager - End-to-End Ack Support (#2394)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 29 Mar 2023 12:31:50 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __ENH: opensearch sink AOSS support (#2385)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 28 Mar 2023 09:14:18 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Updated the Developer Guide to point users to the Data Prepper documentation on opensearch.org. (#2367)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 28 Mar 2023 09:04:13 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/otel-metrics-source (#2336)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Mar 2023 16:35:35 -0500
    
    
    Bumps commons-io:commons-io from 2.10.0 to 2.11.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#2407)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Mar 2023 13:49:01 -0500
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.12.22 to 1.14.2.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.22...byte-buddy-1.14.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/otel-trace-source (#2335)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Mar 2023 13:45:58 -0500
    
    
    Bumps commons-io:commons-io from 2.10.0 to 2.11.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-expression (#2334)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Mar 2023 13:45:02 -0500
    
    
    Bumps org.apache.logging.log4j:log4j-bom from 2.19.0 to 2.20.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Use Gradle version cataglos for Slf4j, Spring, Hamcrest, and Awaitility. Finished some missing JUnit dependencies. Removed some uses of Hamcrest and Awaitility which were not needed since these are part of the root project. (#2382)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Mar 2023 12:30:28 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixing java doc warnings (#2396)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 23 Mar 2023 12:49:41 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __improving logging in OSDP (#2391)__

    [roshan-dongre](mailto:roshan-dongre@users.noreply.github.com) - Tue, 21 Mar 2023 15:31:24 -0500
    
    
    Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;

* __Add Event Factory and generic event builder infrastructure (#2378)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 20 Mar 2023 19:13:31 -0500
    
    
    Add Event Factory and generic event builder infrastructure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __FIX: isolated service node (#2384)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 20 Mar 2023 14:22:52 -0500
    
    
    * FIX: service-map isolated service
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Remove the old peer-forwarder build.gradle file. It is not part of the build and just needs to be deleted. (#2386)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 20 Mar 2023 14:20:49 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the Java serialization allowlist to have specific classes for JsonNode. Added new tests to verify that this new allowlist does not interfere with some expected Event patterns. (#2376)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Mar 2023 15:18:11 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __OpenSearch Sink should make the number of retries configurable - Issue #2291 (#2339)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 16 Mar 2023 16:54:20 -0500
    
    
    OpenSearch Sink should make the number of retries configurable - Issue #2291
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Added change log for 2.1.1 (#2377)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 16 Mar 2023 11:27:35 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added release notes for 2.1.1 (#2375)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 16 Mar 2023 11:27:19 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Use Netty version supplied by dependencies (#2031)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Mar 2023 16:21:11 -0500
    
    
    * Removed old constraints on Netty which were resulting in pulling in older
    version of Netty. Our dependencies (Armeria and AWS SDK Java client) are
    pulling in newer versions so these old configurations are not necessary
    anymore.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix: Fixed IllegalArgumentException in PluginMetrics  (#2369)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 9 Mar 2023 12:54:31 -0600
    
    
    * Fix: Fixed IllegalArgumentException in PluginMetrics caused by pipeline name
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __FIX: traceState not required in Link (#2363)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 3 Mar 2023 16:55:37 -0600
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Added 2.1 change log (#2360)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 2 Mar 2023 15:20:55 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add release notes for 2.1.0 (#2354)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 2 Mar 2023 13:52:16 -0600
    
    
    * Add release notes for 2.1.0
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added backoff for SQS to reduce logging (#2326)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 2 Mar 2023 12:14:20 -0600
    
    
    

* __Removed default service endpoint for otel sources (#2346)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 2 Mar 2023 11:38:54 -0600
    
    
    * Removed default service endpoint for otel sources
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Replace the java.util.* allowed pattern in the ObjectInputFilter with specific classes which are commonly used in Data Prepper. (#2351)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 2 Mar 2023 09:53:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Explicitly set the GitHub Actions thumbprint to resolve #2343. Updated the AWS CDK as well. (#2345)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 1 Mar 2023 15:40:05 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated version to 2.2 on main (#2342)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 1 Mar 2023 13:27:15 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Generated THIRD-PARTY file for 062ae95 (#2344)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 1 Mar 2023 13:26:20 -0600
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: asifsmohammed
    &lt;asifsmohammed@users.noreply.github.com&gt;

* __Use an ObjectInputFilter to serialize allow deserialization of only certain objects in peer-to-peer connections. Additionally, it refactors some application configurations to improve integration testing. Fixes #2310. (#2311)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 1 Mar 2023 10:06:52 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-core (#2333)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Mar 2023 09:47:42 -0600
    
    
    Bumps org.apache.logging.log4j:log4j-bom from 2.19.0 to 2.20.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core from 3.21.0 to 3.24.2 (#2331)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Mar 2023 09:47:06 -0600
    
    
    Bumps org.assertj:assertj-core from 3.21.0 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.junit.jupiter:junit-jupiter-api from 5.9.0 to 5.9.2 (#2332)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Mar 2023 09:46:26 -0600
    
    
    Bumps
    [org.junit.jupiter:junit-jupiter-api](https://github.com/junit-team/junit5)
    from 5.9.0 to 5.9.2.
    - [Release notes](https://github.com/junit-team/junit5/releases)
    - [Commits](https://github.com/junit-team/junit5/compare/r5.9.0...r5.9.2)
    
    ---
    updated-dependencies:
    - dependency-name: org.junit.jupiter:junit-jupiter-api
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Support for path in OTel sources (#2297)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 28 Feb 2023 19:57:38 -0600
    
    
    Initial commit for OTel trace path changes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;

* __Fix grok processor to not create a new record (#2325)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 28 Feb 2023 19:56:04 -0600
    
    
    * Fix grok processor to not create a new record
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkStyleMain  failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump org.springframework:spring-context in /data-prepper-expression (#2223)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Feb 2023 18:27:04 -0600
    
    
    Bumps
    [org.springframework:spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.springframework:spring-context in /data-prepper-core (#2214)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Feb 2023 16:52:20 -0600
    
    
    Bumps
    [org.springframework:spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump io.spring.dependency-management from 1.0.11.RELEASE to 1.1.0 (#2106)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Feb 2023 16:48:25 -0600
    
    
    Bumps io.spring.dependency-management from 1.0.11.RELEASE to 1.1.0.
    
    ---
    updated-dependencies:
    - dependency-name: io.spring.dependency-management
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.fasterxml.jackson.datatype:jackson-datatype-jdk8 (#2217)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Feb 2023 16:47:36 -0600
    
    
    Bumps com.fasterxml.jackson.datatype:jackson-datatype-jdk8 from 2.14.1 to
    2.14.2.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jdk8
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added path support for HTTP source (#2277)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 27 Feb 2023 15:26:58 -0600
    
    
    * Added path support for HTTP source
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated Data Prepper base image for e2e tests (#2269)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 27 Feb 2023 15:08:13 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Loop until interrupted in SqsWorker (#2306)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 24 Feb 2023 00:10:26 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Performance Improvement - Avoid event copying for the first sub-pipeline (#2290)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 23 Feb 2023 22:08:47 -0600
    
    
    Avoid event copying for the first sub-pipeline
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump slf4j-simple from 1.7.36 to 2.0.6 (#2112)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 22 Feb 2023 10:39:21 -0600
    
    
    Bumps [slf4j-simple](https://github.com/qos-ch/slf4j) from 1.7.36 to 2.0.6.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.36...v_2.0.6)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-simple
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Upgrades opentelemetry dependencies (#2288)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Tue, 21 Feb 2023 21:16:58 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Fix error message printed when OpenSearch Sink fails to initialize (#2296)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 21 Feb 2023 14:38:36 -0600
    
    
    Fix error message printed when OpenSearch Sink fails to initialize
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __adding backwards compatibility support in versioning check (#2295)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 21 Feb 2023 14:13:50 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __[2263] adding versioning property to pipeline yaml configuration (#2292)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 21 Feb 2023 09:08:39 -0600
    
    
    * [2263] adding versioning property to pipeline yaml configuration
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;
    
    * adding hashCode support and providing documentation for updating the version
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Updates to opensearch-java 2.2.0 (#2287)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 20 Feb 2023 15:18:55 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Replace an unnecessary builder pattern in trace raw processor with a factory method. This is a little simpler code and it reduces an object creation. (#2271)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Feb 2023 14:34:21 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Create DataPrepper server after pipeline initialization (#2284)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 17 Feb 2023 11:12:08 -0600
    
    
    * Create DataPrepper HttpServer after pipeline initialization
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove injection provider implementation
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Refactor PeerForwarderReceiveBuffer to extend AbstractBuffer to pick up metric logic (#2286)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 17 Feb 2023 10:48:29 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Add more metrics to OTEL metrics source (#2283)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 17 Feb 2023 10:24:30 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Make batching queue depth configurable, fill in some missing documentation around batching (#2278)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 17 Feb 2023 10:24:06 -0600
    
    
    * Make batching queue depth configurable, fill in some missing documentation
    around batching
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix validation message
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Actually fix validation message
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Update to opensearch 1.3.8. Resolves #2192, #2193 (#2285)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Feb 2023 15:56:36 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __OTel Trace Source: Logging and integration tests (#2262)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Feb 2023 11:12:26 -0600
    
    
    Modified some of the logging for the OTel Trace source. Added integration tests
    which actually verify that gRPC requests operate as expected. Print out SLF4J
    logs from tests to stdout to help with debugging.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Created untriaged issue workflow. (#2276)__

    [Daniel (dB.) Doubrovkine](mailto:dblock@amazon.com) - Wed, 15 Feb 2023 13:15:53 -0600
    
    
    Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Long int fix (#2265)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 14 Feb 2023 16:13:20 -0600
    
    
    * OpenSearchSink should close open files before retrying initialization
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Minor fixes to data prepper plugins documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix Long Integer comparisons
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added more tests for long integer comparisons
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __CVE: updated guava version (#2254)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 14 Feb 2023 15:08:18 -0600
    
    
    * CVE: updated guava version
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;
    
    * Updated guava to use catalog
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;

* __Update to Armeria 1.22.1 which fixes #2206. (#2274)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Feb 2023 13:36:21 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix: e2e tests and added documentation (#2267)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 13 Feb 2023 14:40:27 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Removes some unnecessary methods from ServiceMapRelationship and adds additional tests to verify behavior. (#2200)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Feb 2023 13:45:31 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Created a metric for the overall JVM memory usage - both heap and non-heap. (#2266)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Feb 2023 13:24:48 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes index_type convention (#2261)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Fri, 10 Feb 2023 14:47:20 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Minor fixes to data prepper plugins documentation (#2260)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 10 Feb 2023 13:39:12 -0600
    
    
    * OpenSearchSink should close open files before retrying initialization
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Minor fixes to data prepper plugins documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __ENH: peer forwarding codec and model (#2256)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 10 Feb 2023 09:19:51 -0600
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Use the Gradle version catalog for common software versions to the extent that it can replace the versionMap. (#2253)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 9 Feb 2023 13:49:11 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __OpenSearchSink should close open files before retrying initialization (#2255)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 9 Feb 2023 11:15:19 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Added trace peer forwarder doc (#2245)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 8 Feb 2023 18:10:26 -0600
    
    
    * Added trace peer forwarder doc
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactor to remove stream in RemotePeerForwarder as micro optimization (#2250)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 8 Feb 2023 18:09:17 -0600
    
    
    * Refactor to remove stream in RemotePeerForwarder as micro optimization
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove unused import
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Bump org.assertj:assertj-core (#2221)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 16:01:58 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump certifi in /release/smoke-tests/otel-span-exporter (#2063)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 15:51:47 -0600
    
    
    Bumps [certifi](https://github.com/certifi/python-certifi) from 2021.10.8 to
    2022.12.7.
    - [Release notes](https://github.com/certifi/python-certifi/releases)
    -
    [Commits](https://github.com/certifi/python-certifi/compare/2021.10.08...2022.12.07)
    
    
    ---
    updated-dependencies:
    - dependency-name: certifi
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.google.guava:guava in /data-prepper-plugins/aggregate-processor (#2219)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 15:50:29 -0600
    
    
    Bumps [com.google.guava:guava](https://github.com/google/guava) from 10.0.1 to
    23.0.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/compare/v10.0.1...v23.0)
    
    ---
    updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#2226)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 15:42:11 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.springframework:spring-test in /data-prepper-expression (#2224)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 14:53:37 -0600
    
    
    Bumps
    [org.springframework:spring-test](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Moved Random Cut Forest libraries to the Anomaly Detection processor. (#2252)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 8 Feb 2023 11:44:14 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Sink doInitialize() should be able throw exception to stop execution (#2249)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 8 Feb 2023 09:53:30 -0600
    
    
    * Sink doInitialize() should be able throw exception to stop execution
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Sink doInitialize() should be able throw exception to stop execution - fixed
    code coverage failures
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Consistent AWS Pipeline Configurations - #2184 (#2248)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 7 Feb 2023 15:58:59 -0600
    
    
    * Consistent AWS Pipeline Configurations - #2184
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Consistent AWS Pipeline Configurations - updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Consistent AWS Pipeline Configurations  - fixed documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Made some changes to the :release:archives:linux:linuxTar task to reduce dependencies and get a better build time when building multiple times. (#2240)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 7 Feb 2023 14:30:30 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added SDK client metrics and metric filters (#2232)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 7 Feb 2023 14:04:10 -0600
    
    
    * Added SDK client metrics and metric filters
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Improved how the PipelineConnector copies events by using a new static method to perform the copy. For JacksonEvent, this performs a deepCopy() which appears to be more efficient than the old process. (#2241)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 7 Feb 2023 10:06:18 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use snake case for all configurations #2203 (#2243)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 6 Feb 2023 17:30:14 -0600
    
    
    * Use snake case for all configurations #2203
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add back LinkedBlockingQueue::poll to avoid busy wait (#2246)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 6 Feb 2023 14:12:32 -0600
    
    
    * Add back LinkedBlockingQueue::poll to avoid busy wait
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Drop poll timeout when delay=0 to 5 millis
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Use Armeria&#39;s BlockingTaskExecutor to configure thread names (#2235)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Feb 2023 09:32:47 -0600
    
    
    Use Armeria&#39;s BlockingTaskExecutor to configure thread name
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update default values in buffer documentation (#2233)__

    [JannikBrand](mailto:jannik.brand@sap.com) - Sat, 4 Feb 2023 14:04:56 -0600
    
    
    

* __The DefaultEventMetadata almost always creates an empty attributes. This uses the static ImmutableMap.of() which uses a shared instance underneath so that new objects are not created each time. (#2239)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 3 Feb 2023 13:21:31 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __The RecordMetadata default metadata is used by most records and it creates a new instance each time. This is an immutable class, so just return the same metadata instance for all. (#2238)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 3 Feb 2023 12:47:25 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Abstract sink should not create a new retry thread everytime initialization fails (#2231)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 2 Feb 2023 10:25:41 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add Support for retry when Sink fails to initialize  (#2198)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 1 Feb 2023 16:39:50 -0600
    
    
    * Remove opensearch availability dependence - Issue #936
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - issue #936
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - added number of retries
    to dataprepper execute
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Addressed review
    comments. Fixed test failures
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Addressed review
    comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Fixes for failing tests
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Fixes for failing tests
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Fixes for code coverage
    failures
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Modified to check for
    retryable exception
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - addressed review
    comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __DynamicIndexTemplate can cause NPE that shuts down pipeline - Issue #2210 (#2211)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 1 Feb 2023 15:15:01 -0600
    
    
    * DynamicIndexTemplate can cause NPE that shuts down pipeline - Issue #2210
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * DynamicIndexTemplate can cause NPE that shuts down pipeline - addressed
    review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * DynamicIndexTemplate can cause NPE that shuts down pipeline - addressed
    review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * DynamicIndexTemplate can cause NPE that shuts down pipeline - fixed build
    failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add missing metrics for Opensearch Sink #2168 (#2205)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 1 Feb 2023 15:10:21 -0600
    
    
    * Add missing metrics for Opensearch Sink #2168
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- updated the documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- modified documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- fixed build failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Perform serialization sequentially, block for each batch of forwards (#2228)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 1 Feb 2023 11:04:43 -0600
    
    
    * Perform serialization sequentially, block for each batch of forwards
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add test for mixed future results
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Rename forwardRecords to forwardBatchedRecords to avoid naming duplication
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add missing log statement
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Removed getObjectAttributes API call to check object size (#2179)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 1 Feb 2023 10:16:49 -0600
    
    
    * Removed getObjectAttributes API call to check object size
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump org.springframework:spring-core in /data-prepper-expression (#2222)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 21:00:33 -0600
    
    
    Bumps
    [org.springframework:spring-core](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#2225)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:46:44 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/http-source (#2227)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:46:18 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-trace-source (#2212)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:45:50 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.springframework:spring-test in /data-prepper-core (#2215)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:41:19 -0600
    
    
    Bumps
    [org.springframework:spring-test](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.springframework:spring-core in /data-prepper-core (#2216)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:40:24 -0600
    
    
    Bumps
    [org.springframework:spring-core](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Implement batching for peer forwarder request documents (#2197)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Tue, 31 Jan 2023 16:42:25 -0600
    
    
    * Implement batching for peer forwarder request documents
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add configurable forwarding_batch_timeout for low-traffic scenarios
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Slight refactors and add unit tests for batching
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Increase YAML deserialization size
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Refactor for clarity and flush all available batches on each iteration
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix typo in FORWARDING
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Use getOrDefault when checking last flushed time
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Integration test for a pipeline with multiple process workers (#2017)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 31 Jan 2023 14:29:38 -0600
    
    
    Created an integration test for a pipeline with multiple process workers.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add clientTimeout to peer forwarder configuration, optimize CPF seria… (#2190)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Tue, 31 Jan 2023 11:53:15 -0600
    
    
    * Add clientTimeout to peer forwarder configuration, optimize CPF serialization
    
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix miss in rename and use lower write timeout rather than higher request
    timeout
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add missing private
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Bump hibernate-validator in /data-prepper-core (#1854)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Jan 2023 10:29:58 -0600
    
    
    Bumps [hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 7.0.5.Final to 8.0.0.Final.
    - [Release notes](https://github.com/hibernate/hibernate-validator/releases)
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/main/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/7.0.5.Final...8.0.0.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jackson-datatype-jdk8 from 2.13.3 to 2.14.1 in /data-prepper-api (#2101)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Jan 2023 10:25:40 -0600
    
    
    Bumps jackson-datatype-jdk8 from 2.13.3 to 2.14.1.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jdk8
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Support OpenTelementry Logs (#1372)__

    [kmssap](mailto:100778246+kmssap@users.noreply.github.com) - Sat, 28 Jan 2023 09:21:54 -0600
    
    
    * Introduce Support for OpenTelemetry Logs
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix Unused Import Checkstyle Error
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Refactor commit for new architecture
    
    - Move mapping from processor to source
    - Move mapping logic to OtelProtoCodec
    - Enhance metrics of LogsGrpcService
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Remove Otel Logs Processor
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Fix JavaDoc Version
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Improve documentation of metrics
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Remove Otel logs processor from settings.gradle
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    ---------
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Signed-off-by: Kai Sternad
    &lt;ksternad@sternad.de&gt;
    Co-authored-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    
    Co-authored-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten
    Schnitter &lt;k.schnitter@sap.com&gt;
    Co-authored-by: Kai Sternad
    &lt;ksternad@sternad.de&gt;

* __Optimize buffer reads, allow delay to be 0 (#2189)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 27 Jan 2023 17:11:49 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __ENH: implement custom LogEventPatternConverter for desensitization on Data Prepper logging (#2188)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 27 Jan 2023 16:27:09 -0600
    
    
    * ADD: SensitiveArgumentMaskingConverter
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Generated THIRD-PARTY file for bd60dcc (#2201)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 27 Jan 2023 14:30:58 -0600
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: asifsmohammed
    &lt;asifsmohammed@users.noreply.github.com&gt;

* __Updated OpenSearch version to 1.3.7 (#2191)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 27 Jan 2023 09:24:13 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;

* __Fix Data Prepper to not terminate on invalid open telemetry metric/trace data (#2176)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 26 Jan 2023 19:03:00 -0600
    
    
    * Rebased to latest and removed unnecessary System.out.println
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified the test to send one valid record and one invalid record, as per the
    review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add configurations and metrics to OTelTraceRawProcessor (#2164)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 26 Jan 2023 19:01:47 -0600
    
    
    Updated the OTelTraceRawProcessor to use the new plugin configuration model.
    Added two plugin metrics to tracking the usage of two of the collections used
    in this processor.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Don&#39;t block on forwarding request response, populate records that failed to forward in local CPF buffer (#2175)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 26 Jan 2023 17:26:14 -0600
    
    
    * Handle peer forwarding in the background
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Rework tests to pass with new implementation
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Small tweaks to fix the diff
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Refactor to use Consumer&lt;AggregateHttpResponse&gt; and ExecutorService for
    background forwarding work
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests for processFailedRequestsLocally
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add more unit tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Move requestsReceivedFromPeers to HttpService to avoid gauge usage
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Coming full circle + an ExecutorService
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add missing private modifier
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Reuse existing clientThreadCount parameter, cleanup unused code
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix log message
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Add support to convert metrics to json strings without flattening attributes - issue #2146 (#2163)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 26 Jan 2023 15:45:02 -0600
    
    
    Add support to convert metrics to json strings without flattening attributes
    field - issue #2146
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __ENH: add and populate markers (#2180)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 26 Jan 2023 11:31:25 -0600
    
    
    * ENH: add and populate markers
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fixes links to use org/opensearch from com/amazon now that this is the correct the package name. (#2186)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 25 Jan 2023 08:29:54 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added AWS STS header override configurations for OpenSearch sink/S3 source (#1898)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 24 Jan 2023 17:17:13 -0600
    
    
    Added AWS STS header override configurations for the OpenSearch sink and the
    Amazon S3 source. Resolves #1888.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __ENH: add buffer records overflow metrics (#2170)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 24 Jan 2023 09:51:27 -0600
    
    
    * ENH: add buffer records overflow metrics
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Bump decode-uri-component in /release/staging-resources-cdk (#2064)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 23 Jan 2023 19:49:14 -0600
    
    
    Bumps
    [decode-uri-component](https://github.com/SamVerschueren/decode-uri-component)
    from 0.2.0 to 0.2.2.
    - [Release
    notes](https://github.com/SamVerschueren/decode-uri-component/releases)
    -
    [Commits](https://github.com/SamVerschueren/decode-uri-component/compare/v0.2.0...v0.2.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: decode-uri-component
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __New aggregate action - percent sampler (#2096)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 23 Jan 2023 17:36:31 -0600
    
    
    Percent Sampler aggregate action
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Implemented a heap-based circuit breaker (#2155)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Jan 2023 17:11:16 -0600
    
    
    Implemented a heap-based circuit breaker. This circuit breaker will prevent
    entry buffers from accepting events after the heap usage reaches a specified
    value. This checks for heap usage in a background thread and updates the state,
    which the buffer will then use to determine if the circuit breaker is open or
    closed. This also signals to the JVM to start a GC when the threshold is
    reached. Resolves #2150.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Test against multiple OTel version - Issue #1963 (#2154)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 23 Jan 2023 15:08:18 -0600
    
    
    * Test against multiple OTel version - Issue #1963
    Signed-off-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna Kondaka
    &lt;krishkdk@amazon.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#2161)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Sat, 21 Jan 2023 14:48:03 -0600
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.18 to
    1.12.22.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.18...byte-buddy-1.12.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#2160)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 20 Jan 2023 11:11:23 -0600
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.20 to
    1.12.22.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.20...byte-buddy-1.12.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __New Aggregate Action - Event Rate Limiter (#2090)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 19 Jan 2023 17:28:12 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#2100)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 19 Jan 2023 16:22:01 -0600
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.18 to
    1.12.20.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.18...byte-buddy-1.12.20)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added s3 support in Opensearch sink  (#2121)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 19 Jan 2023 16:21:32 -0600
    
    
    * Added implementation of s3 support in Opensearch sink
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added batch delay to CPF configuration (#2159)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 19 Jan 2023 13:05:26 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix for null pointer exception in remote peer forwarding (fix for issue 2123) (#2124)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 18 Jan 2023 20:56:57 -0600
    
    
    * Fix for null pointer exception in remote peer forwarding (fix for issue
    #2123)
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments to add a counter and not skip when an
    identification key is missing
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Modified to increment the counter only when all
    identification keys are missing
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added &#39;final&#39; to the local variable
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added a test with all missing keys
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Adds ScheduledExecutorService for Polling the RSS feed (#2140)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Wed, 18 Jan 2023 15:10:08 -0600
    
    
    * Adds ScheduledExecutor Service and runnable task
    Signed-off-by: Shivani
    Shukla &lt;sshkamz@amazon.com&gt;
    
    

* __Combined two integration tests for conditional routes into one. Also fixed a bug in the tests where the data was not sent to the correct sink. (#2061)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 17 Jan 2023 21:04:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add e2etest for testing log metrics (#2127)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 13 Jan 2023 15:19:16 -0600
    
    
    * Add e2etest for testing log metrics
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments.
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Adds count metrics to the service_map_stateful processor (#2130)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 12 Jan 2023 15:17:03 -0600
    
    
    Adds new metrics to the service_map_stateful processor. These count the number
    of items in the collections used by the service map. These do not have byte
    sizes, but use object counts.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump guava from 10.0.1 to 23.0 in /data-prepper-plugins/opensearch (#2050)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 11 Jan 2023 12:07:17 -0600
    
    
    Bumps [guava](https://github.com/google/guava) from 10.0.1 to 23.0.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/compare/v10.0.1...v23.0)
    
    ---
    updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump json5 from 2.2.0 to 2.2.3 in /release/staging-resources-cdk (#2119)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 11 Jan 2023 12:06:05 -0600
    
    
    Bumps [json5](https://github.com/json5/json5) from 2.2.0 to 2.2.3.
    - [Release notes](https://github.com/json5/json5/releases)
    - [Changelog](https://github.com/json5/json5/blob/main/CHANGELOG.md)
    - [Commits](https://github.com/json5/json5/compare/v2.2.0...v2.2.3)
    
    ---
    updated-dependencies:
    - dependency-name: json5
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated MAINTAINERS.md to match recommended opensearch-project format. (#2117)__

    [Daniel (dB.) Doubrovkine](mailto:dblock@dblock.org) - Thu, 5 Jan 2023 14:58:26 -0600
    
    
    Signed-off-by: dblock &lt;dblock@amazon.com&gt;
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Update OTelProtoCodec for InstrumentationLibrary to InstrumentationScope rename (#2114)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 4 Jan 2023 09:40:06 -0600
    
    
    * Add support for ScopeSpans migration to otel-trace-raw-processor
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove unused import
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Run spotless
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Update slf4j-simple; update log4j-slf4j-impl to log4j-slf4j2-impl (#2113)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 3 Jan 2023 17:16:03 -0600
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __MOD: fix typos (#2084)__

    [Shanelle Marasigan](mailto:39988782+rmarasigan@users.noreply.github.com) - Tue, 3 Jan 2023 15:51:49 -0600
    
    
    Signed-off-by: Russianhielle Marasigan &lt;russianhielle@gmail.com&gt;

* __Created a GitHub Action to generate the Third Party report. (#2033)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 22 Dec 2022 15:01:37 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __updating S3 source documentation to include all codecs (#2091)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 22 Dec 2022 16:04:08 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Provide a type conversion / cast processor #2010 (#2020)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 22 Dec 2022 15:50:01 -0600
    
    
    * Provide a type conversion / cast processor #2010
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Histogram Aggregate Action - Added duration and fixed end time in the aggregated output (#2085)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 22 Dec 2022 12:20:08 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix CVE-2022-41881, CVE-2021-21290 and CVE-2022-41915 (#2093)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 21 Dec 2022 16:36:04 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix: CVE-2022-3509, CVE-2022-3510 (#2079)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 19 Dec 2022 10:14:22 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add robust retry strategy to AcmClients (#2082)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 16 Dec 2022 19:29:07 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Updated info logs to debug level (#2083)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Dec 2022 16:35:28 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix: CVE-2022-36944 (#2080)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Dec 2022 15:59:07 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Histogram aggregate action (#2078)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 16 Dec 2022 11:22:23 -0800
    
    
    * Add Histogram aggregation action
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Updated jackson bom dependency (#2068)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 15 Dec 2022 14:44:59 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Anomaly detector (#2058)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 15 Dec 2022 16:28:28 -0600
    
    
    * Add support for anomaly detection in the pipeline with new anomaly detector
    processor
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add retry strategy to StsClient used for sigv4 auth against OpenSearch sinks (#2069)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 15 Dec 2022 11:26:50 -0600
    
    
    Add retry strategy to STS client used for sigv4 auth
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Parse RSS feed URL items and convert Item to Event (#2073)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Wed, 14 Dec 2022 13:16:46 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    
    * Adds Jackson Jdk8Module to enable usage of Optional in Rss Item model
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    

* __Fix: Updated parse json processor documentation (#2071)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 14 Dec 2022 08:47:12 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Count Aggregate Action - fix aggregation temporality (#2067)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 8 Dec 2022 17:21:41 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add support for count aggregate action (#2034)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 7 Dec 2022 12:51:11 -0600
    
    
    Add support for count aggregate action with raw and otel_metrics output format
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Setup boilerplate for RSS Source Plugin (#2062)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Wed, 7 Dec 2022 12:44:37 -0600
    
    
    * Adds boilerplate config and code for rss source
    Signed-off-by: Shivani
    Shukla &lt;sshkamz@amazon.com&gt;
    
    * Adds RSS Source plugin class
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;
    
    * Adds Document Event Type
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    
    * Adds Document interface and JacksonDocument class
    Signed-off-by: Shivani
    Shukla &lt;sshkamz@amazon.com&gt;
    
    * Adds a simple unit test checking default for pollingFrequency
    Signed-off-by:
    Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Bump slf4j-simple in /data-prepper-logstash-configuration (#2053)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 2 Dec 2022 14:04:27 -0600
    
    
    Bumps [slf4j-simple](https://github.com/qos-ch/slf4j) from 1.7.36 to 2.0.5.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.36...v_2.0.5)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-simple
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump slf4j-api from 1.7.36 to 2.0.5 (#2057)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 2 Dec 2022 14:00:34 -0600
    
    
    Bumps [slf4j-api](https://github.com/qos-ch/slf4j) from 1.7.36 to 2.0.5.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.36...v_2.0.5)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-api
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jsonassert from 1.5.0 to 1.5.1 in /data-prepper-api (#2043)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 1 Dec 2022 17:51:03 -0600
    
    
    Bumps [jsonassert](https://github.com/skyscreamer/JSONassert) from 1.5.0 to
    1.5.1.
    - [Release notes](https://github.com/skyscreamer/JSONassert/releases)
    -
    [Changelog](https://github.com/skyscreamer/JSONassert/blob/master/CHANGELOG.md)
    
    -
    [Commits](https://github.com/skyscreamer/JSONassert/compare/jsonassert-1.5.0...jsonassert-1.5.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.skyscreamer:jsonassert
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-raw-prepper (#2042)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 1 Dec 2022 17:50:08 -0600
    
    
    Bumps protobuf-java-util from 3.21.9 to 3.21.10.
    
    ---
    updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Router integration tests to verify that conditional pipeline routes work as expected. (#1988)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 1 Dec 2022 15:51:52 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the OpenSearch libraries to 1.3.6 and run the integration tests against 1.3.6 instead of 1.3.5. Fixes #2022 (#2030)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 30 Nov 2022 12:57:39 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes double brace initialization and includes this as a checkstyle rule to prevent future use. (#2035)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Nov 2022 16:16:14 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Implemented additional metrics for the S3 source.  (#2028)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Nov 2022 14:25:20 -0600
    
    
    Implemented additional metrics for the S3 source. Resolves #2024
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use the same compression engines for GZIP and NONE in the AUTOMATIC compression engine. Fixes #2026. (#2027)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 21 Nov 2022 11:15:05 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Log the number of messages received from the SQS queue, including a count of the number of messages that will need to be processed. Also, include logging of deletes at the debug level. (#2011)__

    [David Venable](mailto:dlv@amazon.com) - Sat, 19 Nov 2022 13:06:23 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add PluginMetrics in the Auth Plugin for Http, OTel and Metrics Source (#2023)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 18 Nov 2022 21:27:15 -0600
    
    
    * Add PluginMetrics in the Auth Plugin for Http, OTel and Metrics Source
     Issue: https://github.com/opensearch-project/data-prepper/issues/2007
    
    Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    
    * Added unit test for verifying PipelineDescription in DefaultPluginFactory
    class and fixed review comments on unit test
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Unit test update
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Unit test update
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
     Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    Signed-off-by: Dinu John
    &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add when condition to aggregate processor (#2018)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 18 Nov 2022 15:31:54 -0600
    
    
    * Add when condition to aggregate processor
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add when condition to aggregate processor - addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add when condition to aggregate processor - Fixed check style test errors
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Dynamic Index Name in OpenSearch sink  - Resolves #1459 (#1999)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 17 Nov 2022 20:36:47 -0600
    
    
    * Dynamic Index Name in OpenSearch sink #1459
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink #1459 -- updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink #1459 -- fixed a bug and increased test
    coverage
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments to add more tests and re-design index manager to
    accommodate dynamic indexes
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added tests for DynamicIndexManager
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink #1459 -- changed cache weigher to have
    constant value
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink #1459 -- addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink - Resolves Issue #1459 -- addressed
    review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink - Fixed checkSytleMain issues in
    opensearch
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink - Fixed spotlessJavaCheck issues in
    opensearch
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __updated samples for dataperpper 2 (#2019)__

    [Arunachalam Lakshmanan](mailto:arnlaksh@amazon.com) - Thu, 17 Nov 2022 10:02:31 -0600
    
    
    * updated samples for dataperpper 2
     Signed-off-by: Arun Lakshmanan &lt;arnlaksh@amazon.com&gt;
    
    * updated samples for dataperpper 2
     Signed-off-by: Arun Lakshmanan &lt;arnlaksh@amazon.com&gt;
     Signed-off-by: Arun Lakshmanan &lt;arnlaksh@amazon.com&gt;

* __Add IntelliJ&#39;s .iml file extension to the .gitignore. (#2016)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 16 Nov 2022 14:15:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-raw-prepper (#2001)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 11 Nov 2022 09:58:40 -0600
    
    
    Bumps protobuf-java-util from 3.19.4 to 3.21.9.
    
    ---
    updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Data Prepper Core integration tests (#1949)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 10 Nov 2022 09:50:03 -0600
    
    
    Creates an integration test source set for data-prepper-core and adds a small
    framework for running integration tests on data-prepper-core functionality.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding Chase Engelbrecht to the MAINTAINERS.md. (#2005)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 9 Nov 2022 09:55:59 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding Hai Yan to the maintainers. (#2004)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 8 Nov 2022 16:13:31 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump opensearch-java from 1.0.0 to 2.1.0 (#1733)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 8 Nov 2022 08:17:20 -0600
    
    
    Bumps [opensearch-java](https://github.com/opensearch-project/opensearch-java)
    from 1.0.0 to 2.1.0.
    - [Release
    notes](https://github.com/opensearch-project/opensearch-java/releases)
    -
    [Commits](https://github.com/opensearch-project/opensearch-java/compare/v1.0.0...v2.1.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.opensearch.client:opensearch-java
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-trace-raw-prepper (#2003)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 8 Nov 2022 08:15:44 -0600
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.22.0 to
    3.23.1.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.22.0...assertj-core-3.23.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump awaitility in /data-prepper-plugins/otel-trace-raw-prepper (#2002)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 8 Nov 2022 08:15:12 -0600
    
    
    Bumps [awaitility](https://github.com/awaitility/awaitility) from 4.1.1 to
    4.2.0.
    - [Release notes](https://github.com/awaitility/awaitility/releases)
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.1.1...awaitility-4.2.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Support remaining OpenTelemetry Metrics proto spec features (#1335)__

    [kmssap](mailto:ksternad@sternad.de) - Fri, 4 Nov 2022 10:58:14 -0500
    
    
    * Bump OTEL proto version
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Support OTEL ScopeMetrics
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add support for OTEL schemaUrl
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add exemplars to metrics plugin
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add metrics flags
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add support for Exponential Histogram
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add config switch for histogram bucket calculation
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Refactor Otel Metrics Proto
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Change config property to snake_case
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix JavaDoc
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Remove Clock from tests
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Change config parameters
    
    - Introduce allowed max scale
    - Invert histogram calculation params
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Address review comments
    
    - Remove unused import, breaking Checkstyle
    - Change Exponential Histogram filter
    - Add lenient to some Mockito calls
    - Clarify metrics processor documentation
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix OtelMetricsRawProcessorConfigTest
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Change ExponentialHistogram Bucket Calculation
    
    - Precompute all possible bucket bounds
    - Consider negative offset
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix e2e otel dependency coordinates
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix dependency coordinate for otel
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Kai Sternad
    &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1993)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 2 Nov 2022 20:22:12 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.17 to
    1.12.18.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.17...byte-buddy-1.12.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1992)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 2 Nov 2022 19:20:41 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.17 to
    1.12.18.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.17...byte-buddy-1.12.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Fixes for issues #1456 and #1458 - support for complex document ID and routing ID (#1966)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 2 Nov 2022 18:57:46 -0500
    
    
    * Fixes for issues #1456 and #1458 - support for complex document ID and
    routing ID
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixes for issues #1456 and #1458 - updated README.md and added unit tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixes for issues #1456 and #1458 - addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixes for issues #1456 and #1458 - fixed check style build failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add OpenSearch e2e test to github workflows
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Deleted e2e test for OpenSearch
    DocumentId/RoutingField testing
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Remove e2e test entry for removed e2e test
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments and added tests for fromStringAndOptionals method
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkstyle failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Update a few logs that were dumping the actual customer logs and traces into the DataPrepper logs on failure. Logging the request sizes instead, to avoid customer data being logged (#1989)__

    [Deep Datta](mailto:18663532+deepdatta@users.noreply.github.com) - Mon, 31 Oct 2022 14:30:12 -0500
    
    
    Signed-off-by: Deep Datta &lt;deedatta@amazon.com&gt;

* __Updated release notes and change log with spring change (#1981)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 27 Oct 2022 17:02:02 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Created release notes for Data Prepper 2.0.1. (#1969)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 27 Oct 2022 13:28:47 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added release notes for 1.5.2 (#1976)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 27 Oct 2022 12:44:57 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added change log for 1.5.2 (#1977)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 27 Oct 2022 12:28:10 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added the change log for 2.0.1. (#1968)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 27 Oct 2022 10:38:17 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Expressions with null (#1946)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 25 Oct 2022 15:00:55 -0500
    
    
    * fix for issue #1136 Add null support to DataPrepperExpressions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * fix for issue #1136 Add null support to DataPrepperExpressions - added more
    tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * fix for issue #1136 Add null support to DataPrepperExpressions - updated docs
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments - added test cases and updated document
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Reduce smoke test timeout to 8 minutes from 30 minutes. These tests tend to pass within 3 minutes in my personal GitHub branch. So this leaves quite a bit of buffer time. It helps speed up retrying failures from flaky tests. (#1956)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Oct 2022 09:48:44 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Attempt to reduce flakiness in RandomStringSourceTests by using awaitility. Split tests into two. JUnit 5. (#1921)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Oct 2022 09:48:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Run smoke tests against OpenSearch 1.3.6. (#1955)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Oct 2022 06:47:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Python grpcio 1.50.0 in smoke tests to reduce time to run. (#1954)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Oct 2022 06:46:43 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Delete s3:TestEvent objects and log them when they are found in the SQS queue. Resolves #1924. (#1939)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 20 Oct 2022 12:56:56 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add ExecutorService to DataPrepperServer (#1948)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 20 Oct 2022 11:18:27 -0500
    
    
    * Add ExecutorService to DataPrepperServer
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Shutdown executor service after stopping server
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Updated k8s manifest to suit Data Prepper 2.0 (#1928)__

    [Rafael Gumiero](mailto:rafael.gumiero@gmail.com) - Wed, 19 Oct 2022 17:04:42 -0500
    
    
    * Updated new paths for pepelines/config and new processor name
     Signed-off-by: Rafael Gumiero &lt;rafael.gumiero@gmail.com&gt;
    
    * Updated image version
     Signed-off-by: Rafael Gumiero &lt;rafael.gumiero@gmail.com&gt;
    
    * Moved Peer Forwarder to config file
     Signed-off-by: Rafael Gumiero &lt;rafael.gumiero@gmail.com&gt;
     Signed-off-by: Rafael Gumiero &lt;rafael.gumiero@gmail.com&gt;

* __Require protobuf-java-util 3.21.7 to fix #1891 (#1938)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 19 Oct 2022 12:35:33 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bug Fix: S3 source key  (#1926)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 19 Oct 2022 11:39:56 -0500
    
    
    * Fix: S3 source key bug fix
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Jackson 2.13.4.2 (#1925)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 18 Oct 2022 16:52:21 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Refactors the Data Prepper CLI argument parsing into data-prepper-main. Added an interface for the parts of DataPrepperArgs that client classes really need. (#1920)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 14 Oct 2022 10:03:06 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix PipelineConnector to duplicate the events (#1897)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 12 Oct 2022 16:32:00 -0500
    
    
    * Fix string mutate processors to duplicate the events
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix string mutate processors to duplicate the events - made changes as per
    David&#39;s suggestions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed unnecessary changes leftover from 1st commit
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified PipelineConnector to duplicate JacksonSpan type events too. Added
    testcases in PipelineConnectorTest
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comment and added a new testcase for JacksonSpan withData()
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comment and added parallel pipeline test to github/workflows
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * fixed workflow failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Simple duration regex did not allow for 0s or 0ms (#1910)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 11 Oct 2022 10:58:40 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updated the release notes for 2.0.0 (#1911)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 10 Oct 2022 17:46:26 -0500
    
    
    Updated the release notes for 2.0.0
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the change log for 2.0.0 with most recent changes. (#1909)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 10 Oct 2022 17:33:31 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update dev version to 2.1.0-SNAPSHOT (#1904)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 10 Oct 2022 13:34:46 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Increase the default buffer configurations by 25. Capacity to 12,800 and batch size to 200. (#1906)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 10 Oct 2022 11:59:38 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Conditional routing documentation (#1894)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 10 Oct 2022 10:35:15 -0500
    
    
    Add documentation for conditional routing. Resolves #1890
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added change log (#1901)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 7 Oct 2022 20:52:38 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Adds a stack-trace to failures from OpenSearch to help with debugging issues. (#1899)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 7 Oct 2022 18:32:26 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated protobuf dependency (#1896)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 7 Oct 2022 11:45:19 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updates Trace Analytics documentation for Data Prepper 2.0 (#1748)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 7 Oct 2022 11:37:52 -0500
    
    
    * Updates Trace Analytics examples to set the record_type to event in our
    documentation. Also includes a short guide to migrating to Data Prepper 2.0 for
    Trace Analytics.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Updated the documentation for the OTel Trace Source to show how to run on
    either Data Prepper 2.0 or 1.x.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Data Prepper 2.0.0 Release notes  (#1895)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 7 Oct 2022 11:25:06 -0500
    
    
    * Updated release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Addressed feedback
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __MAINT: rename grok match metrics (#1893)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 6 Oct 2022 17:11:30 -0500
    
    
    Signed-off-by: Qi Chen &lt;qchea@amazon.com&gt;
     Signed-off-by: Qi Chen &lt;qchea@amazon.com&gt;

* __Added core peer forwarder e2e tests (#1866)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 6 Oct 2022 11:22:53 -0500
    
    
    * Added e2e tests for core peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Using the file sink is causing build failures in valid_multiple_sinks_with_routes.yml. Use stdout instead. (#1889)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 6 Oct 2022 10:42:51 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __adding bufferUsage to BlockingBuffer plugin (#1882)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 6 Oct 2022 10:05:50 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Implementation of conditional routing of sinks (#1832)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Oct 2022 18:25:16 -0500
    
    
    Implementation of conditional routing of sinks.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Core peer forwarder performance inprovement (#1880)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 5 Oct 2022 11:39:33 -0500
    
    
    * Updated call to setPeerClientPool
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fixes file sink to work with multiple threads (#1842)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Oct 2022 11:04:23 -0500
    
    
    * This fixes an issue with file sink. Each processor thread would overwrite the
    other threads which prevents it from working as expected. Now, it keeps a
    Writer open as long as it is running. It will flush on each output() call.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Updated test files to use stdout instead of file sink since the file sink now
    will attempt to write to the file on construction.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Additional test to improve code coverage.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the bin/data-prepper script to use &#34;readlink -f&#34; instead of &#34;realpath&#34; since this is available by default on more systems. (#1883)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Oct 2022 11:02:07 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Apache AWS client exclusively to fix AWS Java SDK (#1877)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Oct 2022 10:34:54 -0500
    
    
    Fixes Data Prepper with AWS services by setting the Java system property.
    Resolves #1876
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated http source documentation (#1875)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 5 Oct 2022 10:32:36 -0500
    
    
    * Updated http source documentation
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump com.diffplug.spotless from 6.7.1 to 6.11.0 (#1884)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Oct 2022 10:29:01 -0500
    
    
    Bumps com.diffplug.spotless from 6.7.1 to 6.11.0.
    
    ---
    updated-dependencies:
    - dependency-name: com.diffplug.spotless
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.22 to 5.3.23 in /data-prepper-core (#1885)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Oct 2022 10:22:07 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Removed peer forwarder processor plugin (#1874)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 5 Oct 2022 10:02:15 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1851)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Oct 2022 09:50:47 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.16 to
    1.12.17.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.16...byte-buddy-1.12.17)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump micrometer-bom from 1.9.3 to 1.9.4 (#1858)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Oct 2022 09:20:31 -0500
    
    
    Bumps [micrometer-bom](https://github.com/micrometer-metrics/micrometer) from
    1.9.3 to 1.9.4.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.9.3...v1.9.4)
    
    
    ---
    updated-dependencies:
    - dependency-name: io.micrometer:micrometer-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated ssl to true by default for core peer forwarder (#1835)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 4 Oct 2022 10:29:23 -0500
    
    
    * Updated ssl to true by default
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Do not fail all GitHub Actions when other similar ones fail to help reduce the number of builds we have to re-run from flaky tests. (#1879)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 4 Oct 2022 09:46:13 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Jackson 2.13.4. Resolves #1839 (#1871)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 4 Oct 2022 08:28:17 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the OTel Metrics plugins to the org.opensearch.dataprepper package. (#1872)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 4 Oct 2022 08:18:20 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added metrics documentation (#1873)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 4 Oct 2022 00:12:59 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1850)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:16:46 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.14 to
    1.12.17.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.14...byte-buddy-1.12.17)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.22 to 5.3.23 in /data-prepper-core (#1855)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:16:05 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.22 to 5.3.23 in /data-prepper-expression (#1862)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:13:44 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.22 to 5.3.23 in /data-prepper-expression (#1860)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:13:27 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.22 to 5.3.23 in /data-prepper-core (#1856)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:13:04 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.22 to 5.3.23 in /data-prepper-expression (#1861)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:11:15 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump log4j-bom from 2.18.0 to 2.19.0 in /data-prepper-core (#1853)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:10:00 -0500
    
    
    Bumps log4j-bom from 2.18.0 to 2.19.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump log4j-bom from 2.18.0 to 2.19.0 in /data-prepper-expression (#1864)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:09:31 -0500
    
    
    Bumps log4j-bom from 2.18.0 to 2.19.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Use exec command to run application (#1847)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 3 Oct 2022 17:49:33 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __BUG FIX: Core peer forwarder trace pipeline bug (#1865)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 3 Oct 2022 16:49:42 -0500
    
    
    * Fix: Core peer forwarder trace pipeline bug
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump jackson-databind from 2.13.3 to 2.13.4 (#1857)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 16:18:54 -0500
    
    
    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.13.3 to
    2.13.4.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates log4j configurations to use the org.opensearch.dataprepper package. Updates documentation to this package as well to help clarify expected outcomes for users. Contributes toward #344. (#1841)__

    [David Venable](mailto:dlv@amazon.com) - Sat, 1 Oct 2022 13:57:21 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add fingerprint trust store implementation to PeerForwarderHttpServer (#1848)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 30 Sep 2022 14:27:25 -0500
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Update plugin names to use processor (#1838)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 30 Sep 2022 10:30:24 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added additional metrics to peer forwarder (#1836)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 30 Sep 2022 09:22:54 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Support evaluating conditional expressions in multiple threads by using a ThreadLocal ParseTreeParser. Resolves #1189 (#1840)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 18:39:17 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Armeria to 1.19.0 from 1.16.0. (#1837)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 14:43:51 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Decompose archive smoke-tests (#1808)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 13:18:56 -0500
    
    
    * Decompose the archive smoke tests by having the different combinations
    available as command-line arguments. This allows us to split up the release
    GitHub Action to test each archive file independently of each other.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Updated the release smoke test strategy to use specific inclusions.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Update release/smoke-tests/README.md
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Co-authored-by: Hai Yan &lt;8153134+oeyh@users.noreply.github.com&gt;
    
    * Other README corrections.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    Co-authored-by: Hai Yan
    &lt;8153134+oeyh@users.noreply.github.com&gt;

* __Replace example pipeline yaml with a README (#1830)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 29 Sep 2022 12:21:07 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updates otel-proto-common to org.opensearch.dataprepper. (#1834)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 12:07:18 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates data-prepper-api to the org.opensearch.dataprepper package. (#1833)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 09:01:34 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added metrics for peer forwarder (#1812)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 23:15:56 -0500
    
    
    * Added existing metrics to peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactor Processor shutdown behavior to wait until buffers are empty before preparing for shutdown (#1792)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 28 Sep 2022 22:49:54 -0500
    
    
    * Add drainTimeout to PeerForwarderConfiguration, refactor ProcessWorker
    shutdown behavior
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Corrected the way that PipelineModel deserializes the route property. It was previously using a setter approach and used a different property name in the constructor. (#1825)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Sep 2022 18:36:24 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bug fix: peer forwarder not decorating processors (#1831)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 17:21:44 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated third party file (#1828)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 16:06:50 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update examples (#1796)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 28 Sep 2022 14:40:15 -0500
    
    
    * Update examples for directory change
    * Fix analytics sample app
    * Exclude javax.json dependency from antlr4
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updated the PipelineModel to return a SinkModel for sinks (#1802)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Sep 2022 11:31:24 -0500
    
    
    Updated the PipelineModel to return a SinkModel for sinks. This includes fields
    for conditional routing.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated default peer forwarder port (#1823)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 09:48:43 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Change CMD to exec form in Dockerfile (#1822)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 28 Sep 2022 09:38:53 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix: Peer forwarder exception with single thread annotated processors (#1821)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 09:36:51 -0500
    
    
    * Fix: Peer forwarder exception with single thread annotated processors
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Addressed feedback
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated the package for most of the remaining plugins to org.opensearch.dataprepper. This excludes the metrics plugins, proto-commons, and the classic peer-forwarder plugin. Contributes toward #344. (#1815)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 27 Sep 2022 22:05:00 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add sslVerifyFingerprintOnly flag to PeerForwarderConfiguration (#1818)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Tue, 27 Sep 2022 22:04:39 -0500
    
    
    * Add sslVerifyFingerprintOnly flag to PeerForwarderConfiguration
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Move getFingerprint() functionality into Certificate model, add tests for
    getFingerprint()
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add PeerForwarderConfiguration tests for sslFingerprintVerificationOnly
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Wire isSslFingerprintVerificationOnly into PeerForwarderClientFactory, add
    tests for it
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add documentation for sslFingerprintVerificationOnly
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add PeerForwarder_ClientServer integration tests for
    sslFingerprintVerificationOnly
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix jdk version check in script (#1819)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 27 Sep 2022 21:18:09 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added demo cert and key files (#1813)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 27 Sep 2022 18:41:50 -0500
    
    
    * Added default cert and key files
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated file names
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added README.md
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated config files
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Make HTTP source request timeout transparent (#1814)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 27 Sep 2022 15:43:54 -0500
    
    
    * Clean up the usage of request_timeout; update tests and readme
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updated the package for trace analytics plugins to org.opensearch.dataprepper. (#1810)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 27 Sep 2022 09:45:48 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the package for data-prepper-test-common to org.opensearch.dataprepper. (#1809)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 27 Sep 2022 09:45:27 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Parse JSON Processor: README, more testing, support for JSON Pointer (#1696)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Mon, 26 Sep 2022 18:36:57 -0500
    
    
    Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __S3 Event Consistency via metadata and JSON changes (#1803)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 26 Sep 2022 17:30:06 -0500
    
    
    Support a configurable key for S3 metadata and make this base key s3/ by
    default. Moved the output of JSON from S3 objects into the root of the Event
    from the message key. Resolves #1687
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added documentation for core peer forwarder (#1797)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 26 Sep 2022 09:31:32 -0500
    
    
    * Added documentation for core peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Implement AggregateProcessor shutdown behavior (#1794)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 26 Sep 2022 08:39:16 -0500
    
    
    * Implement AggregateProcessor shutdown behavior
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Move branching on isShuttingDown to existing loop
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add missing imports
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove bound from nextLong
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Refactor test case to support either order of map iteration
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add isShuttingDown flag to conclude group check in
    AggregateActionSynchronizer
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Rename isShuttingDown flag to forceConclude, add test for groups size == 1
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Bump protobuf in /examples/trace-analytics-sample-app/sample-app (#1801)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Sat, 24 Sep 2022 09:39:19 -0500
    
    
    Bumps [protobuf](https://github.com/protocolbuffers/protobuf) from 3.15.6 to
    3.18.3.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/main/generate_changelog.py)
    
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.15.6...v3.18.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: protobuf
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump protobuf in /release/smoke-tests/otel-span-exporter (#1800)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 23 Sep 2022 17:59:49 -0500
    
    
    Bumps [protobuf](https://github.com/protocolbuffers/protobuf) from 3.19.1 to
    3.19.5.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/main/generate_changelog.py)
    
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.19.1...v3.19.5)
    
    
    ---
    updated-dependencies:
    - dependency-name: protobuf
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates to several plugins to move to the org.opensearch.dataprepper package (#1787)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 22 Sep 2022 14:52:32 -0700
    
    
    Updates many plugins to the org.opensearch package: csv, date, log-generator,
    mutate, aggregate, drop, grok, http, key-value. #344
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix ignored Logstash config file when loading from directory (#1788)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 22 Sep 2022 14:27:42 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix otel config (#1790)__

    [bryan-aguilar](mailto:46550959+bryan-aguilar@users.noreply.github.com) - Thu, 22 Sep 2022 10:35:57 -0700
    
    
    Signed-off-by: Bryan Aguilar &lt;bryaag@amazon.com&gt;

* __updated aws-cdk to version ^2.42.1 (#1793)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Thu, 22 Sep 2022 10:29:13 -0500
    
    
    Fix cve-2022-36067

* __Update docs to reflect recent changes on directory structure (#1786)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 20 Sep 2022 13:42:34 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Removed io.pebbletemplates dependency (#1784)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Mon, 19 Sep 2022 16:50:57 -0500
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Refactor GrokPrepper shutdown behavior to prevent ExecutorService from terminating prematurely (#1776)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 19 Sep 2022 15:22:10 -0500
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Adds the Data Prepper icon to the start of the README.md file. (#1783)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Sep 2022 15:17:41 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the OpenSearch sink to use the org.opensearch.dataprepper package name. Contributes to #344. (#1780)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Sep 2022 10:37:16 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds the Data Prepper horizontal icon with auto light/dark mode as an SVG. (#1782)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Sep 2022 10:33:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added Java time module to object mapper (#1779)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sat, 17 Sep 2022 19:14:43 -0500
    
    
    * Added Java time module to object mapper
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added new object mapper for peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added qualifier
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Support hostname verification in core peer-forwarding. Provide the ability to disable hostname verification. Generate test certificates with a SAN and document how to do it. (#1778)__

    [David Venable](mailto:dlv@amazon.com) - Sat, 17 Sep 2022 14:53:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Integration testing against latest OpenSearch versions (#1754)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Sep 2022 15:19:55 -0500
    
    
    Renamed the GitHub action for OpenSearch targets and support the latest
    versions in the 2.x series as of now. Test OpenSearch 1.3.5 as the latest
    version in that series.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support Mutual TLS authentication for Core Peer Forwarding. Resolves #1758. (#1771)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Sep 2022 15:19:03 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Create a runnable distribution from assemble task (#1774)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 16 Sep 2022 15:17:25 -0500
    
    
    * Configure install task and add to assemble task
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Load pipeline configurations from directory (#1760)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 16 Sep 2022 14:11:41 -0500
    
    
    * Load one or more pipeline configs from directory
    * Add back the support for command line arguments to specify configs
    * Update docker for smoke tests
    * Update relevant documentation
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix broken PeerForwardingProcessingDecoratorTest by casting object as Processor. (#1777)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Sep 2022 14:10:19 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added receive records to peer forwarder (#1761)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Sep 2022 10:58:22 -0500
    
    
    * Added receive records to peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Implement RequiresPeerForwarding interface (#1767)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Sep 2022 10:14:24 -0500
    
    
    * Aggregate processor implements RequiresPeerForwarding interface
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactor PluginDurationDeserializer to DataPrepperDurationDeserializer, add processorShutdownTimeout to DataPrepperConfiguration, use processorShutdownTimeout from DataPrepperConfiguration in Pipeline (#1757)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 15 Sep 2022 14:06:10 -0500
    
    
    * Refactor PluginDurationDeserializer to DataPrepperDurationDeserializer, add
    processorShutdownTimeout to DataPrepperConfiguration, use
    processorShutdownTimeout from DataPrepperConfiguration in Pipeline
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Updated config defaults and validations for peer forwarder (#1768)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 15 Sep 2022 13:52:55 -0500
    
    
    * Updated config defaults and validations
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Inject PeerForwarderServer in to DataPrepper class (#1764)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 15 Sep 2022 10:25:24 -0500
    
    
    * Injected peer forwarder server in to DataPrepper
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Peer Forwarder client-server integration testing and related fixes (#1753)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 14 Sep 2022 09:08:00 -0500
    
    
    Created an integration test which verifies that a PeerForwarderClient can
    forward events to a PeerForwarderServer. This revealed several bugs which are
    fixed in this PR. Additionally, client exceptions are thrown as exceptions
    rather than incorrect status codes.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated Opensearch rest high-level client (#1755)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 13 Sep 2022 15:27:15 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Load data prepper configurations from home directory (#1737)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 13 Sep 2022 14:01:32 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added Local peer forwarder and updated http servoce class to write to… (#1750)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 13 Sep 2022 13:09:20 -0500
    
    
    * Added Local peer forwarder and updated http service class to write to buffer
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Allows the plugin framework to correctly load classes which inherit from PluginSetting. (#1752)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 12 Sep 2022 16:35:12 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Get PeerForwarder from a PeerForwarderProvider (#1717)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 9 Sep 2022 14:04:35 -0500
    
    
     Refactored the PeerForwarder class such that a PeerForwarderProvider is
    available as the primary means of obtaining a PeerForwarder object. This can
    yield a dynamic approach to PeerForwarder which can provide solutions for when
    peer forwarding is not needed or not configured. Includes making PeerForwarder
    an interface and supplying a package-protected RemotePeerForwarder for
    configurations requiring peer-forwarding.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added server proxy for Peer Forwarder Server (#1738)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 9 Sep 2022 13:22:22 -0500
    
    
    * Added server proxy
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump gradle-license-report from 1.17 to 2.1 (#1555)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Sep 2022 12:30:04 -0500
    
    
    Bumps gradle-license-report from 1.17 to 2.1.
    
    ---
    updated-dependencies:
    - dependency-name: com.github.jk1:gradle-license-report
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy from 1.12.14 to 1.12.16 (#1743)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Sep 2022 12:27:39 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.14 to
    1.12.16.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.14...byte-buddy-1.12.16)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Update Data Prepper to build using JDK 11 making this the minimum sup… (#1739)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 9 Sep 2022 08:52:45 -0500
    
    
    Update Data Prepper to build using JDK 11 making this the minimum supported
    Java version. Resolves #1422. Includes updated documentation.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump guava from 31.0.1-jre to 31.1-jre (#1734)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 8 Sep 2022 20:43:31 -0500
    
    
    Bumps [guava](https://github.com/google/guava) from 31.0.1-jre to 31.1-jre.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Update trace analytics sample app pipeline.yaml to include event record type for otel_trace_source (#1741)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 8 Sep 2022 16:51:12 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Update HTTP source ssl validations (#1740)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 8 Sep 2022 16:46:37 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update Docker images to use JDK 17 (#1727)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 8 Sep 2022 12:45:02 -0500
    
    
    * Update Data Prepper docker to use Temurin jdk17-alpine
    * Update jdk in tar.gz archive file to use Temurin jdk-17
    * Update trace analytics dev sample app and EMF monitoring dev sample
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Renamed router to route in the PipelineModel (#1721)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 8 Sep 2022 10:43:10 -0500
    
    
    Renamed router to route in the PipelineModel. Improved the tests by including
    tests that validate the route properties.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1712)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Sep 2022 22:02:17 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.13 to
    1.12.14.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.13...byte-buddy-1.12.14)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1711)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Sep 2022 20:15:26 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.12 to
    1.12.14.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.12...byte-buddy-1.12.14)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump hibernate-validator from 7.0.4.Final to 7.0.5.Final (#1715)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Sep 2022 20:14:52 -0500
    
    
    Bumps [hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 7.0.4.Final to 7.0.5.Final.
    - [Release notes](https://github.com/hibernate/hibernate-validator/releases)
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/7.0.5.Final/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/7.0.4.Final...7.0.5.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time from 2.10.14 to 2.11.1 (#1714)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Sep 2022 19:26:46 -0500
    
    
    Bumps [joda-time](https://github.com/JodaOrg/joda-time) from 2.10.14 to 2.11.1.
    
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.10.14...v2.11.1)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Deletes PrepperStateTest which is no longer valid after #1707 (#1732)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 7 Sep 2022 18:10:25 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Moved plugins commons projects to org.opensearch.dataprepper package. (#1726)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 7 Sep 2022 16:54:24 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added apache client for creating s3 and acm client (#1731)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 7 Sep 2022 16:43:44 -0500
    
    
    * Added apache client for creating s3 and acm client
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Remove deprecated prepper plugin type (#1707)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 7 Sep 2022 15:52:43 -0500
    
    
    * Replace prepper with processor in yaml samples
    * Replace prepper with processor in code comments
    * Remove Prepper interface and related usage
    * Change PepperState to ProcessorState
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added the SinkModel class for conditional routing on Sinks (#1681)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 7 Sep 2022 10:10:41 -0500
    
    
    Added the SinkModel class which allows applying conditional routes on sinks
    without allowing them on other plugin types.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added peer forwarder http server (#1705)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 6 Sep 2022 12:48:35 -0500
    
    
    * Added peer forwarder server
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Run OpenSearch sink integration tests only when the plugin changes (#1706)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 6 Sep 2022 11:17:32 -0500
    
    
    Configured the OpenSearch integration tests to run only when the OpenSearch
    sink plugin is actually changed or the root Gradle project changes.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Acm private key password is nullable (#1719)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 6 Sep 2022 10:02:45 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update the e2e test to the org.opensearch package name. Partially resolves #344 (#1703)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 2 Sep 2022 14:32:55 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Maintenance: remove support for non event data model (#1689)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Fri, 2 Sep 2022 09:33:03 -0500
    
    
    * MAINT: remove support for OTLP data transport
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Fix: CVE-2020-36518, CVE-2022-24823 (#1704)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 1 Sep 2022 10:58:09 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added custom exceptions and code refactoring (#1698)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 31 Aug 2022 15:13:48 -0500
    
    
    * Added custom exceptions and code refactoring
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Health check auth bug (#1695)__

    [David Powers](mailto:ddpowers@amazon.com) - Wed, 31 Aug 2022 14:21:58 -0500
    
    
    * Add more robust username/password check in order to allow auth-free health
    checks
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Allow specific HTTP methods for core API endpoints (#1697)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 31 Aug 2022 13:27:00 -0500
    
    
    Allows only specific HTTP methods on core API endpoints; Updates docs and unit
    tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __#1513: Updating antlr versions. (#1700)__

    [Jeff Zemerick](mailto:13176962+jzonthemtn@users.noreply.github.com) - Wed, 31 Aug 2022 11:42:31 -0500
    
    
    Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;

* __Remove deprecated configurations for OpenSearch sink (#1690)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 29 Aug 2022 09:23:40 -0500
    
    
    * Remove deprecated raw and service_map flags from index config
    * Update unit tests, integration tests, and README.md for OpenSearchSink
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added Peer Forwarder Client (#1677)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 26 Aug 2022 17:08:36 -0500
    
    
    * Added Peer Forwarder Client
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add CSV Codec to S3 Source IT &amp; add CSV Processor Integration Tests (#1683)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Fri, 26 Aug 2022 15:11:58 -0500
    
    
    * Added CSV Codec to S3 Source integration tests &amp; added CSV Processor
    integration tests
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __ParseJsonProcessor initial implementation (#1688)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Fri, 26 Aug 2022 10:51:49 -0500
    
    
    * Initial implementation of parse_json processor
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Go back to using bash in the bin/data-prepper, it is a bash script and does not work on all machines using &#39;sh&#39;. Update the tarball smoke tests to work with the new directory structure. Additionally, changed the tarball smoke tests to create their own image rather than overwriting the normal Data Prepper image. (#1694)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 26 Aug 2022 10:25:52 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added CsvInvalidEvents metric to CSV Processor (#1684)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Fri, 26 Aug 2022 10:18:32 -0500
    
    
    * Added CsvInvalidEvent metric to CSV Processor
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Updated spring boot version in examples package (#1691)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 25 Aug 2022 10:03:44 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactored the PipelineParser and PipelineConfiguration to use the PipelineDataFlowModel for deserializing the pipeline YAML. This makes the deserialization consistent with serialization. It is also necessary for the upcoming conditional routing on sinks work. (#1680)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Aug 2022 13:46:15 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Prepper plugins to use Processor (#1686)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 23 Aug 2022 09:51:20 -0500
    
    
    * Update prepper to processor for StringPrepper
    * Update prepper to processor for GrokPrepper
    * Update prepper to processor for OTelTraceGroupPrepper
    * Update prepper to processor for OTelTraceRawPrepper
    * Update prepper to processor for NoOpPrepper
    * Update prepper to processor in readme files
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Created the data-prepper-main project. Updated the tar.gz to pull in all the lib files. Updated the data-prepper script to handle the new installation. Updated end-to-end tests as well. Updated the Data Prepper Docker image to use the Linux distribution including the bin/data-prepper script. (#1682)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Aug 2022 09:07:23 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added ACM and S3 certificate support for HTTP source (#1678)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 22 Aug 2022 15:24:40 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Some code clean-up to Pipeline and added unit tests to PipelineTests for publishToSinks. (#1674)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 22 Aug 2022 13:51:12 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added the router to the Data Prepper pipeline model. (#1666)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Aug 2022 12:51:22 -0500
    
    
    Added the router to the Data Prepper pipeline model.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes another incorrect package related to HashRing. (#1676)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Aug 2022 11:01:31 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes an incorrect package related to HashRing. (#1675)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Aug 2022 10:04:20 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the package name to org.opensearch for data-prepper-core. (#1671)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 17 Aug 2022 18:32:52 -0500
    
    
    Updated the package name to org.opensearch for data-prepper-core. Updated the
    Java main class package name to org.opensearch.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added certificate provider,  peer client pool, hash ring and client factory (#1663)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 17 Aug 2022 10:02:40 -0500
    
    
    * Added certificate provider, hash ring, peer client
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added logstash config conversion for csv processor &amp; added explanatio… (#1659)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Wed, 17 Aug 2022 09:55:19 -0500
    
    
    * Added logstash config conversion for csv processor &amp; added explanation of
    boolean to LogstashValueType javadoc
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Removes the data-prepper-benchmarks project which is not in use. (#1662)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 15 Aug 2022 11:13:53 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Build with Gradle 7.5.1, the current latest version. (#1667)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 15 Aug 2022 11:08:32 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Health check auth bug (#1625)__

    [David Powers](mailto:ddpowers@amazon.com) - Fri, 12 Aug 2022 16:40:57 -0500
    
    
    Add auth free health check with a configuration option
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Support loading plugins from both org.opensearch.dataprepper and com.amazon.dataprepper to help migrate to the new package name. (#1661)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 12 Aug 2022 12:26:41 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump micrometer-bom from 1.9.0 to 1.9.3 (#1646)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 11 Aug 2022 20:34:13 -0500
    
    
    Bumps [micrometer-bom](https://github.com/micrometer-metrics/micrometer) from
    1.9.0 to 1.9.3.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.9.0...v1.9.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: io.micrometer:micrometer-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jakarta.validation-api from 3.0.1 to 3.0.2 (#1647)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 11 Aug 2022 20:33:22 -0500
    
    
    Bumps
    [jakarta.validation-api](https://github.com/eclipse-ee4j/beanvalidation-api)
    from 3.0.1 to 3.0.2.
    - [Release notes](https://github.com/eclipse-ee4j/beanvalidation-api/releases)
    -
    [Commits](https://github.com/eclipse-ee4j/beanvalidation-api/compare/3.0.1...3.0.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: jakarta.validation:jakarta.validation-api
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added javadocs to csv processor &amp; codec, added README to csv processor (#1658)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Thu, 11 Aug 2022 16:21:20 -0500
    
    
    * Added javadocs to csv processor &amp; codec, added README to csv processor
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
    
    * Addressed David&#39;s feedback &amp; rewrote quote_character description
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Created the data-prepper script for the new directory structure (#1655) (#1656)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Aug 2022 15:14:55 -0500
    
    
    Created the data-prepper script from the data-prepper-tar-install script. This
    now sits in the bin/ directory of the directory structure. Moved the
    data-prepper-core library to the lib/ directory.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1652)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 11 Aug 2022 14:41:50 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.10 to
    1.12.13.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.10...byte-buddy-1.12.13)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Removed the deprecated type property on DataPrepperPlugin. Resolves #1657. (#1660)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Aug 2022 14:33:58 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1552)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 10 Aug 2022 17:13:27 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.10 to
    1.12.12.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.10...byte-buddy-1.12.12)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump bcpkix-jdk15on in /data-prepper-plugins/otel-metrics-source (#1559)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 10 Aug 2022 17:12:51 -0500
    
    
    Bumps [bcpkix-jdk15on](https://github.com/bcgit/bc-java) from 1.69 to 1.70.
    - [Release notes](https://github.com/bcgit/bc-java/releases)
    -
    [Changelog](https://github.com/bcgit/bc-java/blob/master/docs/releasenotes.html)
    
    - [Commits](https://github.com/bcgit/bc-java/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.bouncycastle:bcpkix-jdk15on
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump junit-jupiter-engine from 5.8.2 to 5.9.0 (#1649)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 10 Aug 2022 17:12:23 -0500
    
    
    Bumps [junit-jupiter-engine](https://github.com/junit-team/junit5) from 5.8.2
    to 5.9.0.
    - [Release notes](https://github.com/junit-team/junit5/releases)
    - [Commits](https://github.com/junit-team/junit5/compare/r5.8.2...r5.9.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.junit.jupiter:junit-jupiter-engine
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Remove dependency on AWS S3 SDKv1 (#1628)__

    [Jeff Zemerick](mailto:13176962+jzonthemtn@users.noreply.github.com) - Wed, 10 Aug 2022 16:45:20 -0500
    
    
    * #1562: Working toward removing dependency.
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;
    
    * #1562: Removing unneeded parts. Changing date processing.
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;
    
    * #1562: Adding file header and comment.
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;
    
    * #1562: Removing sysout and removing public from class.
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;

* __Updated pipeline configurations and documentation to prefer the OpenSearch index_type configuration over the trace_analytics_raw and trace_analytics_service_map booleans. The latter two configurations are deprecated and will be removed in 2.0. (#1650)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 10 Aug 2022 13:10:14 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Implemented CSVCodec for S3 Source, config &amp; unit tests (#1644)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Tue, 9 Aug 2022 15:40:28 -0500
    
    
    * Implemented CSVCodec for S3 Source, config &amp; unit tests
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
    
    * Addressed Travis&#39;s feedback
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
    
    * Addressed David&#39;s feedback &amp; fixed NPE if header is null
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
    
    * Renamed all instances of CSVProcessor to CsvProcessor (and offshoots like
    CsvProcessorConfig)
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Added ACM for SLL and Discovery Mode Configurations (#1645)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 9 Aug 2022 15:39:50 -0500
    
    
    * Added ACM for SLL and Discovery Mode Configurations
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Enhanced Newline Codec w optional header_destination to add first lin… (#1640)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Tue, 9 Aug 2022 15:38:47 -0500
    
    
    * Enhanced Newline Codec w optional header_destination to add first line as
    header to outgoing events
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Bump log4j-bom from 2.17.2 to 2.18.0 in /data-prepper-expression (#1637)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 13:42:01 -0500
    
    
    Bumps log4j-bom from 2.17.2 to 2.18.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump log4j-bom from 2.17.2 to 2.18.0 in /data-prepper-core (#1634)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 13:41:36 -0500
    
    
    Bumps log4j-bom from 2.17.2 to 2.18.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib-common in /data-prepper-plugins/mapdb-prepper-state (#1630)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 13:33:19 -0500
    
    
    Bumps [kotlin-stdlib-common](https://github.com/JetBrains/kotlin) from 1.7.0 to
    1.7.10.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.7.10/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.7.0...v1.7.10)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib-common from 1.7.0 to 1.7.10 (#1635)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 13:32:42 -0500
    
    
    Bumps [kotlin-stdlib-common](https://github.com/JetBrains/kotlin) from 1.7.0 to
    1.7.10.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.7.10/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.7.0...v1.7.10)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump junit-jupiter-api from 5.7.0 to 5.9.0 (#1626)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:23:23 -0500
    
    
    Bumps [junit-jupiter-api](https://github.com/junit-team/junit5) from 5.7.0 to
    5.9.0.
    - [Release notes](https://github.com/junit-team/junit5/releases)
    - [Commits](https://github.com/junit-team/junit5/compare/r5.7.0...r5.9.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.junit.jupiter:junit-jupiter-api
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.21 to 5.3.22 in /data-prepper-core (#1633)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:22:44 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.21 to 5.3.22 in /data-prepper-core (#1631)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:15:38 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.21 to 5.3.22 in /data-prepper-expression (#1638)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:15:11 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.21 to 5.3.22 in /data-prepper-expression (#1639)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:14:50 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.21 to 5.3.22 in /data-prepper-expression (#1636)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:06:41 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.21 to 5.3.22 in /data-prepper-core (#1632)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 8 Aug 2022 13:33:46 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib in /data-prepper-plugins/mapdb-prepper-state (#1629)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 8 Aug 2022 13:32:28 -0500
    
    
    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.7.0 to
    1.7.10.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.7.10/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.7.0...v1.7.10)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added buffer for core peer forwarder (#1641)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sun, 7 Aug 2022 22:14:41 -0500
    
    
    * Added buffer for core peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add @JsonProperty to workers and readBatchDelay in PipelineModel (#1642)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 5 Aug 2022 16:03:33 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Implemented CSV Processor w unit tests, added validation that delimit… (#1627)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Wed, 3 Aug 2022 10:26:28 -0500
    
    
    * Implemented CSV Processor w unit tests, added validation that delimiter and
    quote char are different
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __#1457: Adding bulk create option. (#1561)__

    [Jeff Zemerick](mailto:13176962+jzonthemtn@users.noreply.github.com) - Sat, 30 Jul 2022 11:11:45 -0500
    
    
    Signed-off-by: jzonthemtn &lt;jeff.zemerick@mtnfog.com&gt;

* __Added core peer forwarder config (#1621)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 29 Jul 2022 12:04:55 -0500
    
    
    * Added core peer forwarder config
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Created CSVProcessor skeleton &amp; CSVProcessorConfig (#1620)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Thu, 28 Jul 2022 13:47:12 -0500
    
    
    * Created CSVProcessor skeleton &amp; wrote CSVProcessorConfig
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Add buffer PluginModel to PipelineModel, require 1 source and at least 1 sink (#1611)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 27 Jul 2022 15:37:33 -0500
    
    
    * Add buffer PluginModel to PipelineModel, require 1 source and at least 1 sink
    
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updated PipelineParser to create decorator for stateful processors (#1598)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 20 Jul 2022 12:55:44 -0500
    
    
    * Updated PipelineParser to create decorator for stateful processors
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added Core Peer Forwarder decorator and interface (#1592)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 18 Jul 2022 21:07:17 -0500
    
    
    * Added Core Peer Forwarder decorator and interface
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated integration tests with GZIP compression tests (#1577)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 18 Jul 2022 15:33:50 -0500
    
    
    * Updated integration tests with GZIP compression tests
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Addressed PR feedback
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Wrote unit tests for LogGeneratorSource, updated gradle.build dependencies (#1588)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Sat, 16 Jul 2022 12:03:55 -0500
    
    
    * Wrote unit tests for LogGeneratorSource, updated dependencies
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Updated trace analytics doc URL&#39;s (#1583)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 13 Jul 2022 10:06:38 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Remove deprecated PluginFactory classes #1584 (#1585)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 11 Jul 2022 21:05:04 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump aws-java-sdk-s3 from 1.12.220 to 1.12.257 (#1586)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 11 Jul 2022 13:46:22 -0500
    
    
    Bumps [aws-java-sdk-s3](https://github.com/aws/aws-sdk-java) from 1.12.220 to
    1.12.257.
    - [Release notes](https://github.com/aws/aws-sdk-java/releases)
    - [Changelog](https://github.com/aws/aws-sdk-java/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/aws/aws-sdk-java/compare/1.12.220...1.12.257)
    
    ---
    updated-dependencies:
    - dependency-name: com.amazonaws:aws-java-sdk-s3
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Create Log Generator Source with common apache log type, move ApacheLogFaker class from e2e-test to log-generator-source (#1548)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 11 Jul 2022 12:23:04 -0500
    
    
    Signed-off-by: graytaylor0 &lt;tylgry@amazon.com&gt;

* __Added the CHANGELOG for 1.5.1 (#1580)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 8 Jul 2022 11:44:46 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added release notes for Data Prepper 1.5.1. (#1576)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 8 Jul 2022 11:09:58 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix: Updated GZipCompressionEngine to use GzipCompressorInputStream (#1570)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 7 Jul 2022 19:21:02 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add QueryParameters option to AWS CloudMap-based peer discovery (#1560)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 7 Jul 2022 11:56:10 -0500
    
    
    * Implement QueryParameters filter option for AWS CloudMap peer forwarder
    discovery
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Small fixes related to import statements and unit tests. Update README
    wording to be a bit more specific
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Adding some asserts to unit tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Randomize map generation in AwsCloudMapPeerListProvider and add test case for
    QueryParameters missing from PluginSetting in
    AwsCloudMapPeerListProvider_CreateTest
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix: Added exception handling for S3 processing (#1551)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 5 Jul 2022 11:03:55 -0500
    
    
    * Fix: Added exception handling for S3 processing and updated poll delay
    condition
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated the sample OTel Collector to the current latest version: 0.54.0. (#1545)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 30 Jun 2022 09:45:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Enable HTTP Health Check for OTelTraceSource and OTelMetricsSource. (#1547)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 29 Jun 2022 11:11:48 -0500
    
    
    * Enable HTTP Health Check for OTelTraceSource and OTelMetricsSource.
    * Updated Readme file and added unit test for configurations
     Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    

* __Version bump to 2.0.0-SNAPSHOT on the main branch. (#1534)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 24 Jun 2022 11:28:06 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes a bug in the SqsWorkerIT where an NPE occurs. Updated the README.md to include the new steps. (#1538)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 24 Jun 2022 11:27:54 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added the auto-generated CHANGELOG for Data Prepper 1.5.0 (#1535)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 23 Jun 2022 11:12:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Created release notes for Data Prepper 1.5.0 (#1531)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 23 Jun 2022 09:23:47 -0500
    
    
    Created release notes for Data Prepper 1.5.0
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add in getHttpAuthenticationService to GrpcAuthenticationProvider (#1529)__

    [David Powers](mailto:ddpowers@amazon.com) - Wed, 22 Jun 2022 15:27:11 -0500
    
    
    * Add in getHttpAuthenticationService to GrpcAuthenticationProvider
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Updated the README.md for the S3 Source (#1530)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 22 Jun 2022 15:24:17 -0500
    
    
    Updated the README.md for the S3 Source to contain the documentation for the
    plugin.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use the Region supplied in the S3 configuration for the STS client (#1527)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 22 Jun 2022 11:56:44 -0500
    
    
    Use the Region supplied in the STS client so that users don&#39;t have to redefine
    the region multiple times. Renamed the AWS properties by removing the &#34;aws_&#34;
    prefix. Per PR discussions, allow the AWS region to be null and use a region as
    supplied by the SDK (often via the environment variable). Performed some
    refactoring to help with this.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use the SQS Queue URL account Id to get the bucket ownership (#1526)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 21 Jun 2022 16:00:08 -0500
    
    
    Use the SQS Queue URL account Id to get the bucket ownership. Pipeline authors
    can disable this so that no bucket validation is provided.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes a compiler error. (#1528)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 21 Jun 2022 11:28:25 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added Integration test for sqs (#1524)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 21 Jun 2022 09:36:18 -0500
    
    
    Added Integration test for sqs
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added SQS metrics (#1516)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 21 Jun 2022 09:35:56 -0500
    
    
    Added SQS metrics
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __S3 Source to include the bucket and key in Event (#1517)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 21 Jun 2022 09:16:05 -0500
    
    
    Updated the Event output by the S3 Source to include the bucket and key. Also,
    pushed the JSON data into the &#34;message&#34; property as indicated in the GitHub
    issue design.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix a bug where a null plugin setting throws an exception when attempting to validate that setting. Always return a non-null plugin configuration. (#1525)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Jun 2022 17:23:55 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the THIRD-PARTY file for 1.5.0. Resolves #1518 (#1522)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Jun 2022 11:33:30 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Hibernate Validator&#39;s DurationMin and DurationMax for Duration-based fields in the S3 Source configuration. (#1523)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Jun 2022 11:31:57 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __New metrics on the S3 source - Succeeded Count and Read Time Elapsed (#1505)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Jun 2022 09:27:31 -0500
    
    
    New metrics on the S3 source - S3 Objects succeeded and the read time elapsed
    to read and process an Object.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated call to s3 object worker (#1512)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 15 Jun 2022 10:29:34 -0500
    
    
    Updated call to s3 object worker
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated to Spring 5.3.21. Fixes #1390 (#1514)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 15 Jun 2022 10:10:11 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Validate bucket ownership when loading an object from S3 (#1510)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 15 Jun 2022 09:55:45 -0500
    
    
    When the bucket owner is available, then use the x-amz-expected-bucket-owner
    header on S3 GetObject requests to ensure that the bucket is owned by that
    expected owner. Resolves #1463
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated codec to be a PluginModel (#1511)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 14 Jun 2022 15:55:50 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump com.palantir.docker from 0.25.0 to 0.33.0 (#1306)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 14 Jun 2022 15:22:30 -0500
    
    
    Bumps com.palantir.docker from 0.25.0 to 0.33.0.
    
    ---
    updated-dependencies:
    - dependency-name: com.palantir.docker
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated the S3 source integration test to include JSON. This involved some refactoring to place some logic into the RecordsGenerator interface and implementations. (#1498)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Jun 2022 15:20:00 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Minor clean-up to the Performance Test compile GitHub Actions workflow: Renamed by removing redundant Data Prepper text and make it only run when either the performance-test directory changes or there is a change to the overall Gradle project. (#1509)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Jun 2022 18:51:08 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Uncompress s3object based on compression option (#1493)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 13 Jun 2022 17:45:03 -0500
    
    
    * Added s3 object decompression
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added delete SQS messages feature to SqsWorker (#1499)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 13 Jun 2022 17:44:25 -0500
    
    
    * Added delete functionality to sqs worker
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Create Events from the S3 source with the Log event type. (#1497)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Jun 2022 17:37:12 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the AWS SDK v2 to 2.17.209. (#1508)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Jun 2022 13:34:34 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump Armeria Version, solve SPI Issue (#1507)__

    [kmssap](mailto:100778246+kmssap@users.noreply.github.com) - Mon, 13 Jun 2022 10:27:20 -0500
    
    
    Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
     Co-authored-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten
    Schnitter &lt;k.schnitter@sap.com&gt;

* __Updated to Spotless 6.7.1 which no longer requires a work-around for JDK 17. Removed the work-around. (#1506)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Jun 2022 09:15:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-trace-raw-prepper (#1440)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 16:26:08 -0500
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.22.0 to
    3.23.1.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.22.0...assertj-core-3.23.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1385)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 16:25:29 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.11.20 to
    1.12.10.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.11.20...byte-buddy-1.12.10)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Moved two common test dependencies to the root project: Hamcrest and Awaitility. (#1502)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 10 Jun 2022 16:10:07 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1388)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 15:57:48 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.8 to 1.12.10.
    
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.8...byte-buddy-1.12.10)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added a counter metric for when the S3 Source fails to load or parse an object (#1483)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 10 Jun 2022 15:09:06 -0500
    
    
    Added a counter metric for when the S3 Source fails to load or parse an S3
    object.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Test against OpenDistro 1.3.0 (Elasticsearch 7.3.2) and updated the OpenSearch Sink documentation to note the minimum versions supported. (#1494)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 10 Jun 2022 14:03:05 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump assertj-core in /data-prepper-plugins/http-source (#1445)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 13:57:00 -0500
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.22.0 to
    3.23.1.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.22.0...assertj-core-3.23.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-trace-source (#1438)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 13:56:33 -0500
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.22.0 to
    3.23.1.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.22.0...assertj-core-3.23.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib in /data-prepper-plugins/mapdb-prepper-state (#1492)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 13:55:56 -0500
    
    
    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.6.20 to
    1.7.0.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.7.0/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.6.20...v1.7.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump awaitility in /data-prepper-plugins/otel-metrics-raw-processor (#1300)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 13:39:50 -0500
    
    
    Bumps [awaitility](https://github.com/awaitility/awaitility) from 4.1.1 to
    4.2.0.
    - [Release notes](https://github.com/awaitility/awaitility/releases)
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.1.1...awaitility-4.2.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump log4j-bom from 2.17.1 to 2.17.2 in /data-prepper-expression (#1283)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 12:03:20 -0500
    
    
    Bumps log4j-bom from 2.17.1 to 2.17.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-trace-raw-processor (#1451)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 12:01:12 -0500
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.22.0 to
    3.23.1.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.22.0...assertj-core-3.23.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-metrics-source (#1448)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 12:00:32 -0500
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.21.0 to
    3.23.1.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.21.0...assertj-core-3.23.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-metrics-raw-processor (#1455)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 12:00:00 -0500
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.21.0 to
    3.23.1.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.21.0...assertj-core-3.23.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump junit-jupiter-engine from 5.7.0 to 5.8.2 (#1491)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 10 Jun 2022 11:58:56 -0500
    
    
    Bumps [junit-jupiter-engine](https://github.com/junit-team/junit5) from 5.7.0
    to 5.8.2.
    - [Release notes](https://github.com/junit-team/junit5/releases)
    - [Commits](https://github.com/junit-team/junit5/compare/r5.7.0...r5.8.2)
    
    ---
    updated-dependencies:
    - dependency-name: org.junit.jupiter:junit-jupiter-engine
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Sqs worker improvements (#1479)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 9 Jun 2022 14:35:54 -0500
    
    
    * Added improvements to sqs worker
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump kotlin-stdlib-common from 1.6.10 to 1.7.0 (#1490)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 9 Jun 2022 14:11:36 -0500
    
    
    Bumps [kotlin-stdlib-common](https://github.com/JetBrains/kotlin) from 1.6.10
    to 1.7.0.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.7.0/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.6.10...v1.7.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Consolidate the AWS SDK v2 versions to the one defined in the root project BOM. (#1486)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 9 Jun 2022 13:28:46 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Consolidate the Jackson and Micrometer versions to the current latest for each. (#1485)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 9 Jun 2022 11:16:12 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump bcprov-jdk15on in /data-prepper-plugins/otel-metrics-source (#1301)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 9 Jun 2022 09:51:22 -0500
    
    
    Bumps [bcprov-jdk15on](https://github.com/bcgit/bc-java) from 1.69 to 1.70.
    - [Release notes](https://github.com/bcgit/bc-java/releases)
    -
    [Changelog](https://github.com/bcgit/bc-java/blob/master/docs/releasenotes.html)
    
    - [Commits](https://github.com/bcgit/bc-java/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.bouncycastle:bcprov-jdk15on
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump bcprov-jdk15on from 1.69 to 1.70 in /data-prepper-plugins/common (#791)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 17:06:46 -0500
    
    
    Bumps [bcprov-jdk15on](https://github.com/bcgit/bc-java) from 1.69 to 1.70.
    - [Release notes](https://github.com/bcgit/bc-java/releases)
    -
    [Changelog](https://github.com/bcgit/bc-java/blob/master/docs/releasenotes.html)
    
    - [Commits](https://github.com/bcgit/bc-java/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.bouncycastle:bcprov-jdk15on
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump hibernate-validator in /data-prepper-core (#1254)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 16:53:48 -0500
    
    
    Bumps [hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 7.0.2.Final to 7.0.4.Final.
    - [Release notes](https://github.com/hibernate/hibernate-validator/releases)
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/7.0.4.Final/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/7.0.2.Final...7.0.4.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump bcpkix-jdk15on from 1.69 to 1.70 in /data-prepper-plugins/common (#789)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 16:52:26 -0500
    
    
    Bumps [bcpkix-jdk15on](https://github.com/bcgit/bc-java) from 1.69 to 1.70.
    - [Release notes](https://github.com/bcgit/bc-java/releases)
    -
    [Changelog](https://github.com/bcgit/bc-java/blob/master/docs/releasenotes.html)
    
    - [Commits](https://github.com/bcgit/bc-java/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.bouncycastle:bcpkix-jdk15on
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __#970: Fixing OS dependent paths related to / in path. (#1482)__

    [Jeff Zemerick](mailto:13176962+jzonthemtn@users.noreply.github.com) - Wed, 8 Jun 2022 15:57:43 -0500
    
    
    Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;

* __Integration test to verify that the S3 source can load S3 objects (#1474)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 8 Jun 2022 15:56:11 -0500
    
    
    Created an integration test for verifying that the S3 source correctly
    downloads and parses S3 objects. Added some development documentation for the
    S3 Source since the integration tests need to be run manually.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump guava in /data-prepper-plugins/otel-metrics-raw-processor (#1293)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 14:21:42 -0500
    
    
    Bumps [guava](https://github.com/google/guava) from 31.0.1-jre to 31.1-jre.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.18 to 5.3.20 in /data-prepper-core (#1412)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 14:19:58 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.18 to 5.3.20.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.18...v5.3.20)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.18 to 5.3.20 in /data-prepper-core (#1413)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 14:17:50 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.18 to 5.3.20.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.18...v5.3.20)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.19 to 5.3.20 in /data-prepper-expression (#1449)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 14:17:18 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.19 to 5.3.20.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.19...v5.3.20)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.18 to 5.3.20 in /data-prepper-core (#1437)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 14:17:12 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.18 to 5.3.20.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.18...v5.3.20)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.19 to 5.3.20 in /data-prepper-expression (#1453)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Jun 2022 14:14:53 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.19 to 5.3.20.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.19...v5.3.20)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __#818: Moving to AWS SDK v2. (#1460)__

    [Jeff Zemerick](mailto:13176962+jzonthemtn@users.noreply.github.com) - Wed, 8 Jun 2022 12:54:13 -0500
    
    
    Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;

* __Created the S3 Source JSON codec (#1473)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 7 Jun 2022 20:44:14 -0500
    
    
    Created the S3 Source JSON codec. Resolves #1462
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added SQS interactions for S3 source (#1431)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 7 Jun 2022 15:37:09 -0500
    
    
    * Added sqs configuration and basic sqs interactions
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Run a GHA against OpenSearch 2.0.0 (#1467)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 3 Jun 2022 15:00:44 -0500
    
    
    Run a GHA against OpenSearch 2.0.0 to verify it works. Updated to use 1.3.2
    instead of 1.3.0.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Get S3 objects and support newline-delimited parsing in the S3 Source (#1465)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 3 Jun 2022 13:46:55 -0500
    
    
    Stream data from S3 and support newline-delimited parsing of S3 objects in the
    S3 Source plugin. This adds the Codec interface and the S3ObjectWorker class
    which is responsible for processing any given S3Object. Resolves #1434 and
    #1461.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __update README (#1471)__

    [David Powers](mailto:ddpowers@amazon.com) - Thu, 2 Jun 2022 15:46:04 -0500
    
    
    Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Update verbiage to show port (#1469)__

    [David Powers](mailto:ddpowers@amazon.com) - Thu, 2 Jun 2022 14:21:58 -0500
    
    
    * Update verbiage to show port
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Add health check to HTTP source (#1466)__

    [David Powers](mailto:ddpowers@amazon.com) - Thu, 2 Jun 2022 12:41:33 -0500
    
    
    * Add health check to HTTP source
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Bump jackson-databind in /data-prepper-plugins/drop-events-processor (#1446)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Jun 2022 14:06:26 -0500
    
    
    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.13.2.2 to
    2.13.3.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jackson-databind from 2.13.2.2 to 2.13.3 in /data-prepper-api (#1439)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Jun 2022 13:00:08 -0500
    
    
    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.13.2.2 to
    2.13.3.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jackson-databind from 2.13.2 to 2.13.3 in /data-prepper-expression (#1419)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Jun 2022 12:59:10 -0500
    
    
    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.13.2 to
    2.13.3.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Support building on JDK 17 (#1430)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 1 Jun 2022 06:38:56 -0500
    
    
    Provided a work-around suggested by the Spotless team to allow it to run
    successfully on JDK 17. This change allows developers to build Data Prepper
    using JDK 17. Updated the Spotless Gradle plugin, which also allows us to
    remove an older work-around related to cleaning the project root build
    directory. Updated the developer guide to clarify that Data Prepper can build
    with either JDK 11 or 17. Run the Gradle build and performance test builds on
    JDK 11 and 17 as part of the GitHub Actions CI.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support disabling any form of OpenSearch index management (#1420)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 31 May 2022 11:53:31 -0500
    
    
    Support using Data Prepper without any form of OpenSearch index management
    through the addition of the management_disabled index_type. Resolves #1051.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Enhancement: support custom metric tags (#1426)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Thu, 26 May 2022 16:58:21 -0500
    
    
    * ENH: support custom metric tags
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: TODO and variables
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: test assertion
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: update server configuration docs
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: wording
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Added authentication for S3 source (#1421)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 25 May 2022 09:49:57 -0700
    
    
    * Added aws authentication for s3-source
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added s3 source boilerplate (#1407)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 23 May 2022 15:40:48 -0700
    
    
    * Added s3 source boilerplate
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Removed access key from authentication config
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Make ContextManager public (#1416)__

    [David Powers](mailto:ddpowers@amazon.com) - Fri, 20 May 2022 16:54:03 -0500
    
    
    * Make ContextManager public
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Feature: EMFLoggingMeterRegistry (#1405)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Fri, 20 May 2022 09:46:52 -0500
    
    
    * MAINT: register logging meter
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: unused imports
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ADD: CFN template
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: checkpoint
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * CLN: EMFLoggingMeterRegistry
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * CLN: scratch classes
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: access modifier and style
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * TST: EMFMetricUtilsTest
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: change namespace back
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: final modifier
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: prefix name
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * TST: EMFLoggingRegistryConfig::testDefault
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: make accessible for tests
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * TST: EMFLoggingMeterRegistry
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * TST: test cases for create EMFLogging
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: recover jacoco threshold
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: unused imports
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: delete irrelevant cfn
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * EXP: DP monitoring
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: javadoc
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: unused logger
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: should not change release
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: centralize service_name
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: unused import
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: enrich test clamp magnitude
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: package private
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: rename meter registry type
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: javadoc for clampMetricValue
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: throw checked exception
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: config-file-value
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: filename
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: HasMetric -&gt; hasMetric
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Build on Java 11 with Java 8 as the compilation toolchain (#1406)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 19 May 2022 16:53:07 -0500
    
    
    Updated the project for building on Java 11 using Java 8 as the toolchain for
    compilation. Updated GitHub Actions to build using Java 11 and for end-to-end
    tests, run Data Prepper against multiple versions of Java.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Version bump to 1.5.0 on the main branch. (#1403)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 17 May 2022 15:13:14 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added change log for 1.4.0 (#1401)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 17 May 2022 13:37:16 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added release notes for 1.4.0 (#1398)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 17 May 2022 09:58:41 -0500
    
    
    * Added release notes for 1.4.0
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix flaky e2e tests (#1382)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 16 May 2022 14:59:39 -0500
    
    
    * Fix flaky e2e tests
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __update thirdparty dependency report (#1397)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 13 May 2022 17:32:07 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Improve OpenSearch sink performance by creating a customer JsonpMapper which avoids re-serializing bulk documents. (#1391)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 May 2022 16:52:17 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Sets a 30 minute timeout on each job in the GitHub Actions release process. Sometimes the smoke tests run on and never complete. This should help close those Actions out quickly and automatically. (#1392)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 May 2022 13:12:58 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix the file uploads to S3 with Gradle 7. This is done by changing the plugin used for uploading the S3. The original plugin is unmaintained and does not support Gradle 7. (#1383)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 May 2022 09:53:12 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Migrated Data Prepper to use the opensearch-java client for bulk requests rather than the REST High Level Client. #1347 (#1381)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 May 2022 09:39:42 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated to Gradle 7 (version 7.4.2) (#1377)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 6 May 2022 10:50:29 -0500
    
    
    Updated to Gradle 7, specifically at 7.4.2 which is the current latest version.
    
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated README.md links (#1376)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Thu, 5 May 2022 09:19:02 -0500
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __adding needs-documentation label support (#1373)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 3 May 2022 09:21:39 -0500
    
    
    resolves #1326
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.18 to 5.3.19 in /data-prepper-expression (#1369)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 2 May 2022 13:41:13 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.18 to 5.3.19.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.18...v5.3.19)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.13 to 5.3.19 in /data-prepper-expression (#1368)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 2 May 2022 11:55:00 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.13 to 5.3.19.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.13...v5.3.19)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Run OpenSearch sink integration tests against more versions of OpenDistro. In order to support this range of versions, the code to wipe indices must use the normal Get Indices API since it has supported the expand_wildcards query parameter longer than the _cat/indices API has supported it. (#1348)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 2 May 2022 09:29:21 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump antlr4 from 4.9.2 to 4.9.3 in /data-prepper-expression (#1289)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 29 Apr 2022 16:06:55 -0500
    
    
    Bumps [antlr4](https://github.com/antlr/antlr4) from 4.9.2 to 4.9.3.
    - [Release notes](https://github.com/antlr/antlr4/releases)
    - [Changelog](https://github.com/antlr/antlr4/blob/master/CHANGES.txt)
    - [Commits](https://github.com/antlr/antlr4/compare/4.9.2...4.9.3)
    
    ---
    updated-dependencies:
    - dependency-name: org.antlr:antlr4
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib in /data-prepper-plugins/mapdb-prepper-state (#1297)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 29 Apr 2022 16:03:32 -0500
    
    
    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.6.10 to
    1.6.20.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.6.10...v1.6.20)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump micrometer-core in /data-prepper-plugins/opensearch (#1317)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 29 Apr 2022 16:01:38 -0500
    
    
    Bumps [micrometer-core](https://github.com/micrometer-metrics/micrometer) from
    1.7.5 to 1.8.5.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.7.5...v1.8.5)
    
    
    ---
    updated-dependencies:
    - dependency-name: io.micrometer:micrometer-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump awaitility in /data-prepper-plugins/otel-trace-raw-processor (#1278)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 29 Apr 2022 16:00:47 -0500
    
    
    Bumps [awaitility](https://github.com/awaitility/awaitility) from 4.1.1 to
    4.2.0.
    - [Release notes](https://github.com/awaitility/awaitility/releases)
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.1.1...awaitility-4.2.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump awaitility in /data-prepper-plugins/otel-trace-raw-prepper (#1252)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 29 Apr 2022 15:59:47 -0500
    
    
    Bumps [awaitility](https://github.com/awaitility/awaitility) from 4.1.1 to
    4.2.0.
    - [Release notes](https://github.com/awaitility/awaitility/releases)
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.1.1...awaitility-4.2.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump awaitility in /data-prepper-plugins/peer-forwarder (#1251)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 29 Apr 2022 15:59:12 -0500
    
    
    Bumps [awaitility](https://github.com/awaitility/awaitility) from 4.1.1 to
    4.2.0.
    - [Release notes](https://github.com/awaitility/awaitility/releases)
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.1.1...awaitility-4.2.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Upload and publish the JUnit test reports for some tests (#1336)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 29 Apr 2022 11:58:16 -0500
    
    
    Upload and publish the JUnit test reports for the Gradle tests so that it is
    easier to track down issues. Additionally, build the Gradle GitHub Action for
    all pushes since this is the core build for the project. Renamed the Gradle
    build to be more compact and easier to find in the list of runs. Upload and
    publish JUnit reports for OpenSearch sink integration tests. Updated the
    Developer Guide with some information on Data Prepper continuous integration,
    including information on how to find unit test results.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Made the BulkRetryStrategyTests less reliant on implementation specifics from OpenSearch (#1346)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 29 Apr 2022 10:59:49 -0500
    
    
    Updated the BulkRetryStrategyTests to rely less on specific details from the
    the implementation of the bulk client in OpenSearch. This change works for both
    OpenSearch 1 and 2. Updated to use JUnit 5 as well, and some other refactoring.
    
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated Mockito in the opensearch plugin. This fixes some incompatibilities with upcoming versions of OpenSearch. (#1339)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 28 Apr 2022 14:57:55 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump slf4j-simple in /data-prepper-logstash-configuration (#1287)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 26 Apr 2022 15:45:00 -0500
    
    
    Bumps [slf4j-simple](https://github.com/qos-ch/slf4j) from 1.7.32 to 1.7.36.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.32...v_1.7.36)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-simple
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump slf4j-api from 1.7.32 to 1.7.36 (#1121)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 26 Apr 2022 15:44:26 -0500
    
    
    Bumps [slf4j-api](https://github.com/qos-ch/slf4j) from 1.7.32 to 1.7.36.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.32...v_1.7.36)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-api
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Removed the OpenSearch build-tools Gradle plugin from the OpenSearch plugin (#1327)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 26 Apr 2022 12:27:06 -0500
    
    
    Removed the OpenSearch build-tools Gradle plugin from the OpenSearch plugin&#39;s
    Gradle build. Moved the OpenSearch integration test components into their own
    source set. Made some Checkstyle fixes now that Checkstyle is running against
    this plugin. Fixes #593. Include formerly transitive dependencies into the
    end-to-end tests and Zipkin projects to get the build running again.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Decoupled OpenSearchSinkIT from the OpenSearch Core test cases (#1325)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 25 Apr 2022 16:29:28 -0500
    
    
    Decoupled OpenSearchSinkIT from the OpenSearch core test cases. Added
    OpenSearchIntegrationHelper to clean up some of the changes to
    OpenSearchSinkIT. This includes some TODO items for future improvements and
    consolidation with OpenSearch. Also clean out templates and wait for tasks to
    complete in between tests.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use full links for prcoessor READMEs 1.3 (#1324)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 22 Apr 2022 11:47:06 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Use MatcherAssert.assertThat in OpenSearchSinkIT. This reduces methods used from the base class which we will need to eventually remove. (#1323)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 21 Apr 2022 13:26:26 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Documentation of performance test 1.3 (#1309)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 21 Apr 2022 11:24:47 -0500
    
    
    Documentation of performance test 1.3
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bump spring-test from 5.3.15 to 5.3.18 in /data-prepper-expression (#1288)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 20 Apr 2022 10:41:17 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.15 to 5.3.18.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.15...v5.3.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.15 to 5.3.18 in /data-prepper-expression (#1290)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 20 Apr 2022 09:14:46 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.15 to 5.3.18.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.15...v5.3.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Minor clean-up to build files which load the OpenSearch version. Removed legacy configuration which allowed for configuring the groupId for OpenSearch as it is not needed. (#1315)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 19 Apr 2022 19:54:36 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the Dependabot configuration with some missing projects. (#1276)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 7 Apr 2022 12:33:49 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __TST: trace event migration backward compatibility e2e tests (#1264)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Thu, 7 Apr 2022 09:31:56 -0500
    
    
    * MAINT: additional 3 e2e tests
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * REF: e2e tests task definition and README
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: github workflow
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: spotless
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: window_duration
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * CLN: PR comments
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Bump jackson-databind in /data-prepper-plugins/drop-events-processor (#1261)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Apr 2022 10:55:07 -0500
    
    
    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.13.2 to
    2.13.2.2.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jackson-databind from 2.13.2 to 2.13.2.2 in /data-prepper-api (#1250)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Apr 2022 10:39:55 -0500
    
    
    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.13.2 to
    2.13.2.2.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __OTel Metric fixes (#1271)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 Apr 2022 09:45:45 -0500
    
    
    * Fixed the main Data Prepper build by fixing Javadoc errors in
    OTelMetricsProtoHelper.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Fix the Data Prepper end-to-end tests by using Armeria 1.9.2 in OTel Metrics
    Raw Processor.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Maintenance: add custom metrics in otel-trace-source README (#1246)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Tue, 5 Apr 2022 14:51:45 -0500
    
    
    * MAINT: add custom metrics in README
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: duplicate metric
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Support OpenTelemetry Metrics (#1154)__

    [kmssap](mailto:100778246+kmssap@users.noreply.github.com) - Tue, 5 Apr 2022 14:08:33 -0500
    
    
    * Support OpenTelemetry Metrics
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix WhiteSource Security Check
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Bump protobuf and armeria versions
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Review comment: Fix comment header
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Change Port Number of MetricsSource, Fix Names
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Rename Histogram Bucket Bounds
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add Summary Test, Remove OTel internal class
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add tests for metrics source plugin
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Refactor Plugin for Event Model
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add DCO to new classes, Remove unused imports
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add tests, improve coverage of metrics-raw-processor
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Refactor bucket, rename bucket fields
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Refactor Quantiles, introduce Interface
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Package Protect ParameterValidator
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add tests for builders, fix tests
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix Typo
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Increase API test coverage
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Address Checkstyle findings
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add count field to histogram
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix minor documentation issues and variable names
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Adapt histogram bucket algorithm to metrics.proto
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
     Co-authored-by: Kai Sternad &lt;kai@sternad.de&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __fixes link to NOTICE file (#1268)__

    [Kyle J. Davis](mailto:halldirector@gmail.com) - Mon, 4 Apr 2022 14:44:34 -0500
    
    
    Signed-off-by: Kyle J. Davis &lt;kyledvs@amazon.com&gt;

* __Bump spring-test from 5.3.16 to 5.3.18 in /data-prepper-core (#1255)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 4 Apr 2022 12:00:46 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.16 to 5.3.18.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.16...v5.3.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.16 to 5.3.18 in /data-prepper-core (#1253)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 4 Apr 2022 11:34:52 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.16 to 5.3.18.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.16...v5.3.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.16 to 5.3.18 in /data-prepper-core (#1256)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 4 Apr 2022 11:34:38 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.16 to 5.3.18.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.16...v5.3.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Maintenance: peer forwarder from trace event migration branch (#1239)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Fri, 1 Apr 2022 09:16:06 -0500
    
    
    * MAINT: migrate and adapt to both ExportTraceServiceRequest and event
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: merge test cases on event
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: unsupported record data type
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: TODO
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Maintenance: add OTelTraceGroupProcessor from trace ingestion migration branch (#1224)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 30 Mar 2022 17:17:54 -0500
    
    
    * ADD: otel-trace-group-processor
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: update header
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: migrate to processor interface
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: README and renaming plugin
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: fix plugin names in README
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * REF: normalizeDateTime
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Maintenance: add OTelTraceRawProcessor from trace ingestion migration branch (#1223)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 30 Mar 2022 15:47:33 -0500
    
    
    * ADD: OTelTraceRawProcessor
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: update header and dependency
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: use new processor interface
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: prepper -&gt; processor misses
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: metrics rephrase
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Maintenance: adjust otel-trace-source from trace event migration branch (#1241)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Mon, 28 Mar 2022 14:28:38 -0500
    
    
    * FEAT: support recordType for otel-trace-source
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: fix default value for recordType
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: update README with new config
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: zipkin research
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: use Jackson codec on enum
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: unused import
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: unused import
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: remove unused method
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Fix confusion in Log Ingestion Demo Guide where Docker prepends folder to network name (#1242)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 28 Mar 2022 10:43:16 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Maintenance: migrate service map stateful to accept both Event and ExportTraceServiceRequest as record data type (#1237)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Mon, 28 Mar 2022 08:59:06 -0500
    
    
    * MAINT: adapt input and output data type
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: TODO comment
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * TST: fix and cover service-map-prepper
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: AbstractPrepper -&gt; AbstractProcessor
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: type in benchmark
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: reset clock
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ENH: use real time for testing
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: magic number and string to constants
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Bump minimist from 1.2.5 to 1.2.6 in /release/staging-resources-cdk (#1244)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 25 Mar 2022 19:02:36 -0500
    
    
    Bumps [minimist](https://github.com/substack/minimist) from 1.2.5 to 1.2.6.
    - [Release notes](https://github.com/substack/minimist/releases)
    - [Commits](https://github.com/substack/minimist/compare/1.2.5...1.2.6)
    
    ---
    updated-dependencies:
    - dependency-name: minimist
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add an integration test which tests against OpenSearch 1.3.0. (#1232)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 25 Mar 2022 16:18:54 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Remove faker dependency (#1213)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Fri, 25 Mar 2022 16:01:27 -0500
    
    
    * Removed Faker
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Update 1.3 ChangeLog to include backported commits (#1235)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 23 Mar 2022 17:00:04 -0500
    
    
    Update 1.3 ChangeLog to include backported commits
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Maintenance: add otel-proto-common from trace ingestion migration branch (#1220)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 23 Mar 2022 14:38:50 -0500
    
    
    * MAINT: migrate otel-proto-common
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: bump protobuf to remove vulnerability
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: javadoc
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Updated 1.3.0 release date to Mar 22. (#1233)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Mar 2022 12:49:18 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bug where a group can be concluded twice in the Aggregate Processor (#1229)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 21 Mar 2022 14:03:28 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix incorrect key-value documentation (#1222)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 18 Mar 2022 16:41:37 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Smoke test tar (#1200)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Fri, 18 Mar 2022 09:51:18 -0500
    
    
    * Added tar smoke test
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __MAINT: cherry-pick changes on event model from trace migration branch (#1216)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Thu, 17 Mar 2022 19:20:18 -0500
    
    
    * MAINT: remove unused fromSpan
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: unnecessary change of import order
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Next Data Prepper version: 1.4.0-SNAPSHOT (#1210)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 17 Mar 2022 16:13:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added drop event conditional examples (#1214)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Thu, 17 Mar 2022 14:38:46 -0500
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Add in clarification sentence (#1208)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Thu, 17 Mar 2022 11:35:04 -0500
    
    
    * Add in clarification sentence
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Fixed broken links (#1205)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Wed, 16 Mar 2022 17:29:30 -0500
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __FIX: remove extra quotes in string literal (#1207)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 16 Mar 2022 16:25:15 -0500
    
    
    * FIX: remove extra quotes in string literal
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: ParseTreeCoercionServiceTest
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Fix checkstyle error (#1203)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Wed, 16 Mar 2022 15:14:24 -0500
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;


