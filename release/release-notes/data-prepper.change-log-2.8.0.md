
* __Adding &#39;single_word_only&#39; option to obfuscate processor (#4476) (#4550)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 16 May 2024 09:37:36 -0700
    
    EAD -&gt; refs/heads/2.8, tag: refs/tags/2.8.0, refs/remotes/origin/2.8
    Adding &#39;single_word_only&#39; option to obfuscate processor
     Signed-off-by: Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;
    (cherry picked from commit 6d48efba0d71ae0e0674b65ef33ce492b8b215ee)
     Co-authored-by: Utkarsh Agarwal
    &lt;126544832+Utkarsh-Aga@users.noreply.github.com&gt;

* __Generated THIRD-PARTY file for cedaf87 (#4548)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 15 May 2024 15:36:08 -0700
    

    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Updates werkzeug to 3.0.3 in examples to fix CVE-2024-34069. Resolves #4515 (#4546) (#4547)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 15 May 2024 15:26:50 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 906e8255f3440faa90bc2cde4cbd6ffa76ba004c)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Change version for 2.8 release to be 2.8.0 (#4540)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 15 May 2024 10:55:21 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Logging update and config validation (#4541) (#4542)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 15 May 2024 10:54:55 -0700
    
    
    * Logging improvements for export and stream processing for DocumentDB source
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add validation for DocumentDB Collection Config
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    (cherry picked from commit 94fa30d74d5857a19110eb04e959d990b755b48e)
     Co-authored-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add aggregate metrics (#4531) (#4537)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 14 May 2024 17:43:09 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    (cherry picked from commit 0495fae1ae0c6eb36a45e587de89932892e5cbbb)
     Co-authored-by: Hai Yan &lt;8153134+oeyh@users.noreply.github.com&gt;

* __Adds an ndjson input codec. This reads JSON objects for ND-JSON and more lenient formats that do not have the newline. (#4533) (#4536)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 14 May 2024 14:17:04 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 59e9fd6227117d3b309975256e87b1ab324d6e30)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Address route and subpipeline for pipeline tranformation (#4528) (#4535)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 14 May 2024 12:15:31 -0700
    
    
    Address route and subpipeline for pipeline tranformation
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    (cherry picked from commit e35b4eaf26af79d67834c0f183a846f501be4c16)
     Co-authored-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;

* __Fix an issue that exception messages are masked (#4416)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 13 May 2024 16:25:42 -0500
    
    
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


