
* __Add 2.4 release notes (#3220) (#3222)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 23 Aug 2023 09:16:00 -0500
    
    EAD -&gt; refs/heads/2.4, refs/remotes/upstream/2.4
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 990497a91c5df9d45cc71b1ed96f33ec0cd7f6e4)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Kafka source fixes: commit offsets, consumer group mutations, consumer shutdown (#3207) (#3218)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 22 Aug 2023 15:27:14 -0700
    
    
    Removed acknowledgments_timeout config from kafka source
     Signed-off-by: Hardeep Singh &lt;mzhrde@amazon.com&gt;
    (cherry picked from commit b5443634a4704cec4c33ec386747e12270aed073)
     Co-authored-by: Hardeep Singh &lt;mzhrde@amazon.com&gt;

* __Updates documentation for the Avro codec and S3 sink. Resolves #3162. (#3211) (#3219)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 22 Aug 2023 15:18:17 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 3d67468ab1ec7008378410fd14bb2e8d6742f9a2)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Catch exceptions when writing to the output codec and drop the event. (#3210) (#3216)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 22 Aug 2023 15:17:57 -0700
    
    
    Catch exceptions when writing to the output codec and drop the event. Correctly
    release failed events in the S3 sink.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit c1cbb22fc5a80003d881a66bd028772909b33bba)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Set release version to 2.4 (#3214)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 22 Aug 2023 15:16:28 -0700
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Removes some experimental projects from the 2.4 release and branch. (#3217)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Aug 2023 15:15:21 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Generated THIRD-PARTY file for fecb842 (#3212) (#3213)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 22 Aug 2023 14:20:56 -0700
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;
    (cherry picked from commit 733e7bc8381d1fe02c30b6c75c4cc2dd8984c21c)
     Co-authored-by: opensearch-trigger-bot[bot]
    &lt;98922864+opensearch-trigger-bot[bot]@users.noreply.github.com&gt;

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
    
    efs/heads/3158-avro-schema
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
    
    efs/heads/csv-validation-up-front
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


