
* __Add retry to make sure source is not shutdown when exceptions are thrown on the main thread (#5029)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Thu, 10 Oct 2024 19:47:40 -0700
    
    EAD -&gt; refs/heads/add-change-log-2.10, refs/heads/main
    * Add retry to make sure source is not shutdown when exceptions are thrown on
    the main thread
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    ---------
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __New OpenSearch API source implementation (#5024)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Thu, 10 Oct 2024 19:45:54 -0700
    
    
    * Create kinesis-source-integration-tests.yml
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    Signed-off-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;
    
    * Update kinesis-source-integration-tests.yml
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    Signed-off-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;
    
    * OpenSearch API source implementation
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    Signed-off-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;
    
    * Refactoring http source functionality and address comments
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    Signed-off-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;
    
    * Fix the build break
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    Signed-off-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;
    
    * Revert &#34;Create kinesis-source-integration-tests.yml&#34;
     This reverts commit c52f584b61d100935e970ccfd345f6c7d2ddd117.
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Fix the code.
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Disable the health check endpoint.
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Fix checkstyle imports.
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    ---------
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    Signed-off-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;
    Co-authored-by: Souvik Bose &lt;souvbose@amazon.com&gt;

* __Adding support for compression config per stream. (#5046)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Thu, 10 Oct 2024 19:20:41 -0700
    
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Rename convert_entry_type to convert_type and keep the original name as a deprecated name. (#5047)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 10 Oct 2024 21:08:33 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding Kinesis README (#5048)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Thu, 10 Oct 2024 21:08:17 -0500
    
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Adding ttl_delete parameter to metadata for DynamoDB (#4982)__

    [Lee](mailto:leeroyhannigan@yahoo.ie) - Thu, 10 Oct 2024 15:11:27 -0500
    
    
    Signed-off-by: Lee Hannigan &lt;lhnng@amazon.com&gt;

* __Fix service map tests (#5043)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 10 Oct 2024 12:28:07 -0500
    
    
    

* __Support secret rotation in RDS source (#5016)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 10 Oct 2024 12:04:07 -0500
    
    
    * Add stream worker refresher
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update unit tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __FIX: mutate string processor configs (#5042)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 10 Oct 2024 09:37:26 -0700
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add NotEmpty annotation to grok match parameter (#5033)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 9 Oct 2024 15:03:34 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __ENH: expose dbPath to be configurable (#5037)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 9 Oct 2024 13:54:25 -0500
    
    
    * ENH: expose dbPath to be configurable
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Adding a metadata attribute for sequence number of record (#5036)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Wed, 9 Oct 2024 11:26:30 -0700
    
    
    * Adding a metadata attribute for sequence number of record
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Add test to check for the metadata
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    ---------
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Documentation improvements for the aggregate processor. (#5035)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 9 Oct 2024 09:28:54 -0700
    
    
    Adds property and class description to configurations. Corrects property order.
    Adds configuration classes with documentation for put_all and remove_duplicates
    which now allows for including these. Corrects use of enums and using
    @JsonValue to have usable documentation on these enums.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix the build break for kinesis source (#5034)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Tue, 8 Oct 2024 14:18:12 -0700
    
    efs/heads/base64-codec
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Documentation improvements for a number of Data Prepper processors: Re-ordering properties; adding missing documentation; missed enums. (#5026)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 8 Oct 2024 11:22:50 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Documentation improvements for a number of Data Prepper processors. (#5023)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 7 Oct 2024 13:22:45 -0700
    
    
    Documentation improvements for a number of Data Prepper processors.
     Updates some enumerations used in processors to support the @JsonValue.
    Corrects @JsonClassDescription to use HTML rather than Markdown.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Documentation improvements for a number of Data Prepper processors. (#5025)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 7 Oct 2024 13:17:55 -0700
    
    
    Adds missing enumerations in the key-value processor to support better
    documentation. Corrects @JsonClassDescription to use HTML rather than Markdown.
    This set of changes is for key_value, flatten, translate, parse_json,
    parse_xml, parse_ion, and csv. Also, this adds documentation to the csv input
    codec.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add integration test for Json codec in S3 sink (#4411)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 7 Oct 2024 11:19:00 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Make acknowledgment_timeout configurable for s3 scan source, configure timeout to 10 minutes for MongoDB L2 transform (#4988)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 7 Oct 2024 11:18:13 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Caching implementation of EventKeyFactory (#4843)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 7 Oct 2024 09:01:54 -0700
    
    
    Create a caching implementation of EventKeyFactory that will cache a
    configurable number of EventKeys.
     Refactored the approach to loading the EventKeyFactory in the application
    context for a couple of reasons: 1. Allow for skipping the
    CachingEventKeyFactory if not needed; 2. Help it run better in the
    data-prepper-test-event project.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update the test logging to include the failed assertion to help investigations, especially during CI builds on GitHub Actions. (#4987)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 7 Oct 2024 07:24:46 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Providing an option for the plugins to use Spring DI (#5012)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Fri, 4 Oct 2024 11:40:57 -0700
    
    
    providing an option for the plugins to use Spring DI
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
     Update
    data-prepper-api/src/main/java/org/opensearch/dataprepper/model/annotations/DataPrepperPlugin.java
    
     modified the comment line based on the suggession
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;
    Signed-off-by: Santhosh Gandhe
    &lt;1909520+san81@users.noreply.github.com&gt;
     Integration test to validate the DI context enabling in plugins
    
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Remove references to deprecated processor names `otel_trace_raw` and `service_map_stateful` (#5017)__

    [Ling Hengqian](mailto:linghengqian@outlook.com) - Thu, 3 Oct 2024 13:32:21 -0700
    
    
    Signed-off-by: linghengqian &lt;linghengqian@outlook.com&gt;

* __Updates to JSON schema and Data Prepper documentation. (#5019)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 3 Oct 2024 11:00:32 -0700
    
    
    Support @JsonValue for determining enumeration values in the JSON Schema.
    Provide a JSON schema type of string for EventKey objects. Documentation
    wording improvements to the mutate event and mutate string processors.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add OpenTelemetry Metrics example (#5014)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Thu, 3 Oct 2024 19:24:09 +0200
    
    
    Add OpenTelemetry Metrics example
     Provides a small Docker Compose setup to show metrics ingestion
    with Data
    Prepper. This example is similar to the traces and logs
    examples. It also
    shows the metrics Data Prepper emits on its metrics
    endpoint.
     Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    

* __ENH: support plugin loading in conifg (#4974)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 2 Oct 2024 16:00:58 -0500
    
    
    ---------
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Updates JsonPropertyDescription annotations to use HTML rather than Markdown. (#4985)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 2 Oct 2024 09:52:26 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding a new API to get the current transformed pipelines as a JSON (#4980)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Tue, 1 Oct 2024 11:34:43 -0700
    
    
    * Adding a new API to get the current transformed pipelines as a JSON
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Rename the api and address comments
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    ---------
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Add: trace peerforwarder config (#4992)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 1 Oct 2024 10:12:09 -0500
    
    
    * ADD: trace peerforwarder config
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    
    ---------
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __FIX: missing required fields annotation (#4990)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 1 Oct 2024 09:55:58 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: update required properties and property descriptions in some processors (#4989)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Mon, 30 Sep 2024 14:58:12 -0500
    
    
    * update rename key config with missing descriptions
    
    * update convert entry type required fields
    
    * update delay property description
    
    * putting convert entry type processor on hold due to the mutually exclusive
    properties
     Signed-off-by: Katherine Shen &lt;katshen@amazon.com&gt;
    
    ---------
     Signed-off-by: Katherine Shen &lt;katshen@amazon.com&gt;

* __Additional logging when shutting down the pipeline. (#4986)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 30 Sep 2024 08:59:14 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support convert entry type on arrays (#4925)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 27 Sep 2024 12:12:43 -0700
    
    
    * Support convert entry type on arrays
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Support BigDecimal data type in expressions (#4930)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 27 Sep 2024 09:04:09 -0700
    
    
    * Support BigDecimal data type in expressions
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed unused import
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __OpenSearchSink - Enhance logs to include index name and last exception information (#4841)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 27 Sep 2024 09:03:47 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Delete .github/workflows/static.yml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * OpenSearchSink - Enhance logs to include index name and last exception
    information
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Rebased to latest and cleanup messages
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed test errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add origination time to buffer event and populate the partition key (#4971)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Thu, 26 Sep 2024 12:32:09 -0500
    
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Add integration test for Kinesis source (#4967)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Tue, 24 Sep 2024 16:25:02 -0500
    
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;

* __Add Lambda Synchronous processor support (#4700)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Tue, 24 Sep 2024 11:01:45 -0700
    
    
    Add Lambda Processor Synchronous Mode support
    Make LambdaClientFactory common
    to sink and processor
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;

* __Variable drain timeouts when shutting down over HTTP shutdown. (#4970)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Sep 2024 16:03:21 -0700
    
    
    Variable drain timeouts when shutting down over HTTP shutdown.
     Adds two new parameters to the shutdown API. The first is bufferReadTimeout
    which controls the amount of time to wait for the buffer to be empty. The
    second is bufferDrainTimeout which controls the overall wait time for the
    process worker threads to complete.
     To support Data Prepper durations in HTTP query parameters, I extracted the
    parsing logic for durations out of DataPrepperDurationDeserializer and into a
    new DataPrepperDurationParser class.
     Resolves #4966.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add mongodb as alternate source name for documentdb source (#4969)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Sun, 22 Sep 2024 13:39:13 -0500
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add alternate name for Plugin (#4968)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Sat, 21 Sep 2024 16:24:34 -0700
    
    
    Add alternate name for Plugin
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Refactoring chunking in the HTTP source to improve the performance. (#4950)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 20 Sep 2024 14:00:50 -0700
    
    
    The HTTP source was parsing the entire message and then serializing from
    strings. This created a bit of memory churn and probably duplicate processing.
    The new approach is to chunk the message from the start which allows us to
    stream the reading and perform copies of data. This also has a JMH benchmark
    added which shows that this new approach doubles the number of operations per
    second.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update rds template with new s3 sink client options (#4963)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 20 Sep 2024 12:39:15 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix a bug with event listener in RDS source (#4962)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 20 Sep 2024 09:41:45 -0700
    
    
    Unregister eventListener on stop
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add s3 sink client options (#4959)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 19 Sep 2024 22:28:03 -0500
    
    
    * Add s3 sink client options
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add upper bounds for new options
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Support AWS Kinesis Data Streams as a Source (#4836)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Thu, 19 Sep 2024 15:23:02 -0700
    
    
    Support AWS Kinesis Data Streams as a Source
     Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;

* __Add acknowledgment progress check for s3 source with folder partitions (#4957)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 19 Sep 2024 10:06:46 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Shorten S3 prefix to meet the requirement of RDS export API (#4955)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 18 Sep 2024 14:22:37 -0500
    
    
    * Shorten prefix
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add unit tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Improve logging for exceptions (#4942)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Wed, 18 Sep 2024 09:31:49 -0700
    
    
    Improve logging for exceptions
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;

* __Add ReplaceStringProcessor for simple string substitution that doesn&#39;t involve regex (#4954)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 18 Sep 2024 10:18:08 -0500
    
    
    * Add ReplaceStringProcessor for simple string substitution that doesn&#39;t
    involve regex
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Remove unused imports
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Update when condition parameter name
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Kafka Plugin: support for SASL/SCRAM mechanisms (#4912)__

    [franky-m](mailto:franky.meier.1@gmx.de) - Tue, 17 Sep 2024 12:44:09 -0700
    
    
    Kafka Plugin SCRAM Support
     Signed-off-by: Franky Meier &lt;franky.meier.1@gmx.de&gt;
     Signed-off-by: Franky Meier &lt;franky.meier.1@gmx.de&gt;
    Signed-off-by: George
    Chen &lt;qchea@amazon.com&gt;
    Co-authored-by: Qi Chen &lt;qchea@amazon.com&gt;

* __Adds config transformation template for RDS source (#4946)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 17 Sep 2024 14:26:40 -0500
    
    
    * Add rules and templates for rds source
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update rds config to prep for template
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Refactor record converters
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add template
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Remove unused import
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Revert changes on prefix
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Decrease the sleep when pausing the Kafka consumer to 1 second when the circuit breaker is in use. (#4947)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 Sep 2024 13:30:13 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Release events for Noop Sink (#4944)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Thu, 12 Sep 2024 16:31:16 -0500
    
    
    Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;

* __Move CsvMapper and Schema creation to constructor (#4941)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 12 Sep 2024 10:10:10 -0700
    
    
    Move CsvMapper and Schema creation to constructor
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fixes Jaeger Hotrod demo failure on OpenSearch 2.16.0 (#4921)__

    [Ling Hengqian](mailto:linghengqian@outlook.com) - Wed, 11 Sep 2024 19:21:59 -0700
    
    
    Signed-off-by: linghengqian &lt;linghengqian@outlook.com&gt;

* __Updating the WhiteSource/Mend configuration to match the version found in opensearch-system-templates. (#4933)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 11 Sep 2024 13:48:12 -0700
    
    
    https://github.com/opensearch-project/opensearch-system-templates/blob/e3b4fc6/.whitesource
    
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __adding registry to image value in docker-compose.yaml (#2980)__

    [qhung](mailto:quanghung.b@gmail.com) - Tue, 10 Sep 2024 13:06:24 -0700
    
    
    Podman requires the registry url in order to pull out the image
     Signed-off-by: qhung &lt;11665161+quanghungb@users.noreply.github.com&gt;

* __Support start_time or range options for the first scan of scheduled s3 scan (#4929)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 10 Sep 2024 13:53:16 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add progress check callback to update partition ownership in S3 scan source (#4918)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 10 Sep 2024 11:14:00 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __ENH: add shutdown into extension plugin (#4924)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 10 Sep 2024 10:35:51 -0500
    
    
    * ENH: add shutdown into extension plugin
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Updated README.md according to previous changes of docker files. (#4845)__

    [Jayesh Parmar](mailto:89792517+jayeshjeh@users.noreply.github.com) - Mon, 9 Sep 2024 22:07:40 +0200
    
    
    * Updated README.md for updated /examples/log-ingestion files
     Signed-off-by: jayeshjeh &lt;jay.parmar.11169@gmail.com&gt;
    
    * Updated README.md for updated /examples/log-ingestion files
     Signed-off-by: jayeshjeh &lt;jay.parmar.11169@gmail.com&gt;
    
    * correction
     Signed-off-by: jayeshjeh &lt;jay.parmar.11169@gmail.com&gt;
    
    ---------
     Signed-off-by: jayeshjeh &lt;jay.parmar.11169@gmail.com&gt;

* __Updates to the code for HTTP chunking. (#4919)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 9 Sep 2024 09:27:44 -0700
    
    
    This refactors the code by placing all logic for serializing the data into the
    Codec itself. In so doing, it allows for improved testing such as symmetric
    testing. It also decouples the serialization format from the HTTP server. This
    also uses the Jackson library for the serialization which yields more accurate
    JSON.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __MAINT: add data prepper plugin schema module into build resource (#4920)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 6 Sep 2024 13:24:03 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Move event processing to separate threads and add event processing timer in RDS source (#4914)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 5 Sep 2024 10:58:59 -0500
    
    
    * Move event processing to separate threads and add event processing timer
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Corrects the S3SinkStack for AWS testing (#4913)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 4 Sep 2024 14:30:28 -0700
    
    
    Corrects the S3SinkStack for AWS testing.
     The S3SinkStack was not in use and didn&#39;t quite work. This corrects it so that
    we can use it to automate the tests for the S3 sink in GitHub.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use awaitility to read data in Kafka buffer tests to fix flakiness (#4703)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 4 Sep 2024 14:18:09 -0700
    
    
    Use awaitility to read data in KafkaBufferIT to promote stability and speed of
    execution. Contributes toward #4168
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds a .gitignore for Python virtual environments. (#4881)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 4 Sep 2024 12:24:52 -0700
    
    
    Some projects such as the trace-analytics-sample-app and the current smoke
    tests use Python. Using Python virtual environments lets developers use these
    without affecting their local Python environment. Ignore .venv directories
    which Python virtual environments use.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updating the instructions for releasing a new version of Data Prepper. (#4878)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 4 Sep 2024 12:24:25 -0700
    
    
    This adds instructions for the release setup so that maintainers can have a
    consistent set of instructions to follow. Specifically, it adds steps for
    setting up the branch, updating version numbers, creating the release notes and
    change log.
     Additionally, this fixes a broken fragment link to the Backporting section of
    the Developer Guide.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __ENH: respect JsonProperty defaultValue in JsonSchemaConverter (#4889)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 3 Sep 2024 10:38:08 -0500
    
    
    * ENH: respect JsonProperty defaultValue in JsonSchemaConverter
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Minor updates and bug fixes for RDS source (#4887)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 29 Aug 2024 13:20:47 -0500
    
    
    * Minor updates and bug fixes to prepare for performance testing
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Adds a script to help generate the Thank You text for the release blogs. (#4884)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Aug 2024 08:44:17 -0700
    
    
    Adds a script to help generate the Thank You text for the release blogs. Use en
    dashes in the Thank You text to meet the standards of the project-website. When
    there is no name for a GitHub user, don&#39;t include a None.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    Co-authored-by: Hai Yan
    &lt;oeyh@amazon.com&gt;

* __Creates the release notes for Data Prepper 2.9.0 (#4879)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Aug 2024 08:20:02 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __ENH: Plugin errors consolidator (#4863)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 27 Aug 2024 11:16:55 -0500
    
    
    * ENH: Plugin errors consolidator
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __REF: data prepper plugin schema (#4872)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 27 Aug 2024 09:57:53 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add e2e acknowledgment and checkpointing to RDS source (#4819)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 26 Aug 2024 17:37:22 -0500
    
    
    * Add acknowledgment and checkpoint for stream
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add unit tests for stream checkpoint
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add acknowledgment to export
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Simplify the stream checkpointing workflow
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * A few fixes and cleanups
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Extend lease while waiting for ack
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address more review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Update Spring to 5.3.39 to fix CVE-2024-38808. Require commons-configuration2 2.11.0 to fix CVE-2024-29131 and CVE-2024-29133. Hadoop pulls this dependency in. (#4874)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 26 Aug 2024 10:34:06 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates Bouncy Castle to 1.78.1. Update any projects that attempt to use Bouncy Castle jdk15on dependencies with the jdk18on dependency instead. This will prevent any of the older jdk15on dependencies from getting into our classpath. In particular, this was coming from hadoop-common. (#4871)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 26 Aug 2024 08:30:35 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Require dnsjava 3.6.1 to resolve CVE-2024-25638. This is a transitive dependency from Hadoop. (#4868)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 23 Aug 2024 14:49:17 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes the trace-analytics-sample-app from the examples that are provided in the release. (#4867)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 23 Aug 2024 14:49:10 -0700
    
    
    The motivation for removing this is that the samples pull in dependencies which
    often trigger CVE reports. It is not likely customers are trying to run this
    example from a Data Prepper deployment, especially since the example is not
    made to run from the installed version, but runs from Docker and runs using the
    latest 2.x version.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix visibility timeout errors (#4812) (#4831)__

    [Daniel Li](mailto:68623003+danhli@users.noreply.github.com) - Fri, 23 Aug 2024 11:59:12 -0700
    
    
    Fix visibility timeout errors (#4812)
     Signed-off-by: Daniel Li &lt;lhouqua@amazon.com&gt;

* __Change version in DataPrepper Version class to 2.10 (#4852)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 23 Aug 2024 11:35:50 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Delete .github/workflows/static.yml
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Change Data Prepper Version to 2.10
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Adds integration tests for pipeline connectors. (#4834)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 23 Aug 2024 07:58:01 -0700
    
    
    This commit adds integration testing for the pipeline connector sink/source
    which connects two pipelines. There are two tests. The first tests against a
    single connection with a single sink. The second test also includes a second
    sink to verify that pipeline connections work with additional sinks.
     This commit also includes fixes for CoreHttpServerIT. When running the new
    pipeline connector tests, the CoreHttpServerIT tests started failing. I found
    some places where shutdowns were not occurring and fixed those. And I added
    some additional logging to help debug. The root problem turned out to be that
    the ExecutorService used in the DataPrepperServer was a static field. The
    CoreHttpServerIT was working because it was the first test that JUnit chose.
    With the new tests, it is being chosen later and by that point, the static
    ExecutorService was shutdown. The fix is simply to avoid using a static
    ExecutorService.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Mask s3 object key in logs (#4861)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 22 Aug 2024 17:12:30 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Changelog for release 2.9 (#4855)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 22 Aug 2024 12:58:22 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __ENH: collect plugin config and loading errors during data prepper launch (#4816)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 21 Aug 2024 17:41:09 -0500
    
    
    * ENH: collect plugin errors within core application context
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __ENH: respect json order and class description on processors (#4857)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 21 Aug 2024 14:32:05 -0500
    
    
    * ENH: json order and class description on processors
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Address missing processor JsonPropertyDescriptions and validations (#4837)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 21 Aug 2024 10:03:27 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Catch error that could cause LeaderScheduler thread to crash (#4850)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 20 Aug 2024 19:43:10 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Change main branch version to 2.10-SNAPSHOT (#4851)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 20 Aug 2024 13:46:09 -0700
    
    
    Change main branch version to 2.10-SNAPSHOT
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Add support for AWS security lake sink as a bucket selector mode in S3 sink (#4846)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 20 Aug 2024 13:19:42 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Delete .github/workflows/static.yml
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for AWS security lake sink as a bucket selector mode in S3 sink
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed tests
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added javadoc for S3BucketSelector
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added new  tests for KeyGenerator
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added new  tests and fixed style errors
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed test build failure
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;


