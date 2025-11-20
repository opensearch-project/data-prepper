
* __Update the version to Data Prepper 2.13.0 (#6282)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 19 Nov 2025 12:11:12 -0800
    
    EAD -&gt; refs/heads/2.13, refs/remotes/upstream/2.13
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Add sanitization to metric names and labels (#6277)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 19 Nov 2025 11:15:49 -0800
    
    efs/remotes/origin/main, refs/remotes/origin/HEAD
    * Add sanitization to metric names and labels
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review  comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed build issues
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed unnecessary debug statement
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Introduce support for Opensearch data streams (#6249)__

    [Jonah Calvo](mailto:caljonah@amazon.com) - Mon, 17 Nov 2025 18:01:34 -0800
    
    
    Add OpenSearch Data Stream support with automatic action selection
    
    Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    

* __Do not wait for export to exit ShardConsumer for shards that have no records to write to buffer (#6265)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 14 Nov 2025 10:51:34 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add lookback time adjustment for Office 365 source plugin partition execution (#6266)__

    [alparish](mailto:152813728+alparish@users.noreply.github.com) - Thu, 13 Nov 2025 16:47:20 -0600
    
    
    Signed-off-by: Alekhya Parisha &lt;aparisha@amazon.com&gt; Co-authored-by: Alekhya
    Parisha &lt;aparisha@amazon.com&gt;

* __Standardize Exception handling in souce plugins (#6255)__

    [Vecheka](mailto:vecheka@amazon.com) - Thu, 13 Nov 2025 13:06:39 -0800
    
    
    Signed-off-by: Vecheka Chhourn &lt;vecheka@amazon.com&gt;

* __Remove unnecessary dependencies in Prometheus sink build (#6267)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 13 Nov 2025 13:04:10 -0800
    
    
    * Remove unnecessary dependencies
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed version for snappy
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix SQS exception counter mock setup in tests (#6256)__

    [mzurita-amz](mailto:mzurita@amazon.com) - Thu, 13 Nov 2025 07:51:36 -0800
    
    
    - Add lenient mocks for SQS counter metrics initialized in SqsWorker
    constructor
    - Fix test methods to use processSqsMessages() instead of run() to avoid
    infinite loops
    - Ensure all SQS exception tests properly verify counter increments
    - All 333 tests now pass
    
    Signed-off-by: Manuel Mangas Zurita &lt;mzurita@amazon.com&gt;

* __Add Prometheus Sink (#6229)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 12 Nov 2025 20:38:40 -0800
    
    
    * Add Prometheus Sink
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Remove debug statements
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Make Prometheus sink Experimental
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add sanitize_names config option to sanitize metric/label names
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Cleaned up HTTP sender and Sigv4Signer
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added check for https in valid config
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Delay 5 minutes in DimensionalTimeSliceCrawler for partition creation on live event (#6104)__

    [wjyao0316](mailto:88009805+wjyao0316@users.noreply.github.com) - Wed, 12 Nov 2025 20:31:34 -0600
    
    
    This commit add 5 minutes delay to partition creation on live event in 
    DimensionalTimeSliceCrawler.
    
    In general, newly generated events become queryable after 30 ~ 120 second. 
    Delay 5 minutes give enough time for the newly generated events to become
    queryable to largely reduce the possibility of losing events due to eventual
    consistency in vender API side.
    
    Signed-off-by: Wenjie Yao &lt;wjyao@amazon.com&gt; Co-authored-by: Wenjie Yao
    &lt;wjyao@amazon.com&gt;

* __Add metric tracking total number of open shards, do not skip shards just because there is no record at the ending sequence number of that shard (#6260)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 12 Nov 2025 19:16:57 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add configurable range parameter to Office 365 source plugin (#6261)__

    [alparish](mailto:152813728+alparish@users.noreply.github.com) - Wed, 12 Nov 2025 14:48:57 -0600
    
    
    Signed-off-by: Alekhya Parisha &lt;aparisha@amazon.com&gt;
    
    Co-authored-by: Alekhya Parisha &lt;aparisha@amazon.com&gt;

* __Add read failure metric to S3 source (#6258)__

    [mzurita-amz](mailto:mzurita@amazon.com) - Wed, 12 Nov 2025 14:44:01 -0600
    
    
    Signed-off-by: Manuel Mangas Zurita &lt;mzurita@amazon.com&gt;

* __Add EMF config that enables adding extra properties to the EMF record (#6259)__

    [mzurita-amz](mailto:mzurita@amazon.com) - Wed, 12 Nov 2025 11:57:55 -0800
    
    
    Signed-off-by: Manuel Mangas Zurita &lt;mzurita@amazon.com&gt;

* __Handle DynamoDB source leader exceptions correctly by attempting to reacquire partition (#6195)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 11 Nov 2025 23:00:34 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Get shard iterator at sequence number for last shard iterators on ending sequence numbers (#6251)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 11 Nov 2025 12:55:50 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add metrics in SecretsRefreshJob for SecretsManager exceptions (#6252)__

    [bninishi](mailto:bninishi@amazon.com) - Tue, 11 Nov 2025 06:39:32 -0800
    
    
    * Add metrics for SecretsManager ResourceNotFound and LimitExceeded exception
    

* __Add Retryable/Non-Retryable Exception + API Calls Metrics for O365 (#6238)__

    [Brett Zeligson](mailto:85852739+zeligsonbrett@users.noreply.github.com) - Mon, 10 Nov 2025 17:35:54 -0800
    
    
    Add Retryable/Non-Retryable Exception + API Calls Metrics for O365
    
    Signed-off-by: Brett Zeligson &lt;brettzel@amazon.com&gt; Signed-off-by: Vecheka
    &lt;vecheka@amazon.com&gt; Signed-off-by: Brett Zeligson
    &lt;85852739+zeligsonbrett@users.noreply.github.com&gt; Co-authored-by: Brett
    Zeligson &lt;brettzel@amazon.com&gt; Co-authored-by: Vecheka &lt;vecheka@amazon.com&gt;

* __Added API metrics to SQS common worker (#6248)__

    [mzurita-amz](mailto:mzurita@amazon.com) - Mon, 10 Nov 2025 16:41:06 -0800
    
    
    Signed-off-by: Manuel Mangas Zurita &lt;mzurita@amazon.com&gt;

* __Address typo and improve publishErrorTypeMetricCounter function (#6253)__

    [Vecheka](mailto:vecheka@amazon.com) - Fri, 7 Nov 2025 18:36:33 -0600
    
    
    Signed-off-by: Vecheka Chhourn &lt;vecheka@amazon.com&gt;

* __Added throttle metric to S3 input stream (#6245)__

    [mzurita-amz](mailto:mzurita@amazon.com) - Fri, 7 Nov 2025 06:13:52 -0800
    
    
    Signed-off-by: Manuel Mangas Zurita &lt;mzurita@amazon.com&gt;

* __Add worker retry mechanism for failed batch processing in LeaderOnlyTokenCrawler (#6244)__

    [alparish](mailto:152813728+alparish@users.noreply.github.com) - Thu, 6 Nov 2025 17:13:08 -0600
    
    
    Signed-off-by: Alekhya Parisha &lt;aparisha@amazon.com&gt; Co-authored-by: Alekhya
    Parisha &lt;aparisha@amazon.com&gt;

* __fix validation bug in delete entries config (#6243)__

    [Kennedy Onyia](mailto:145404406+kennedy-onyia@users.noreply.github.com) - Thu, 6 Nov 2025 10:19:55 -0600
    
    
    Signed-off-by: Kennedy Onyia &lt;kennedy.onyia@gmail.com&gt;

* __Adjust CWL Sink Threshold to allow minimal 10 second flush interval (#6242)__

    [wjyao0316](mailto:88009805+wjyao0316@users.noreply.github.com) - Wed, 5 Nov 2025 16:01:05 -0600
    
    
    **What?**
    
    This commit updates CWL sink minimal allowed flush interval from 60s to 10s.
    
    **Why?**
    
    CWL is a high TPS dataplane service with small batch of events. It is 
    reasonable to allow shorter flush interval to improve the ingestion latency
    
    Signed-off-by: Wenjie Yao &lt;wjyao@amazon.com&gt; Co-authored-by: Wenjie Yao
    &lt;wjyao@amazon.com&gt;

* __allow custom name pattern prefix for S3 sink (#6193)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Wed, 5 Nov 2025 12:49:33 -0600
    
    
    * allow custom name pattern for S3 sink
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    * update to allow name pattern prefix
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Increase m365 worker thread count from 2 to 4 (#6197)__

    [wjyao0316](mailto:88009805+wjyao0316@users.noreply.github.com) - Tue, 4 Nov 2025 18:26:49 -0600
    
    
    This commit increases the m365 connector worker thread count from 2 to 4. This
    is because the worker thread is per host and we need to ensure there are not
    too many threads in total to avoid api throttling.
    
    We had alignment between different teams that 3pE connector will be fixed at 2
    OCU. The performance test confirms a total of 8 threads cross all fleet.
    Changing to 4 would fit 2 OCU.
    
    Signed-off-by: Wenjie Yao &lt;wjyao@amazon.com&gt; Co-authored-by: Wenjie Yao
    &lt;wjyao@amazon.com&gt;

* __Increase test coverage and bug fix in JacksonEvent (#6181)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 3 Nov 2025 16:12:09 -0800
    
    
    Increase test coverage in JacksonEvent and fix a bug with trying to modify
    input maps which may be immutable.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use the STS region for the default AWS configuration when it is provided. Resolves #6068. (#6234)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 3 Nov 2025 16:08:51 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Integration tests for the DynamoDbSourceCoordinationStore (#6190)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 3 Nov 2025 15:20:18 -0800
    
    
    Adds integration tests for the DynamoDbSourceCoordinationStore that use an
    embedded DynamoDB database to test.
    
    To support better testing and cleaner code, this also updates the plugin to use
    the Spring DI capabilities that Data Prepper now offers to plugin authors.
    
    Exclude the plugin-test-framework project from the DynamoDB source coordination
    store project because it creates an ANTLR conflict with DynamoDBLocal.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Move MetricsHelper to common utils class (#6235)__

    [Vecheka](mailto:cvecheka07@gmail.com) - Mon, 3 Nov 2025 14:19:48 -0800
    
    
    Signed-off-by: Vecheka Chhourn &lt;vecheka@amazon.com&gt;

* __Fix unsatisfied dependency issue in leaderOnlyTokenCrawler (#6230)__

    [Brendan B.](mailto:32278900+bbenner7635@users.noreply.github.com) - Mon, 3 Nov 2025 11:37:20 -0800
    
    
    Signed-off-by: Brendan Benner &lt;bbenner@amazon.com&gt; Co-authored-by: Brendan
    Benner &lt;bbenner@amazon.com&gt;

* __M365 error type metric names (#6214)__

    [Vecheka](mailto:cvecheka07@gmail.com) - Fri, 31 Oct 2025 15:27:52 -0700
    
    
    * Adding specific metric names on exception/error for m365 plugins
    

* __Implement LeaderOnlyTokenCrawler (#6160)__

    [alparish](mailto:152813728+alparish@users.noreply.github.com) - Fri, 31 Oct 2025 11:47:21 -0500
    
    
    Signed-off-by: Alekhya Parisha &lt;aparisha@amazon.com&gt; Co-authored-by: Alekhya
    Parisha &lt;aparisha@amazon.com&gt;

* __Fixing the crawler framework to handle ddb outage scenario (#6207)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Thu, 30 Oct 2025 12:38:08 -0700
    
    
    * Fixing the crawler framework to handle ddb outage scenario
    

* __Migrate Bitnami Zookeeper and Kafka Docker Images (#6210)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Thu, 30 Oct 2025 15:10:16 +0100
    
    
    Broadcom discontinued support of non-secured Docker images used in the
    integration tests. They were moved to the bitnamilegacy repository. This PR
    changes the images accordingly.
    
    Eventually the images should be replaced by supported images as there is no
    further updates on the images. The images were not changed to avoid conflicts
    that might arise using other images.
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Introduce otlp/http support in OTelTraceSource (#5322)__

    [Tomas](mailto:tlongo@sternad.de) - Thu, 30 Oct 2025 06:14:12 +0100
    
    
    * Update OpenTelemetryProto to 1.3.2-alpha and refactor scope usage
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Process ExportTraceServiceRequest in http service
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Move gRPC request tests to own class
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Cleanup
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Separate concerns when it comes to configuring the server/services
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Use retry calculator to provide backoff info
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Add metrics to http exception handler
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Revert accidental changes
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Infer protocol from config. Isolate tests regarding unframed requests
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Add basic auth to http service
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Move configuration of http service into own class
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Add pr description
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Remove pr description
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Fix checkstyle findings
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * [WIP] Fix issue with http service being enabled, while grpc service accepts
    unframed requests
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * Refactor EndToEndRawSpanTest
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * Create ArmeriaAuthenticationProvider via PluginFactory
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * Rename GrpcRetryInfoCalculator
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * Create test for invalid payload
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * Add test for healthcheck
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * Add test for http exception handler
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * Remove tests
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    * Fix missing imports
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Remove unused imports
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Fix imports
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Remove/edit todos
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Remove accidentally added default password
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Declare assertJ as test lib and reference it from e2e test
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Fix merge error
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Use insecure ssl connection in test
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    ---------
    
    Signed-off-by: Tomas Longo &lt;tomas.longo@sap.com&gt; Signed-off-by: Tomas Longo
    &lt;tlongo@sternad.de&gt; Co-authored-by: Tomas Longo &lt;tomas.longo@sap.com&gt;

* __M365 Crawler Metric, Buffer, and Unit Test Updates (#6142)__

    [chrisale000](mailto:alexchristensen11131997@gmail.com) - Wed, 29 Oct 2025 18:21:48 -0500
    
    
    Signed-off-by: Alex Christensen &lt;alchrisk@amazon.com&gt; Co-authored-by: Alex
    Christensen &lt;alchrisk@amazon.com&gt;

* __Add metrics for partitions created and completed to enhanced source coordination store (#6203)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 29 Oct 2025 14:43:41 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add common sink framework for DataPrepper sinks (#6183)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 28 Oct 2025 15:57:25 -0700
    
    
    * Add common sink framework for DataPrepper sinks
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed unnecessary file
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Increment Office365 request failure metrics on retry (#6204)__

    [Brendan B.](mailto:32278900+bbenner7635@users.noreply.github.com) - Tue, 28 Oct 2025 15:09:09 -0500
    
    
    Signed-off-by: Brendan Benner &lt;bbenner@amazon.com&gt; Co-authored-by: Brendan
    Benner &lt;bbenner@amazon.com&gt;

* __Fix consecutive hyphens in export id (#6194)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 23 Oct 2025 12:14:24 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fixes a subtle bug with DataPrepperVersion. A recent commit to parse the version from the Gradle project updated the public parse() method to support a full version string. However, this is not an allowable version to parse for the purposes of pipeline configurations. This change fixes that by bringing back the restriction on the public parse() method to support only major or major.minor patterns. Only when reading from the version string from the VersionProvider, do we now allow reading the full version string. (#6199)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 23 Oct 2025 09:35:49 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Some refactoring to how we determine the Data Prepper version to allow other builds to modify the Data Prepper version. Log the current Data Prepper version on start up. (#6196)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 23 Oct 2025 00:26:49 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add negative tests to sqs and cloudwatch logs sink (#6189)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 22 Oct 2025 16:18:19 -0700
    
    
    * Add negative tests to sqs and cloudwatch logs sink
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix checkstyle error
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix how failures are handled for shard partitions in DDB source (#6184)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 22 Oct 2025 14:26:07 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fixes the AWS Secrets end-to-end test by setting permissions for the .aws directory. (#6192)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 21 Oct 2025 11:53:45 -0700
    
    
    Removes getting the STS caller. Updates the configure-aws-credentials GHA
    action to v5. Use a String for the binds map rather than a GString.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Automatically generate the Data Prepper version from the gradle.properties version. (#6182)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 20 Oct 2025 12:31:57 -0700
    
    
    Updates the approach for getting the current version of Data Prepper to
    generate a dynamic class in data-prepper-core with the current version of the
    build. Use Java SPI to load this from DataPrepperVersion in data-prepper-api.
    With this change we no longer need to update a class file on each version
    update.
    
    Additionally, because this method is static and used in many other classes,
    there is now a test library to facilitate getting a version. This one is
    hard-coded for version 2.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Create a data_prepper_test user in the end-to-end tests for Java. This way the .aws volume mapping will be able to load for both the release Docker image and the end-to-end test custom image. (#6188)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 20 Oct 2025 06:46:39 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __CloudWatchLogs Sink: Update max event size and drop error events if DLQ is not configured (#6154)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Fri, 17 Oct 2025 16:13:11 -0700
    
    
    * CloudWatchLogs Sink: Update max event size and drop error events if DLQ is
    not configured
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added distribution summary metrics for log size and request size
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Provide AWS credentials in the /.aws directory instead of the /root/.aws directory for the end-to-end tests. (#6177)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Oct 2025 10:29:41 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Integration test the OpenSearch sink against 2.12.0 and 2.19.3. (#6173)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Oct 2025 10:29:32 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __add jsonl as a supported extension for S3 sink (#6159)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Thu, 16 Oct 2025 14:10:34 -0500
    
    
    * add jsonl as a supported extension for S3 sink
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    * make ExtensionOption an enum
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    * address more comments
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Data Prepper 2.12.2 release notes. (#6178)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Oct 2025 09:59:22 -0700
    
    
    Data Prepper 2.12.2 release notes.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Data Prepper 2.12.2 change log (#6179)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 15 Oct 2025 12:08:50 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Minor update to the release notes for 2.11.0 for a breaking change. (#6176)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 15 Oct 2025 10:49:18 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

