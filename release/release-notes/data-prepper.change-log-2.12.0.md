
* __Adds the release notes for Data Prepper 2.12. (#5829) (#5835)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 26 Jun 2025 09:22:12 -0700
    
    EAD -&gt; refs/heads/2.12, refs/remotes/upstream/2.12
    (cherry picked from commit 21fcbcd63f8a498dd61f7864235270e2cabcf2a2)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Generated THIRD-PARTY file for 0dda9ce (#5833)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 26 Jun 2025 08:15:47 -0700
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt; Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __OTLP Source unified endpoint for logs, traces and metrics (#5677) (#5832)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 21:39:00 -0700
    
    
    * init single endpoint
    
    
    
    * add reflection service
    
    
    
    * update getmetadata expression to support eventType
    
    
    
    * format files
    
    
    
    * update config and add health check
    
    
    
    * add http support and update config options
    
    
    
    * reset Otel trace source
    
    
    
    * add tests for healthcheck, auth, requests and config
    
    
    
    * add tests for grpc
    
    
    
    * update retry tests
    
    
    
    * update plugin name from otel-telemetry-source to otlp
    
    
    
    * update readme with usage details
    
    
    
    * reset Otel trace source changes
    
    
    
    * revert GetMetadataExpressionFunction and OTelTraceSource changes
    
    
    
    * remove example
    
    
    
    * update source to use http-common server
    
    
    
    * added back retry tests, use http-common health check
    
    
    
    * update readme with authentication details
    
    
    
    * fix checkstyle issues
    
    
    
    * add support for OpenSearch formats &amp; update readme
    
    
    
    * remove dupe in settings
    
    
    
    * remove junit and mockito
    
    
    
    * update config fields and move certs
    
    
    
    * update tests with new config options
    
    
    
    * use data prepper duration and add generic output_format
    
    
    
    * update timeouts to duration
    
    
    
    * update the output format defaults to null
    
    
    
    ---------
    
    
    (cherry picked from commit 5ad289dd00cfaa73509c7b0fdb757b73d0f18a0c)
    
    Signed-off-by: Shenoy Pratik &lt;sgguruda@amazon.com&gt; Co-authored-by: Shenoy
    Pratik &lt;sgguruda@amazon.com&gt;

* __Update the version to Data Prepper 2.12.0. (#5827)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 25 Jun 2025 12:52:38 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump aws-cdk-lib in /release/staging-resources-cdk (#5819)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 09:17:26 -0700
    
    
    Bumps
    [aws-cdk-lib](https://github.com/aws/aws-cdk/tree/HEAD/packages/aws-cdk-lib)
    from 2.88.0 to 2.189.1.
    - [Release notes](https://github.com/aws/aws-cdk/releases)
    - [Changelog](https://github.com/aws/aws-cdk/blob/main/CHANGELOG.v2.alpha.md)
    -
    [Commits](https://github.com/aws/aws-cdk/commits/v2.189.1/packages/aws-cdk-lib)
    
    --- updated-dependencies:
    - dependency-name: aws-cdk-lib
     dependency-version: 2.189.1
     dependency-type: direct:production
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __chore(deps): update dependency urllib3 to v2 (#5821)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 09:16:15 -0700
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __chore(deps): update dependency requests to v2.32.4 (#5824)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 09:15:05 -0700
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Bump protobuf in /examples/trace-analytics-sample-app/sample-app (#5786)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 09:13:26 -0700
    
    
    Bumps [protobuf](https://github.com/protocolbuffers/protobuf) from 3.20.3 to
    4.25.8.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/main/protobuf_release.bzl)
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.20.3...v4.25.8)
    
    --- updated-dependencies:
    - dependency-name: protobuf
     dependency-version: 4.25.8
     dependency-type: direct:production
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Update s3 scan ack timeout and scan interval in rds template (#5820)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 25 Jun 2025 11:08:06 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __chore(deps): update dependency requests to v2.32.4 (#5781)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 08:51:02 -0700
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __chore(deps): update dependency protobuf to v4 (#5814)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 08:43:47 -0700
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __chore(deps): update dependency urllib3 to v2.5.0 (#5803)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 08:41:16 -0700
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Bump brace-expansion from 1.1.11 to 1.1.12 in /testing/aws-testing-cdk (#5817)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 25 Jun 2025 08:39:51 -0700
    
    
    Bumps [brace-expansion](https://github.com/juliangruber/brace-expansion) from
    1.1.11 to 1.1.12.
    - [Release notes](https://github.com/juliangruber/brace-expansion/releases)
    -
    [Commits](https://github.com/juliangruber/brace-expansion/compare/1.1.11...v1.1.12)
    
    --- updated-dependencies:
    - dependency-name: brace-expansion
     dependency-version: 1.1.12
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Creates an IAM role granting the OpenSearch CI access to the S3 artifacts (#5815)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 24 Jun 2025 15:58:22 -0700
    
    
    Creates an IAM role that the OpenSearch CI build server can assume to gain
    access to the S3 bucket for archives. Contributes toward #5796 by allowing the
    server to perform a full S3 download of the Maven artifacts.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for convering keys to lowercase/uppercase in RenameKeyProcessor (#5810)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 24 Jun 2025 10:32:28 -0700
    
    
    * Add support for convering keys to lowercase/uppercase in RenameKeyProcessor
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add traceId to documentId used for spans (#5684)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Tue, 24 Jun 2025 09:44:09 +0200
    
    
    * Add traceId to documentId used for spans
    
    adresses #5370
    
    OpenTelemetry spans are currently indexed using the spanId as documentId. This
    can lead to collisions where spans are not indexed or overwrite each other.
    
    This change adds the traceId to the documentId. OpenTelemetry assumes the 
    combination of traceId and spanId to be unique. If there is still a collision
    it is safe to assume, that it occurs because of a resending of a previously
    indexed span.
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add integration test forcing the change
    
    Adds an integration tests, that indexes two spans with the same span id but
    different trace ids. With the old implementation the two spans would overwrite
    each other. This tests checks, that two spans are returned by the query. This
    test is only green with the new document id implementation and fails with the
    earlier.
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    ---------
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Decoupling PipelineDataFlowModel dependency from PipelineTransformer (#5809)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Mon, 23 Jun 2025 15:12:38 -0700
    
    
    * decoupled PipelineDataModel dependency from PipelineTransformer. It will now
    get the pipeline data model as an argument instead of constructor argument
    
    Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;

* __Fix style and build errors from #5778 (#5811)__

    [Jeffrey Aaron Jeyasingh](mailto:jeffreyaaron06@gmail.com) - Mon, 23 Jun 2025 14:19:15 -0700
    
    
    * Fix import style and integration test fixes
    
    Signed-off-by: Jeffrey Aaron Jeyasingh &lt;jeffreyaaron06@gmail.com&gt;

* __Enable zSTD Compression for Kafka Buffer - Json type (#5778)__

    [Jeffrey Aaron Jeyasingh](mailto:jeffreyaaron06@gmail.com) - Fri, 20 Jun 2025 16:53:00 -0700
    
    
    * Implement Kafka buffering compression
    
    Signed-off-by: Jeffrey Aaron Jeyasingh &lt;jeffreyaaron06@gmail.com&gt;

* __Adding Processor Registry to provision Atomic swapping of Processor instances (#5794)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Fri, 20 Jun 2025 12:57:00 -0700
    
    
    * Processor Registry class added to provision Atomic swapping of processor list
    
    Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    

* __Fix query size for query_lookup to return more than 10 documents (#5807)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 20 Jun 2025 10:16:43 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fixes two new projects which were added after the PR to re-organize the tests projects. (#5798)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 19 Jun 2025 14:26:11 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Moves the Data Prepper test projects into a single project and updates the project naming to follow the pattern used in data-prepper-plugins. (#5726)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 19 Jun 2025 13:53:05 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Gradle build support for publishing Maven snapshots. Updates the release.yml to publish only the Maven release artifacts. (#5797)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 19 Jun 2025 13:52:35 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add auto conversion option to convert_type processor (#5782)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 17 Jun 2025 14:20:26 -0700
    
    
    * Add auto conversion option to convert_type processor
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified to coerse floats to double
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add multi-line csv support (#5784)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Mon, 16 Jun 2025 15:44:27 -0700
    
    
    * Add multi-line csv support
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __add internal batching of the records for Sagemaker job creator (#5734)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Fri, 13 Jun 2025 13:24:45 -0700
    
    
    * add internal batching of the records for sagemaker job creator
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    * remove schduler in lieu of worker pulling schedule
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Add detect format processor (#5774)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Fri, 13 Jun 2025 12:54:23 -0700
    
    
    * Add detect format processor
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add iterate_on support for convert_type processors to iterate over array of objects and convert within each element (#5775)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 13 Jun 2025 14:53:14 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Better error handling and printing exception stack trace (#5776)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Fri, 13 Jun 2025 12:24:09 -0700
    
    
    * Better error handling and printing exception stack trace to make sure that
    every failure is captured and logged
    

* __Iterate on support for add_entries and delete_entries processors (#5773)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 12 Jun 2025 20:33:01 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt; Signed-off-by: Taylor Gray
    &lt;tylgry@amazon.com&gt; Co-authored-by: George Chen &lt;qchea@amazon.com&gt;

* __Add support for updating JacksonEvent array elements (#5772)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 12 Jun 2025 11:46:45 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Modify cloudwatch sink to use FixedThreadPool (#5770)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 11 Jun 2025 16:41:34 -0700
    
    
    * Modify cloudwatch sink to use FixedThreadPool
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __feat: Allow plugins to access default pipeline role via AwsCredentialsSupplier (#5604)__

    [Saketh Pallempati](mailto:pallempati.saketh@fmr.com) - Tue, 10 Jun 2025 10:19:13 -0500
    
    
    This change enables plugins to access the default STS role ARN configured in
                     data-prepper-config.yaml via the AwsCredentialsSupplier
    interface.
    
                      Changes:
                     - Added getDefaultStsRoleArn() method to
    AwsCredentialsSupplier interface
                     - Implemented the method in DefaultAwsCredentialsSupplier
                     - Added corresponding method to CredentialsProviderFactory
                     - Added unit tests for the new functionality
    
                      This maintains a consistent pattern with how the default
    region is already
                     accessible to plugins through the same interface.
    
    Signed-off-by: Pallempati Saketh &lt;pallempati.saketh@fmr.com&gt;

* __Move token refresh inside retry operations for Office 365 connector (#5766)__

    [alparish](mailto:152813728+alparish@users.noreply.github.com) - Mon, 9 Jun 2025 12:40:28 -0500
    
    
    Signed-off-by: Alekhya Parisha &lt;aparisha@amazon.com&gt; Co-authored-by: Alekhya
    Parisha &lt;aparisha@amazon.com&gt;

* __fix: lastPollTime updating for O365 (#5764)__

    [Savit Aluri](mailto:savit.aluri@gmail.com) - Mon, 9 Jun 2025 12:33:35 -0500
    
    
    Signed-off-by: Savit Aluri &lt;savaluri@amazon.com&gt; Co-authored-by: Savit Aluri
    &lt;savaluri@amazon.com&gt;

* __Fix kafka source with glue registry (#5765)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Fri, 6 Jun 2025 17:56:30 -0700
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Support running the end-to-end tests against defined Docker images. Resolves #3567. (#5711)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 4 Jun 2025 15:08:29 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Improvements to artifacts to support Maven artifacts for plugins. Add data-prepper prefix to jar names, though they aren&#39;t used in the Maven coordinates. Move some projects into plugins or core groupIds. (#5722)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 4 Jun 2025 15:08:09 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add OTLP sink plugin for exporting spans to AWS X-Ray (#5664)__

    [huyPham](mailto:huypham612@gmail.com) - Tue, 3 Jun 2025 11:29:44 -0700
    
    
    Add OTLP sink plugin for exporting spans to AWS X-Ray (#5664)
    
    Feature/xray init
    
    feat(xray-otlp-sink): Add X-Ray OTLP Sink plugin skeleton
    - Added test resources and support for Span records
    - Added sample pipeline config and OTLP test span JSON under
    `src/test/resources`
    - Verified local pipeline ingest and logging using `grpcurl`
    - Added README with developer instructions for running and testing locally
    
    These changes establish the foundation for local testing and future X-Ray e2e
    testing.
    
    Signed-off-by: huy pham &lt;huyp@amazon.com&gt; Signed-off-by: Heli
    &lt;desaiheli17@gmail.com&gt; Co-authored-by: Heli &lt;desaiheli17@gmail.com&gt;

* __FEAT: encryption extension integration with kafka (#5625)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 30 May 2025 16:21:04 -0500
    
    
    * FEAT: encryption extension integration with kafka buffer
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add mod operator to Data Prepper (#5729)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Fri, 30 May 2025 15:46:07 -0500
    
    
    * mod operator implementation
    
    Signed-off-by: Katherine Shen &lt;katshen@amazon.com&gt;

* __Feature/office365 source v2 (#5721)__

    [alparish](mailto:152813728+alparish@users.noreply.github.com) - Thu, 29 May 2025 22:41:20 -0700
    
    
     Office365 SAAS Source Plugin
    

* __ENH: encryption extension (#5581)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 29 May 2025 15:58:16 -0500
    
    
    ADD: encryption extension
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Set server id for mysql binlog client (#5725)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 28 May 2025 16:37:57 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Rename Atlassian state classes to PaginationCrawler (#5718)__

    [alparish](mailto:152813728+alparish@users.noreply.github.com) - Fri, 23 May 2025 15:36:40 -0500
    
    efs/heads/5413-ddb-streams-data-loss
    Signed-off-by: Alekhya Parisha &lt;aparisha@amazon.com&gt;
    
    Co-authored-by: Alekhya Parisha &lt;aparisha@amazon.com&gt;

* __Fix CloudwatchLogs and Sqs sink config to use correct Jakarta annotations (#5714)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 22 May 2025 09:54:07 -0700
    
    
    * Fix CloudwatchLogs sink config to use correct Jakarta annotations
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Also, fixed SQS sink Jakarta annotation issue
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Re-enable ZeroBuffer (#5661)__

    [Mohammed Aghil Puthiyottil](mailto:57040494+MohammedAghil@users.noreply.github.com) - Wed, 21 May 2025 19:35:10 -0500
    
    
    Signed-off-by: Mohammed Aghil Puthiyottil
    &lt;57040494+MohammedAghil@users.noreply.github.com&gt;

* __Increase default scroll time per batch from 1m to 10m (#5704)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 21 May 2025 13:13:29 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add codec to http source (#5694)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 21 May 2025 10:27:15 -0500
    
    
    * Add codec to http source
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add a test with sample data
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add aggregate metrics for rds source (#5697)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 19 May 2025 16:55:35 -0500
    
    
    * Add aggregated metrics for rds source
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add more granular error metrics
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add more tests
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Change slot naming (#5699)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 19 May 2025 13:42:16 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Serialization fix when the expected type is PluginConfigVariable (#5698)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Fri, 16 May 2025 17:50:00 -0700
    
    
    * Serialization fix when the expected type is PluginConfigVariable
    

* __Increase Sqs sink test coverage. Add more metrics (#5693)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Fri, 16 May 2025 15:30:44 -0700
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Added integration tests for validating that events are processed exactly once (#5691)__

    [Mohammed Aghil Puthiyottil](mailto:57040494+MohammedAghil@users.noreply.github.com) - Thu, 15 May 2025 16:48:09 -0700
    
    
    * Added integration tests for validating that events are processed exactly once
    by any processor
    
    - Added additional validation for BasicEndToEndTests
    
    Signed-off-by: Mohammed Aghil Puthiyottil
    &lt;57040494+MohammedAghil@users.noreply.github.com&gt;
    
    * Moved getEventsMap() and getName() methods to base class
    
    Signed-off-by: Mohammed Aghil Puthiyottil
    &lt;57040494+MohammedAghil@users.noreply.github.com&gt;
    
    * Addressed comments on PIPELINE_TO_PROCESSORS_MAP
    
    Signed-off-by: Mohammed Aghil Puthiyottil
    &lt;57040494+MohammedAghil@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Mohammed Aghil Puthiyottil
    &lt;57040494+MohammedAghil@users.noreply.github.com&gt;

* __Add `getEventType()` expression function (#5686)__

    [Shenoy Pratik](mailto:sgguruda@amazon.com) - Wed, 14 May 2025 14:25:23 -0500
    
    
    * add geteventType expresion
    
    Signed-off-by: Shenoy Pratik &lt;sgguruda@amazon.com&gt;
    
    * update antlr grammar, parser and add expression tests
    
    Signed-off-by: Shenoy Pratik &lt;sgguruda@amazon.com&gt;
    
    * update expression syntax documentation
    
    Signed-off-by: Shenoy Pratik &lt;sgguruda@amazon.com&gt;
    
    * move expression parse exception check to evaluation exceptions
    
    Signed-off-by: Shenoy Pratik &lt;sgguruda@amazon.com&gt;
    
    * resolve comments, update getEventTypet test to become parameterized
    
    Signed-off-by: Shenoy Pratik &lt;sgguruda@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Shenoy Pratik &lt;sgguruda@amazon.com&gt;

* __Kds cross account stream (#5687)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Tue, 13 May 2025 08:19:40 -0700
    
    
    Implementation for cross account stream support in KDS
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    

* __fix log warnings for HTTP server instanced source names (#5689)__

    [Shenoy Pratik](mailto:sgguruda@amazon.com) - Mon, 12 May 2025 09:04:11 -0700
    
    
    Signed-off-by: Shenoy Pratik &lt;sgguruda@amazon.com&gt;

* __Avro 1.11.4 (#5680)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 8 May 2025 13:44:28 -0700
    
    
    Fixes CVE-2024-47561
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes an uncommon test failure in TruncateProcessorConfigTests by not using negative zero. Refactors the tests by splitting a single test into two and sharing common code. (#5683)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 7 May 2025 12:28:26 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Enabling experimental plugins specifically by plugin type and name (#5676)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 7 May 2025 11:58:02 -0700
    
    
    Support enabling experimental plugins specifically by plugin type and name.
    This also includes a change to the core plugin classes to allow them to define
    themselves as a plugin component type along with a name that is used to create
    the mapping in the YAML file.
    
    Resolves #5675
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __CrowdStrike client and coordinator implementation (#5678)__

    [Neha Gupta](mailto:35155714+nsgupta1@users.noreply.github.com) - Tue, 6 May 2025 13:29:54 -0700
    
    
    * CrowdStrike client and coordinator implementation
    
    Signed-off-by: nsgupta1 &lt;nsgupta1@users.noreply.github.com&gt;

* __Add support for api token in config (#5544)__

    [Derek Ho](mailto:dxho@amazon.com) - Tue, 6 May 2025 13:05:18 -0700
    
    
    Add api token
    
    Signed-off-by: Derek Ho &lt;dxho@amazon.com&gt;

* __Add OTel Metrics String Attributes to Index Template (#5589)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Tue, 6 May 2025 21:03:44 +0200
    
    
    Introduces keyword mappings for string fields. This was the default before the
    latest change for all fields.
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Fix validation for default route (#5682)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 6 May 2025 13:14:43 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Allow disabling metrics with data-prepper-config.yaml (#5627)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Thu, 1 May 2025 03:04:13 -0700
    
    efs/heads/5312-pipe-processor
    Add support for disabling metrics via data-prepper-config.yaml
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt; Signed-off-by: Siqi Ding
    &lt;109874435+Davidding4718@users.noreply.github.com&gt;

* __Add SQS sink (#5628)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 29 Apr 2025 14:59:35 -0700
    
    
    * Addressed review comments and updated to use latest codec changes
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed failing testcase and added other tests
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix checkstyle error
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __CrowdStrike API call and retry mechanism (#5654)__

    [Neha Gupta](mailto:35155714+nsgupta1@users.noreply.github.com) - Mon, 28 Apr 2025 11:46:54 -0700
    
    
    * CrowdStrike API call and retry mechanism
    
    Signed-off-by: nsgupta1 &lt;nsgupta1@users.noreply.github.com&gt; Signed-off-by:
    ngsupta1 &lt;guptaneha.e@gmail.com&gt; Co-authored-by: nsgupta1
    &lt;nsgupta1@users.noreply.github.com&gt;

* __Merging common auth to main and fixing the conflicts (#5653)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Mon, 28 Apr 2025 09:46:23 -0700
    
    
    Common server builder and auth module for push based plugins (#5423)
    
    Add Custom Auth Provider with support for gRPC, plus tests and exception
    handling
    
    Signed-off-by: Maxwell Brown &lt;mxwelwbr@amazon.com&gt; Signed-off-by: Siqi Ding
    &lt;109874435+Davidding4718@users.noreply.github.com&gt; Co-authored-by: Maxwell
    Brown &lt;55033421+Galactus22625@users.noreply.github.com&gt;

* __Initial commit to refactor the OutputCodec to support a Writer per OutputStream (#5606)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 28 Apr 2025 05:03:14 -0700
    
    
    Initial commit to refactor the OutputCodec to support a Writer that is bound to
    a specific OutputStream. This supports backward compatibility with the existing
    APIs and an update to the JsonOutputCodec and NdjsonOutputCodec to start the
    migration. Adds missing unit tests for NdjsonOutputCodec.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Release notes for Data Prepper 2.11 (#5633)__

    [David Venable](mailto:dlv@amazon.com) - Sun, 27 Apr 2025 08:40:19 -0700
    
    
    * Release notes for Data Prepper 2.11
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Adds missing issue/PR as pointed out in PR for release notes.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __FIX: NPE in JsonCodec with keyName (#5659)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 25 Apr 2025 20:02:11 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Adds some information about the OpenSearch CI build along with useful links. (#5615)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 24 Apr 2025 12:18:28 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Release notes for Data Prepper 2.11 (#5633)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 24 Apr 2025 07:23:02 -0700
    
    
    Release notes for Data Prepper 2.11
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __changelog-2.11.0 (#5648)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Wed, 23 Apr 2025 15:34:56 -0700
    
    
    Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;

* __Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt; (#5621)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Wed, 23 Apr 2025 15:46:04 -0500
    
    
    Adding retries and backoff to handle delayed sync between GSI and primary table
    when DDB is used Source Coordination Store

* __FIX: opensearch serverless parameter constraint (#5641)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 22 Apr 2025 17:14:49 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Explicit enforcing of order (#5640)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Tue, 22 Apr 2025 10:57:49 -0700
    
    
    * mentioning explicit ordering
    
    Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    

* __Change main branch version to 2.12.0-SNAPSHOT (#5637)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 22 Apr 2025 09:03:57 -0700
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Change version in DataPrepper Version class to 2.12 (#5638)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 22 Apr 2025 09:03:27 -0700
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add missing OTEL standard fields (#5635)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Mon, 21 Apr 2025 17:57:13 -0700
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Marking CrowdStrike as Experimental (#5630)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Mon, 21 Apr 2025 08:44:51 -0700
    
    
    Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;

* __Add support for multiple entries of &#39;with_keys&#39; with &#39;delete_when&#39; co… (#5356)__

    [Niketan Chandarana](mailto:42366580+niketan16@users.noreply.github.com) - Mon, 21 Apr 2025 08:31:25 -0700
    
    
    Add support for multiple entries of &#39;with_keys&#39; with &#39;delete_when&#39; condition in
    delete_entries processor
    
    Signed-off-by: Niketan Chandarana &lt;niketanc@amazon.com&gt;


