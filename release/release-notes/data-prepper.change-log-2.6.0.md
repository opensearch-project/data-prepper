
* __Release notes for Data Prepper 2.6.0 (#3710) (#3712)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 28 Nov 2023 08:59:32 -0800
    
    Adds the release notes for Data Prepper 2.6.0.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 2cb172466de9a0a02b43928c82480138f1b02b37)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bug so GLOBAL read-only items do not expire from TTL in ddb source coordination store (#3703) (#3711)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 28 Nov 2023 10:20:50 -0600
    
    
    Fix bug so GLOBAL read-only items do not expire from TTL in ddb source
    coordination store
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit c8548a0962c2396f111e60a32b0f471a0d424f30)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Generated THIRD-PARTY file for 250e1a0 (#3707)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 27 Nov 2023 15:48:27 -0800
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Gradle parallel max (#3700) (#3706)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 27 Nov 2023 15:47:57 -0800
    
    
    Set the maximum workers to 2 when running the GHA build and release tasks.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 6878f56f4add30520448211352d0965744a96812)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Check if failedDeleteCount is positive before logging (#3686) (#3705)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 27 Nov 2023 16:01:30 -0600
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    (cherry picked from commit 6dc1d12a4b84ade389d7cc311799363e3ea3114d)
     Co-authored-by: Hai Yan &lt;8153134+oeyh@users.noreply.github.com&gt;

* __Update Data Prepper version to 2.6.0. (#3697)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Nov 2023 13:25:03 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Remove projects which are not ready and not releasing with 2.6.0. (#3702)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Nov 2023 13:21:21 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Generated THIRD-PARTY file for c88c27f (#3701)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 27 Nov 2023 13:11:01 -0800
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Require Apache Avro 1.11.3 to fix CVE-2023-39410. Resolves #3430. (#3695)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Nov 2023 10:58:50 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates werkzeug to 3.0.1 which fixes CVE-2023-46136. This required updating to dash 2.14.1 as 2.13 does not support newer versions of werkzeug. Resolves #3552. (#3690)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Nov 2023 08:52:29 -0800
    
    efs/heads/data-prepper-2.6.0-thank-you
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


