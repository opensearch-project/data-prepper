
* __Updated the version to 1.5.0 for the release. (#1533)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 23 Jun 2022 10:29:42 -0500
    
    EAD -&gt; refs/heads/1.5, refs/remotes/upstream/1.5, refs/remotes/origin/1.5
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Created release notes for Data Prepper 1.5.0 (#1531)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 23 Jun 2022 09:23:47 -0500
    
    efs/remotes/upstream/main, refs/remotes/upstream/1.x, refs/remotes/origin/main, refs/remotes/origin/HEAD, refs/heads/main
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
    
    efs/remotes/kmssap/main
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


