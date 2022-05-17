
* __Fix flaky e2e tests (#1382)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 16 May 2022 14:59:39 -0500
    
    EAD -&gt; refs/heads/changelog, refs/remotes/upstream/main, refs/remotes/origin/main, refs/remotes/origin/HEAD, refs/heads/main
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


