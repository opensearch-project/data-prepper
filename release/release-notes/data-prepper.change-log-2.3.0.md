
* __Adds release notes for Data Prepper 2.3.0. (#2833) (#2835)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 6 Jun 2023 11:05:19 -0500
    
    EAD -&gt; refs/heads/2.3, refs/remotes/upstream/2.3
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit c02f01d4dd9494c4e88eb29774b40b7987b13f52)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the build to Data Prepper 2.3.0. (#2831)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 6 Jun 2023 10:58:25 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bug where s3 stream was closing too early. (#2830) (#2834)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 6 Jun 2023 07:41:06 -0500
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    (cherry picked from commit 122f447a296713b9c1aaad10748f3a4235e4e7e1)
     Co-authored-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Generated THIRD-PARTY file for 3a70e73 (#2828) (#2829)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 5 Jun 2023 17:09:32 -0500
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;
    (cherry picked from commit cecb3f58ebb89740ff9650a7545c8e2cc5f234e1)
     Co-authored-by: opensearch-trigger-bot[bot]
    &lt;98922864+opensearch-trigger-bot[bot]@users.noreply.github.com&gt;

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
    
    efs/heads/s3-extensions
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
    
    efs/heads/1-click-release
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


