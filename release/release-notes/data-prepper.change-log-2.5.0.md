
* __Some updates to the 2.5.0 release notes. (#3479) (#3482)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 11 Oct 2023 09:00:57 -0700
    
    EAD -&gt; refs/heads/2.5, tag: refs/tags/2.5.0, refs/remotes/upstream/2.5
    Some updates to the 2.5.0 release notes.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 5e5e0cdf9319aeb7fa4ed441075e89e9007a77c3)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Generated THIRD-PARTY file for 56283fd (#3478)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 11 Oct 2023 07:43:45 -0700
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Resolve Netty to 4.1.100.Final, require Jetty 11.0.17 in Data Prepper. Use Tomcat 10.1.14 in the example project. These changes fix CVE-2023-44487 to protect against HTTP/2 reset floods. Resolves #3474. (#3475) (#3477)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 11 Oct 2023 07:38:17 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit d3179f09eb40add22c7a24378f925cbfb828a70d)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Generated THIRD-PARTY file for e44d60f (#3460)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 10 Oct 2023 16:33:41 -0700
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: asifsmohammed
    &lt;asifsmohammed@users.noreply.github.com&gt;

* __ENH: support index template for serverless (#3071) (#3467)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 9 Oct 2023 18:19:28 -0700
    
    
    * ENH: support index template for serverless
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    (cherry picked from commit 73a80a12388faa6dd4285db2f8ea6e16fecf4a19)

* __ENH: opensearch source secrets refreshment suppport (#3437) (#3462)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 9 Oct 2023 08:29:48 -0700
    
    
    ENH: opensearch source secrets refreshment suppport (#3437)
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add release-notes for 2.5.0 (#3449) (#3456)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Sat, 7 Oct 2023 09:50:18 -0700
    
    
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
    (cherry picked from commit 7beb2e16059890f006f286ad9417874a96963971)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix CVE-2023-39410 (#3450) (#3455)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Sat, 7 Oct 2023 01:32:51 +0530
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 74409e2cf8cd5add484592e87b7c68bff1dede04)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add inline template_content support to the opensearch sink (#3431) (#3453)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 6 Oct 2023 14:04:39 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit e19ae8f2302b096363229abad77c1ab5a2042569)
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add support for Update/Upsert/Delete operations in OpenSearch Sink (#3424) (#3446)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 6 Oct 2023 11:14:19 -0700
    
    
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
    (cherry picked from commit 36b0b9c95006697ece9ad678b9553bf43526b655)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Remove experimental projects from 2.5 branch (#3435)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 6 Oct 2023 21:18:36 +0530
    
    
    * Remove experimental projects from 2.5 branch
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __ENH: data-prepper-core support for secrets refreshment (#3415) (#3440)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 6 Oct 2023 05:50:21 -0700
    
    
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
    (cherry picked from commit 71a6956e2a29c00094bbb859b1ce2fbbd6ee411e)
     Co-authored-by: Qi Chen &lt;qchea@amazon.com&gt;

* __Bump org.xerial.snappy:snappy-java in /data-prepper-plugins/common (#3411) (#3442)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 5 Oct 2023 17:32:06 -0700
    
    
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
    (cherry picked from commit db32da523d48fd7762163e7a8064a443117ce476)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Remove support for Enum and Duration values from secrets manager (#3433) (#3444)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 5 Oct 2023 17:31:34 -0700
    
    
    * Remove support for Enum and Duration values from secrets manager
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added unit tests
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit d3a027a738d65dfcb576884aad895d40f56319ac)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add retry to Kafka Consumer Create in source (#3399) (#3406)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 5 Oct 2023 09:52:25 -0700
    
    
    Signed-off-by: Jonah Calvo &lt;caljonah@amazon.com&gt;
    (cherry picked from commit 49bf4611d6065308483023316c2ff3293778c2ca)
     Co-authored-by: Jonah Calvo &lt;caljonah@amazon.com&gt;

* __Bump urllib3 in /examples/trace-analytics-sample-app/sample-app (#3425) (#3441)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 5 Oct 2023 09:51:24 -0700
    
    
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
    (cherry picked from commit ccbd95c51ec13f556a4cae4ebe8c2bc80b375f7f)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump urllib3 in /release/smoke-tests/otel-span-exporter (#3427) (#3429)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 5 Oct 2023 09:49:36 -0700
    
    
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
    (cherry picked from commit 463fa4590d5f9d7f0a397430de0dbd634384dd03)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __-download task support for geoip (#3373) (#3428)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 5 Oct 2023 09:48:57 -0700
    
    
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
    (cherry picked from commit 56b74bec7f0f4691bf5db9783cc8cf5159b72aeb)
     Co-authored-by: rajeshLovesToCode
    &lt;131366272+rajeshLovesToCode@users.noreply.github.com&gt;

* __Set 2.5 release version (#3438)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 5 Oct 2023 08:58:20 -0700
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update release notes and change log for 2.4.1 (#3416) (#3419)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 3 Oct 2023 09:57:18 -0700
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 01ed83adfd9f63572bf8596079736b89eb767596)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix CVE-2022-45688, CVE-2023-43642 (#3404) (#3410)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 2 Oct 2023 20:55:01 +0530
    
    
    * Fix CVE-2022-45688
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Fix CVE-2023-43642
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 57ac3b84df3c8d45cb317aad244d9a0c35a49e55)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated release notes file name (#3403) (#3408)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 29 Sep 2023 19:15:25 +0530
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit b493355774dbeca47b9660cad8aba43c791451ad)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

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


