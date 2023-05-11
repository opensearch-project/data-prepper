
* __Update to Snakeyaml 2.0 in the Trace Analytics sample app. (#2651) (#2658)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 8 May 2023 18:19:26 -0500

  EAD -&gt; refs/heads/2.2, refs/remotes/upstream/2.2
  Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
  (cherry picked from commit 7ef5ab41dead0be8e48eac0ccc97b4f7e45eabd9)
  Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix OpenSearch Retry mechanism (#2643) (#2648)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 5 May 2023 13:22:15 -0700


    Fix OpenSearch Retry mechanism
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    (cherry picked from commit b7ef680901ead5e147f0a88e98b187913ef9dfc1)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Updates the example Spring Boot application to Spring Boot 2.7.11 and Java 11. Should resolve CVE-2023-20863, CVE-2022-45143. (#2634) (#2642)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 4 May 2023 17:29:53 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 52fce30876c392b250054afb962ac39eeaa5a616)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Log clear messages when OpenSearch Sink fails to push. Modify retries to be iterative instead of recursive (#2605) (#2627)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 4 May 2023 11:31:29 -0700


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
    (cherry picked from commit 1f90f387599bff0c797dcadd1ecb230284d07523)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Updates Jackson to 2.15 and Snakeyaml to 2.0. This should resolve security warnings on CVE-2022-1471, though according to the Jackson team, Jackson was already not vulnerable to this CVE. (#2632) (#2633)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 4 May 2023 13:18:18 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 90cc4334bde0f1ad4f0d48264d76c8aeb4440f03)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Increase timeout values in in-memory source PipelinesWithAcksIT to fix occasional test failures (#2606) (#2616)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 2 May 2023 10:05:27 -0500


    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    (cherry picked from commit 94974df301f46443b964d7d2a8859b2ba99469d4)


* __Consolidates use of OpenSearch clients using the Gradle version catalog. Removes some unnecessary Gradle configurations. (#2569) (#2614)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 1 May 2023 12:39:39 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit f05c4b2c979b690dc0f27e048cff468657cd27e7)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump net.bytebuddy:byte-buddy-agent in /data-prepper-plugins/opensearch (#2432) (#2602)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Sat, 29 Apr 2023 09:41:47 -0500


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
    (cherry picked from commit 9fa43238116f9c6e7fa9da3ab54695a0ad1259ec)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-jpl in /data-prepper-core (#2430) (#2592)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 28 Apr 2023 15:10:47 -0500


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
    (cherry picked from commit 787bc9f7360d10af4f5c6dd176fc73ee999907e3)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.jetbrains.kotlin:kotlin-stdlib from 1.7.10 to 1.8.20 (#2434) (#2593)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 28 Apr 2023 09:17:31 -0500


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
    (cherry picked from commit 39ec9380790fb53c21af4b56f36050917952d910)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add null check for bulkRetryCountMap in opensearch sink (#2600) (#2601)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 27 Apr 2023 10:44:13 -0500


    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit bd25456ead32dcf621c8dadb20cf1e70000eca84)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Adds object filter patterns for core peer-forwarder&#39;s Java deserialization to put restrictions on the maximum array length and the maximum object depth. (#2576) (#2584)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 24 Apr 2023 10:34:00 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit f151668941c99dc813cde7b2fac7c0e7353e5ef8)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Apply exponential backoff for exceptions when reading from S3 (#2580) (#2583)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 24 Apr 2023 09:45:39 -0500


    Apply exponential backoff for exceptions when reading from S3 in the S3 source.
    Apply exponential backoff for SQS DeleteMessage requests as well. #2568
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit b2c3f27d8f275b253c4501b57dec97c9783a98f4)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Increase the backoff delays in the S3 source polling thread to run in the range of 20 seconds to 5 minutes. The current behavior still produces too many logs. Fixes #2568. (#2574) (#2575)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 24 Apr 2023 09:44:54 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit aef2baa3ec0a0aca7e74026db18553e0ef9eda8a)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Next version: 2.2.1 (#2577)__

  [David Venable](mailto:dlv@amazon.com) - Mon, 24 Apr 2023 09:17:20 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Log full errors when the OpenSearch sink fails to start (#2565) (#2566)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Sat, 22 Apr 2023 13:57:41 -0500


    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 32418b869be9487742fe5fa158e1d5c1aae748e8)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;


