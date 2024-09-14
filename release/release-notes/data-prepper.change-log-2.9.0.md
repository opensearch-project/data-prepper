
* __Change main branch version to 2.10-SNAPSHOT (#4851)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 20 Aug 2024 13:46:09 -0700
    
    EAD -&gt; refs/heads/main, refs/remotes/origin/main, refs/remotes/origin/HEAD
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

* __Add handle failed events option to parse json processors (#4844)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 19 Aug 2024 17:12:29 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix bug where race condition on ack callback could cause S3 folder partition to not be given up (#4835)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 19 Aug 2024 16:15:05 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Update the parse JSON/XML/ION processors to use EventKey. (#4842)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Aug 2024 11:12:27 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for dynamic rule detection for pipeline config transformation (#4601)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Fri, 16 Aug 2024 18:10:52 -0700
    
    
    * Add support for dynamic rule detection for pipeline config transformation
     Signed-off-by: Srikanth Govindarajan &lt;srikanthjg123@gmail.com&gt;
    
    * Address comments
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;
    
    * Move rules and templates to plugin level
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;
    
    * Add dummy plugin for testing dynamic rule detection
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;
    
    * Address comments
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;
    
    ---------
     Signed-off-by: Srikanth Govindarajan &lt;srikanthjg123@gmail.com&gt;
    Signed-off-by:
    Srikanth Govindarajan &lt;srigovs@amazon.com&gt;

* __Add startsWith expression function (#4840)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 16 Aug 2024 16:37:44 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Logging improvements when failing to parse JSON/XML/ION. Do not include the stack trace since it doesn&#39;t provide any value with these exceptions which are expected when the JSON is invalid. Log the input string rather than the Event which was not readable. (#4839)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Aug 2024 05:52:46 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Translate proc optimizations (#4824)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 15 Aug 2024 10:25:43 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Delete .github/workflows/static.yml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Optimize translateSource in translate processor
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

* __Data Prepper expressions - Set operator fix (#4818)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 14 Aug 2024 14:05:26 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Delete .github/workflows/static.yml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed check style errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Http chunking fixes (#4823)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 14 Aug 2024 11:49:12 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Delete .github/workflows/static.yml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fix http message chunking bug
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified tests to test for chunks correctly
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed offline review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed  review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added tests
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

* __FIX: include schema cli into release (#4833)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 14 Aug 2024 09:38:44 -0700
    
    
    MAINT: include schema cli into release
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __PersonalizeSink: add client and configuration classes (#4803)__

    [Ivan Tse](mailto:115105835+ivan-tse@users.noreply.github.com) - Wed, 14 Aug 2024 09:36:12 -0700
    
    
    PersonalizeSink: add client and configuration classes
     Signed-off-by: Ivan Tse &lt;tseiva@amazon.com&gt;

* __Config description changes for aggregate and anomaly detector processors. (#4829)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 13 Aug 2024 13:41:22 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Delete .github/workflows/static.yml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Add json property description for aggregate processor and anomaly detector
    processors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed build failure
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add delete_source parameter to the csv processor (#4828)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 12 Aug 2024 16:21:05 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __FIX: build service map relationship even when trace group is missing (#4822)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 12 Aug 2024 13:24:50 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __ENH: add folder path as output for schema generation (#4820)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 12 Aug 2024 11:51:51 -0500
    
    
    * ENH: add folder path as output
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    

* __Release notes for Data Prepper 2.8.1 (#4807)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 12 Aug 2024 09:25:22 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Create docker-compose-dataprepper.yaml (#4756)__

    [Jayesh Parmar](mailto:89792517+jayeshjeh@users.noreply.github.com) - Mon, 12 Aug 2024 08:56:55 -0700
    
    
    * Create docker-compose-dataprepper.yaml
     Signed-off-by: Jayesh Parmar &lt;89792517+jayeshjeh@users.noreply.github.com&gt;
    
    Signed-off-by: jayeshjeh &lt;jay.parmar.11169@gmail.com&gt;
    
    * Necessary chnages made
     Signed-off-by: jayeshjeh &lt;jay.parmar.11169@gmail.com&gt;
    
    ---------
     Signed-off-by: Jayesh Parmar &lt;89792517+jayeshjeh@users.noreply.github.com&gt;
    
    Signed-off-by: jayeshjeh &lt;jay.parmar.11169@gmail.com&gt;

* __Fixes a regex expression bug. When the left-hand side of the operation is null, always return false rather than throwing an exception. Resolves #4763. (#4798)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 12 Aug 2024 08:20:48 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix null document in DLQ object (#4814)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Sat, 10 Aug 2024 12:04:18 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Delete .github/workflows/static.yml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fix null document in DLQ object
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Several improvements to RDS source (#4810)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 8 Aug 2024 16:51:12 -0500
    
    
    * Add schema manager to query database
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Get real primary keys for export
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Get binlog start position for stream
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Refactor SnapshotStrategy to RdsApiStrategy
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update unit tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * address comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add retry to database queries
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Handle describe exceptions
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address more comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __ADD: data prepper plugin schema generation (#4777)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 2 Aug 2024 14:44:32 -0500
    
    
    * ADD: data-prepper-plugin-schema
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Using Awaitility and mocks in the LogGeneratorSourceTest to attempt to improve reliability. (#4746)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 2 Aug 2024 09:28:55 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes @asifsmohammed from the CODEOWNERS to allow the release to proceed. (#4800)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 1 Aug 2024 14:11:53 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Increase timeout in Acknowledgement IT tests  (#4774)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 31 Jul 2024 18:45:39 -0700
    
    
    Increase timeout for acknowledgement IT tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Signed-off-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    Co-authored-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Cleanup resources properly when Opensearch sink fails to initialize (#4758)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 30 Jul 2024 13:33:51 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * rebased to latest
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * removed unnecessary file
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add rds source metrics (#4769)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 30 Jul 2024 12:09:20 -0500
    
    
    * Add rds source metrics
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Remove unused imports
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add exportS3ObjectsErrors metric
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Reapply &#34;Run tests on the current JVM for Java 17 &amp; 21 / Gradle 8.8 (#4730)&#34; (#4762) (#4771)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 30 Jul 2024 09:35:12 -0700
    
    
    This reverts commit 5c7d58c03059c7a753d882f5b74fa6ed32f45641.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __4602 one way hash (#4750)__

    [mishavay-aws](mailto:140549901+mishavay-aws@users.noreply.github.com) - Mon, 29 Jul 2024 18:41:20 -0700
    
    
    added capabilities for working with OneWay Hash
     Signed-off-by: mishavay-aws &lt;140549901+mishavay-aws@users.noreply.github.com&gt;

* __Corrects the TRIAGING.md with a video meeting since we currently use Chime. (#4743)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 29 Jul 2024 16:34:34 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add json property description for list-to-map, map-to-list and user-agent processor (#4759)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 29 Jul 2024 11:32:31 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add json property descriptions to dissect, flatten, copy_value and translate processor (#4760)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 29 Jul 2024 11:32:25 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Lambda sink refactor (#4766)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Fri, 26 Jul 2024 22:56:36 -0500
    
    
    * Lambda sink refactor
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;
    
    * Address comments
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;
    
    ---------
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;

* __A few improvements to rds source (#4765)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 26 Jul 2024 13:31:54 -0500
    
    
    * Add error logging to event handlers
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add tls config and enable tls by default
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add original event name to metadata
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update metadata for export and stream events
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add some fixes
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Remove config alias ssl
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fixes a bug with HistogramAggregateAction where the startTime may be incorrect. This was discovered by a flaky test. (#4749)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 24 Jul 2024 23:01:21 -0700
    
    
    The HistogramAggregateAction was incorrectly using the current time as the
    start time for the aggregation when creating the group. The very first event&#39;s
    time was overridden by the current system time. If this event had the earliest
    time, then the overall histogram would never get the correct start time. This
    is fixed by removing an errant line. I also added a unit test to directly test
    this failure scenario.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for taking snapshots on RDS/Aurora Clusters (#4761)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 24 Jul 2024 22:52:38 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Revert &#34;Run tests on the current JVM for Java 17 &amp; 21 / Gradle 8.8 (#4730)&#34; (#4762)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 23 Jul 2024 17:03:57 -0500
    
    
    This reverts commit 67f3595805f07442d8f05823c9959b50358aa4d9.
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add stream processing for rds source (#4757)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 23 Jul 2024 16:30:35 -0500
    
    
    * Add stream processing
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Change s3 partition count default value
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add json description to AddEntry processor (#4752)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 23 Jul 2024 14:34:05 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __doc strings for enhanced UI view auto-generation (#4755)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Mon, 22 Jul 2024 14:40:18 -0700
    
    
    Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;

* __MAINT: Add json property descriptions for csv processor (#4751)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Mon, 22 Jul 2024 15:06:13 -0500
    
    
    * add json property descriptions
    Signed-off-by: Katherine Shen
    &lt;katshen@amazon.com&gt;

* __Improve the SQS shutdown process such that it does not prevent the pipeline from shutting down and no longer results in failures. Resolves #4575 (#4748)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 19 Jul 2024 11:40:57 -0700
    
    
    The previous approach to shutting down the SQS thread closed the SqsClient.
    However, with acknowledgments enabled, asynchronous callbacks would result in
    further attempts to either ChangeVisibilityTimeout or DeleteMessages. These
    were failing because the client was closed. Also, the threads would remain and
    prevent Data Prepper from correctly shutting down. With this change, we
    correctly stop each processing thread. Then we close the client. Additionally,
    the SqsWorker now checks that it is not stopped before attempting to change the
    message visibility or delete messages.
     Additionally, I found some missing test cases. Also, modifying this code and
    especially unit testing it is becoming more difficult, so I performed some
    refactoring to move message parsing out of the SqsWorker.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __MAINT: add json property descriptions for kv configs (#4747)__

    [Katherine Shen](mailto:40495707+shenkw1@users.noreply.github.com) - Fri, 19 Jul 2024 10:04:50 -0700
    
    
    add json property descriptions for kv configs
     Signed-off-by: Katherine Shen &lt;katshen@amazon.com&gt;

* __Updates Jackson to 2.17.2. Related to #4729. (#4744)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 17 Jul 2024 15:49:55 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updating the Python dependencies to resolve CVEs. Resolves #4715, #4713, 4714. (#4733)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 15 Jul 2024 11:55:11 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __REF: service-map processor with the latest config model (#4734)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 15 Jul 2024 11:02:40 -0500
    
    
    * REF: service-map processor with the latest config model
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: add documentation in json property description for date processor (#4719)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 15 Jul 2024 10:30:14 -0500
    
    
    * MAINT: add documentation in json property description for date processor
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __REF: grok processor with the latest config model (#4731)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 15 Jul 2024 10:05:10 -0500
    
    
    * REF: grok processor with the latest config model
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Load exported S3 files in RDS source (#4718)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 15 Jul 2024 09:57:24 -0500
    
    
    * Add s3 file loader
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Make checkExportStatus a callable
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Fix unit tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add load status and record converter
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update unit tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Restore changes for test
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __MAINT: backfill doc into json property for trim_string (#4728)__

    [Qi Chen](mailto:qchea@amazon.com) - Sat, 13 Jul 2024 00:31:45 -0500
    
    
    * MAINT: backfill doc into json property for trim_string
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: backfill documentation in JsonPropertyDescription for split_string (#4720)__

    [Qi Chen](mailto:qchea@amazon.com) - Sat, 13 Jul 2024 00:31:23 -0500
    
    
    * MAINT: add documentation in JsonPropertyDescription for split_string
    processor
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: backfill doc in json property description for otel_metrics (#4722)__

    [Qi Chen](mailto:qchea@amazon.com) - Sat, 13 Jul 2024 00:30:56 -0500
    
    
    * MAINT: backfill doc in json property description for otel_metrics
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: add json property description into obfuscate processor (#4706)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 12 Jul 2024 22:28:10 -0500
    
    
    * MAINT: add json property description
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Run tests on the current JVM for Java 17 &amp; 21 / Gradle 8.8 (#4730)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 12 Jul 2024 15:35:02 -0700
    
    
    Run tests on the current JVM rather than always using Java 11 for the tests.
    This fixes a problem with our current GitHub tests where we are running against
    only Java 11 even though we want to run against different Java versions (11,
    17, 21). Updates the Gradle version to 8.8.
     Fix Java 21 support in the AbstractSink by removing usage of Thread::stop
    which now always throws an UnsupportedOperationException.
     Use only microsecond precision time when comparing the times in the event_json
    codec. These tests are failing now on Java 17 and 21 with precision errors.
     Fixed a randomly failing test in BlockingBufferTests where a value 0 caused an
    IllegalArgumentException.
     Logging changes to avoid noise in the Gradle builds in GitHub.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __MAINT: backfill documentation into json description for truncate processor (#4726)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 12 Jul 2024 13:30:05 -0500
    
    
    * MAINT: backfill documentation into json description for truncate processor
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: backfill documentation into json property for substitute_string (#4727)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 12 Jul 2024 13:29:41 -0500
    
    
    * MAINT: backfill documentation into json property for substitute_string
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: backfill documentation into json description for delete_entries (#4721)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 12 Jul 2024 13:29:16 -0500
    
    
    * MAINT: backfill documentation into json description for delete_entries
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: backfill documentation in json description for otel_traces (#4724)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 12 Jul 2024 12:40:52 -0500
    
    
    * MAINT: backfill documentation in json property description for otel_traces
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __MAINT: backfill documentation into json description for string_converter (#4725)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 12 Jul 2024 12:40:24 -0500
    
    
    * MAINT: backfill documentation into json description for string_converter
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Mockito 5 (#4712)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Jul 2024 11:17:14 -0700
    
    
    Mockito 5
    
    * Synchronize the MetricsTestUtil methods to avoid test failures.
    * Create a copy of the collections to remove in MetricsTestUtil.
    * Updated two tests to JUnit 5 and to use mocks instead of actual metrics.
    Updates to MetricsTestUtil to provide clarity on NPEs.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates to the AWS Lambda Sink tests to fix a flaky test. Also adds SLF4J logging for these tests. (#4723)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Jul 2024 08:07:32 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update the rename_keys and delete_entries processors to use the EventKey. (#4636)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 10 Jul 2024 08:50:39 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update the mutate string processors to use the EventKey. #4646 (#4649)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 10 Jul 2024 08:50:21 -0700
    
    
    Change the source and keys properties for mutate string processors to use
    EventKey such that they are parsed by Data Prepper core. Also, use the
    TestEventFactory in the tests to avoid use of JacksonEvent directly. Removes an
    unused class.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes Zookeeper from Data Prepper. This was a transitive dependency from Hadoop. (#4707)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 10 Jul 2024 08:49:20 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the user_agent processor to use the EventKey. (#4628)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 9 Jul 2024 13:57:19 -0700
    
    
    Updates the user_agent processor to use the EventKey.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    Co-authored-by: Karsten
    Schnitter &lt;k.schnitter@sap.com&gt;

* __Introducing delete input configuration option for some parsers (#4702)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Thu, 4 Jul 2024 11:11:27 -0700
    
    
    * Introduced delete_source configuration option to give flexibility for the
    user to drop the raw source record if they don&#39;t want to propagate it
    downstream
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * addressing review comments
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * added delete_source option to other similar parser classes
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#4593)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 3 Jul 2024 13:49:02 -0700
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.14.12 to 1.14.17.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.12...byte-buddy-1.14.17)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy-agent in /data-prepper-plugins/opensearch (#4592)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 3 Jul 2024 12:56:23 -0700
    
    
    Bumps [net.bytebuddy:byte-buddy-agent](https://github.com/raphw/byte-buddy)
    from 1.14.12 to 1.14.17.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.14.12...byte-buddy-1.14.17)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Key Value processor value grouping optimization (#4704)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 3 Jul 2024 12:43:46 -0700
    
    
    * dplive1.yaml
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Optimize findInStartGroup in KV processor
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Rebased to latest
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Cleanup
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Minor improvements to code
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Updates to the CODE_OF_CONDUCT.md from the opensearch-project&#39;s official CODE_OF_CONDUCT.md. (#4665)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 3 Jul 2024 10:51:37 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates Parquet to 1.14.1 and Hadoop to 3.4.0. Make use of Gradle&#39;s version catalogue for Parquet. (#4705)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 3 Jul 2024 09:41:10 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Export to S3 in RDS source (#4664)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 3 Jul 2024 09:51:34 -0500
    
    
    * Trigger RDS export to S3
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add unit tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Remove unused imports
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address further comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Make checkSnapshotStatus a runnable
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Revert &#34;Make checkSnapshotStatus a runnable&#34;
     This reverts commit 5caed6ffb218d64180b10285c5c9115f21d6f3a2.
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Parquet codec tests fix (#4698)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 2 Jul 2024 09:26:16 -0700
    
    
    Parquet codec tests fix
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Bump org.skyscreamer:jsonassert from 1.5.1 to 1.5.3 in /data-prepper-api (#4678)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 2 Jul 2024 07:18:48 -0700
    
    
    Bumps [org.skyscreamer:jsonassert](https://github.com/skyscreamer/JSONassert)
    from 1.5.1 to 1.5.3.
    - [Release notes](https://github.com/skyscreamer/JSONassert/releases)
    -
    [Changelog](https://github.com/skyscreamer/JSONassert/blob/master/CHANGELOG.md)
    
    -
    [Commits](https://github.com/skyscreamer/JSONassert/compare/jsonassert-1.5.1...jsonassert-1.5.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.skyscreamer:jsonassert
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.maven:maven-artifact in /data-prepper-plugins/opensearch (#4692)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Jul 2024 14:46:28 -0700
    
    
    Bumps [org.apache.maven:maven-artifact](https://github.com/apache/maven) from
    3.9.6 to 3.9.8.
    - [Release notes](https://github.com/apache/maven/releases)
    - [Commits](https://github.com/apache/maven/compare/maven-3.9.6...maven-3.9.8)
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.maven:maven-artifact
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#4685)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Jul 2024 14:45:24 -0700
    
    
    Bumps software.amazon.awssdk:auth from 2.25.21 to 2.26.12.
    
    ---
    updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump braces from 3.0.2 to 3.0.3 in /testing/aws-testing-cdk (#4638)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Jul 2024 14:44:18 -0700
    
    
    Bumps [braces](https://github.com/micromatch/braces) from 3.0.2 to 3.0.3.
    - [Changelog](https://github.com/micromatch/braces/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/micromatch/braces/compare/3.0.2...3.0.3)
    
    ---
    updated-dependencies:
    - dependency-name: braces
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump ws from 7.5.9 to 7.5.10 in /release/staging-resources-cdk (#4639)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Jul 2024 14:43:47 -0700
    
    
    Bumps [ws](https://github.com/websockets/ws) from 7.5.9 to 7.5.10.
    - [Release notes](https://github.com/websockets/ws/releases)
    - [Commits](https://github.com/websockets/ws/compare/7.5.9...7.5.10)
    
    ---
    updated-dependencies:
    - dependency-name: ws
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.wiremock:wiremock in /data-prepper-plugins/s3-source (#4683)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Jul 2024 14:43:01 -0700
    
    
    Bumps [org.wiremock:wiremock](https://github.com/wiremock/wiremock) from 3.4.2
    to 3.8.0.
    - [Release notes](https://github.com/wiremock/wiremock/releases)
    - [Commits](https://github.com/wiremock/wiremock/compare/3.4.2...3.8.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.wiremock:wiremock
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.apptasticsoftware:rssreader in /data-prepper-plugins/rss-source (#4672)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Jul 2024 14:42:31 -0700
    
    
    Bumps [com.apptasticsoftware:rssreader](https://github.com/w3stling/rssreader)
    from 3.6.0 to 3.7.0.
    - [Release notes](https://github.com/w3stling/rssreader/releases)
    - [Commits](https://github.com/w3stling/rssreader/compare/v3.6.0...v3.7.0)
    
    ---
    updated-dependencies:
    - dependency-name: com.apptasticsoftware:rssreader
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates our usage of the Apache Parquet project to use their new interfaces over the old Hadoop ones. This allows use to be ready to extract Hadoop as other changes are made to the Parquet project. Remove some Hadoop transitive dependencies and make Hadoop runtime only where possible. Added a test for INT96, clean up some test files.  Contributes toward #4612. (#4623)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 1 Jul 2024 13:54:53 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds the TRIAGING.md file to outline our triaging process (#4630)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 27 Jun 2024 17:47:18 -0700
    
    
    Adds the TRIAGING.md file, which outlines for the community the Data Prepper
    triaging process.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Enhanced Kafka source logging through the use of MDC and better thread names for Kafka source threads. Resolves #4126. (#4663)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 26 Jun 2024 12:25:57 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support default route option for Events that match no other route (#4662)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 26 Jun 2024 07:15:04 -0700
    
    
    Support default route option for Events that match no other route
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Updates the chunking algorithm for http source&#39;s JsonCodec to account for actual byte size. Test using Unicode characters to prove this was incorrectly chunking and verify against future changes. (#4656)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 25 Jun 2024 15:09:27 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Refactor lambda plugin (#4643)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Tue, 25 Jun 2024 09:51:28 -0700
    
    
    * Refactor lambda plugin
     Signed-off-by: Srikanth Govindarajan &lt;srikanthjg123@gmail.com&gt;
    
    * Address comments
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;
    
    * Address comments 2
     Signed-off-by: Srikanth Govindarajan &lt;srigovs@amazon.com&gt;
    
    ---------
     Signed-off-by: Srikanth Govindarajan &lt;srikanthjg123@gmail.com&gt;
    Signed-off-by:
    Srikanth Govindarajan &lt;srigovs@amazon.com&gt;

* __Fixes the loading of peer-forwarders when using multiple workers. This fixes a bug where the service_map processor would not load in a pipeline with multiple workers. Resolves #4660. (#4661)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 25 Jun 2024 08:07:40 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __FIX: remove logging that includes credential info on kafka (#4659)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 25 Jun 2024 09:11:40 -0500
    
    
    * FIX: use sensitive marker
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add an option to count unique values of specified key(s) to CountAggregateAction (#4652)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 24 Jun 2024 15:46:42 -0700
    
    
    Add an option to count unique values of specified key(s) to
    CountAggregateAction
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __MAINT: change log level for consumer properties in kafka source (#4658)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 24 Jun 2024 16:56:23 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fixes performance regression with JacksonEvent put/delete operations. (#4650)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Jun 2024 08:37:03 -0700
    
    
    With the addition of the EventKey, JacksonEvent always creates a
    JacksonEventKey in order to use the same code for all paths. However, when
    put/delete calls are made with a String key, JacksonEvent does not need the
    JSON Pointer. But, it is created anyway. This adds more work to the put/delete
    calls that have not yet migrated to the String version. This fixes regression
    by adding a lazy initialization option when used in JacksonEvent. We should not
    be lazy when used with the EventKeyFactory since we may lose some up-front
    validations.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __add remove_brackets option to flatten-processor (#4616) (#4653)__

    [timo-mue](mailto:timo.mueller@tower.telekom-cloudcenter.de) - Fri, 21 Jun 2024 10:04:04 -0500
    
    
    Signed-off-by: Timo Mueller &lt;timo.mueller@tower.telekom-cloudcenter.de&gt;

* __Add support to configure metric name for count and histogram actions (#4642)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 20 Jun 2024 11:05:40 -0700
    
    
    * rebased to latest
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * renamed name to metric_name
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Support plugins defining the EventKey in the plugin configuration classes. Data Prepper will deserialize the EventKey from the pipeline configuration and validate @NotEmpty validations. Builds on the #1916. (#4635)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 19 Jun 2024 15:08:12 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __DocumentDB Source improvements (#4645)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 19 Jun 2024 16:09:54 -0500
    
    
    * Extend the export partition ownership during query partition creation
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add support to shutdown task refresher that starts export and stream
    scheduler/worker on data prepper shutdown
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add AcknowledgmentStatus enum and code refactor to fail negative ack right
    away
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Track last scan time from before scan starts instead of based on last Modified of objects (#4493)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 19 Jun 2024 14:55:10 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bump urllib3 in /release/smoke-tests/otel-span-exporter (#4640)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jun 2024 07:23:44 -0700
    
    
    Bumps [urllib3](https://github.com/urllib3/urllib3) from 1.26.18 to 1.26.19.
    - [Release notes](https://github.com/urllib3/urllib3/releases)
    - [Changelog](https://github.com/urllib3/urllib3/blob/1.26.19/CHANGES.rst)
    - [Commits](https://github.com/urllib3/urllib3/compare/1.26.18...1.26.19)
    
    ---
    updated-dependencies:
    - dependency-name: urllib3
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump urllib3 in /examples/trace-analytics-sample-app/sample-app (#4631)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 18 Jun 2024 16:50:42 -0700
    
    
    Bumps [urllib3](https://github.com/urllib3/urllib3) from 2.0.7 to 2.2.2.
    - [Release notes](https://github.com/urllib3/urllib3/releases)
    - [Changelog](https://github.com/urllib3/urllib3/blob/main/CHANGES.rst)
    - [Commits](https://github.com/urllib3/urllib3/compare/2.0.7...2.2.2)
    
    ---
    updated-dependencies:
    - dependency-name: urllib3
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Implements equals, hashCode, and toString for JacksonEventKey. (#4633)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 18 Jun 2024 16:49:34 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Deprecates PluginSetting which should not be used for plugins anymore. (#4624)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 17 Jun 2024 12:55:38 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add Aggregate event handle (#4625)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 17 Jun 2024 12:34:33 -0700
    
    
    Aggregate event handle
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
     Co-authored-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Adds the EventKey and EventKeyFactory.  (#4627)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 17 Jun 2024 08:40:28 -0700
    
    
    Adds the EventKey and EventKeyFactory. Resolves #1916.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates to Armeria 1.29.0 which fixes a bug that may help with #4080. (#4629)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 17 Jun 2024 07:26:05 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __MAINT: bump io.confluent:* packages to match org.apache.kafka.* (#4626)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 14 Jun 2024 11:55:28 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Support multiple aggregate processors in local mode (#4574)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 14 Jun 2024 09:27:17 -0700
    
    
    * Rebased to latest
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Tools to generate User Agent strings in the performance-test project (#4620)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 14 Jun 2024 09:00:52 -0700
    
    
    Changes to the performance-test project to generate User Agent strings. Used to
    help reproduce and test #4618.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Rebased to latest (#4614)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 14 Jun 2024 08:37:55 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __OpenSearch Sink add support for sending pipeline parameter in BulkRequest (#4609)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Thu, 13 Jun 2024 23:36:00 -0700
    
    
    * Add support in OpenSearch sink to pass pipeline parameter in the index and
    create action operation for bulk api requests
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    
    * Add more unit tests to increase code coverage
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    
    * Update README
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    
    * Fix the OpenSearch Integration tests
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;
    
    ---------
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;

* __Caffeine-based caching parser for the user_agent processor (#4619)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 13 Jun 2024 08:22:15 -0700
    
    
    Adds and uses a Caffeine-based caching parser for the user_agent processor.
    Resolves #4618
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __FIX: decouple msk auth from glue auth in KafkaSource (#4613)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 12 Jun 2024 14:46:08 -0500
    
    
    * FIX: decouple msk from aws block
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fix missing closing parenthesis in CLOUDFRONT_ACCESS_LOG pattern (#4607)__

    [Joël Marty](mailto:134835+joelmarty@users.noreply.github.com) - Wed, 12 Jun 2024 10:45:37 -0700
    
    
    Signed-off-by: Joël Marty &lt;134835+joelmarty@users.noreply.github.com&gt;

* __Aggrerate processor : add option to allow raw events (#4598)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 10 Jun 2024 14:55:09 -0700
    
    
    * Aggregate Processor: Add support to allow raw events
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modify test to check for aggregated tag
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

* __Updates Python requests to 2.32.3 in the smoke tests project to address CVE-2024-35195. (#4610)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 7 Jun 2024 14:07:03 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for lambda sink (#4292)__

    [Srikanth Govindarajan](mailto:srikanthjg123@gmail.com) - Thu, 6 Jun 2024 20:17:27 +0000
    
    
    * Add support for lambda sink
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    
    * Address event handle comment
     Signed-off-by: Srikanth Govindarajan &lt;srikanthjg123@gmail.com&gt;
    
    ---------
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;
    Signed-off-by: Srikanth
    Govindarajan &lt;srikanthjg123@gmail.com&gt;

* __Fix KeyValue Processor value grouping bug (#4606)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 6 Jun 2024 12:25:36 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Refactor http source functionality for supporting a new OpenSearch API source in DataPrepper (#4570)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Tue, 4 Jun 2024 16:49:07 -0700
    
    
    Refactor http source configuration to a separate http source common package.
     Signed-off-by: Souvik Bose &lt;souvik04in@gmail.com&gt;

* __Modify Key Value processor to support string literal grouping (#4599)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 4 Jun 2024 14:19:48 -0700
    
    
    * Key Value Processor fixes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * New options to KV processor
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Add string literal support
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Remove unnecessary changes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Remove unnecessary changes
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed tests
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

* __Introduced BigDecimalConverter  (#4557)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Tue, 4 Jun 2024 13:40:46 -0700
    
    
    * Introduced BigDecimalConverter that users can use as part of
    convert_entry_type processor that currently exists. Optionally, users can also
    specify required scaling needed on the converted
     Signed-off-by: Santhosh Gandhe &lt;gandheaz@amazon.com&gt;
    
    * Added Test case for the newly introduced class. Removed * imports as per the
    review comment
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * Avoiding using a deprecated method. Added additional test cases
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * Additional tests to increase the coverage
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * removed &#34;scale&#34; being the state of BigDecimal converter. We are now passing
    the scale while converting the instance only when the instance is
    BigDecimalConverter
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * test case fix to be inline with the previous commit
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * test case fix to be inline with the previous commit
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * addressing review comments
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * renaming bigdecimal to big_decimal
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * Introduced ConverterArguments as a way to pass additional arguments to the
    converter and avoided conditional statement for calling converter methods
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * Added additional override convert method to reduce the changes across the
    code
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * additional Test cases to increase the coverage
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    * added additional tests for converter cases
     Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Santhosh Gandhe &lt;gandheaz@amazon.com&gt;
    Signed-off-by: Santhosh
    Gandhe &lt;1909520+san81@users.noreply.github.com&gt;

* __Add Rds source config (#4573)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 4 Jun 2024 15:19:35 -0500
    
    
    * Add rds source config and some skeleton code
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add unit tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add cluster and aurora options
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Track the source of request for Kafka server (#4572)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 31 May 2024 09:46:28 -0500
    
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add support for Kafka headers and timestamp in the Kafka Source (#4566)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 30 May 2024 09:31:47 -0700
    
    
    * Add support for Kafka headers and timestamp in the Kafka Source
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fix the typo
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * fixed checkstyle error
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Add default role and region configuration to the data-prepper-config.yaml via extensions (#4559)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 22 May 2024 12:50:07 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Corrected the release date for 2.8.0. (#4555)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 20 May 2024 13:30:25 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Release Notes for version 2.8 (#4538)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 20 May 2024 12:46:53 -0700
    
    
    * Release Notes for version 2.8
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
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

* __Addressed review comments (#4552)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 20 May 2024 11:48:31 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Fix DocDB export and stream processing self recovery with invalid database or collection name (#4553)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 20 May 2024 13:15:55 -0500
    
    
    * Fix DocDB export and stream processing self recovery with invalid database or
    collection name
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Fix unit test
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Change log for version 2.8 (#4539)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 16 May 2024 10:03:13 -0700
    
    
    * Change log for version 2.8
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Change log - updated to latest
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Change log - updated to latest in 2.8 branch
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Adding &#39;single_word_only&#39; option to obfuscate processor (#4476)__

    [Utkarsh Agarwal](mailto:126544832+Utkarsh-Aga@users.noreply.github.com) - Wed, 15 May 2024 15:38:36 -0700
    
    
    Adding &#39;single_word_only&#39; option to obfuscate processor
     Signed-off-by: Utkarsh Agarwal &lt;utkarsh07379@gmail.com&gt;

* __Updates werkzeug to 3.0.3 in examples to fix CVE-2024-34069. Resolves #4515 (#4546)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 15 May 2024 15:04:53 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Logging update and config validation (#4541)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 14 May 2024 17:57:24 -0500
    
    
    * Logging improvements for export and stream processing for DocumentDB source
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add validation for DocumentDB Collection Config
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add aggregate metrics (#4531)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 14 May 2024 16:30:13 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updates the next Data Prepper version to 2.9 (#4532)__

    [Krishna Kondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 14 May 2024 14:19:56 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;

* __Adds an ndjson input codec. This reads JSON objects for ND-JSON and more lenient formats that do not have the newline. (#4533)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 May 2024 12:10:01 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Address route and subpipeline for pipeline tranformation (#4528)__

    [Srikanth Govindarajan](mailto:srigovs@amazon.com) - Mon, 13 May 2024 15:58:11 -0700
    
    
    Address route and subpipeline for pipeline tranformation
     Signed-off-by: srigovs &lt;srigovs@amazon.com&gt;


