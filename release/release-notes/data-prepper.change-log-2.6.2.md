
* __Adds release notes for Data Prepper 2.6.2 (#4149) (#4151)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 19 Feb 2024 08:33:17 -0800
    
    EAD -&gt; refs/heads/2.6, tag: refs/tags/2.6.2, refs/remotes/upstream/2.6
    * Adds release notes for Data Prepper 2.6.2
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 119ccb6410400e51554be41c11b0a115dd176eac)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Generated THIRD-PARTY file for 52f4697 (#4150)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 19 Feb 2024 08:28:41 -0800
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Bump grpcio in /release/smoke-tests/otel-span-exporter (#4104) (#4148)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 19 Feb 2024 06:40:20 -0800
    
    
    Bumps [grpcio](https://github.com/grpc/grpc) from 1.53.0 to 1.53.2.
    - [Release notes](https://github.com/grpc/grpc/releases)
    -
    [Changelog](https://github.com/grpc/grpc/blob/master/doc/grpc_release_schedule.md)
    
    - [Commits](https://github.com/grpc/grpc/compare/v1.53.0...v1.53.2)
    
    ---
    updated-dependencies:
    - dependency-name: grpcio
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;
    (cherry picked from commit 30d68966a85366b244dd4db6c067707dd74764c2)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates the JDK version of the release to jdk-17.0.10+7. (#4136) (#4141)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Sat, 17 Feb 2024 07:12:07 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 8bf0daa4bb35d80a5c63e30f552eda04414c3e4b)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __FIX: plugin callback not loaded for secret refreshment (#4079) (#4140)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 16 Feb 2024 12:02:51 -0800
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
    (cherry picked from commit 2f4c8c9c7f8d4ec6e76c3653ef8446fcee35cd50)
     Co-authored-by: Qi Chen &lt;qchea@amazon.com&gt;

* __Bump com.github.seancfoley:ipaddress in /data-prepper-expression (#4060) (#4077)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 16 Feb 2024 09:57:16 -0600
    
    
    Bumps
    [com.github.seancfoley:ipaddress](https://github.com/seancfoley/IPAddress) from
    5.4.0 to 5.4.2.
    - [Release notes](https://github.com/seancfoley/IPAddress/releases)
    - [Commits](https://github.com/seancfoley/IPAddress/compare/v5.4.0...v5.4.2)
    
    ---
    updated-dependencies:
    - dependency-name: com.github.seancfoley:ipaddress
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;
    (cherry picked from commit 16d0d907a29483a5f7bdcf2c04c7bda87121366d)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Require json-path 2.9.0 to fix CVE-2023-51074. Resolves #3919. (#4132) (#4133)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 16 Feb 2024 06:23:02 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 838744c9f00fa9e75c4cd254a83f1fb493ca9ba9)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bug where s3 scan could skip when lastModifiedTimestamps are the same (#4124) (#4127)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 14 Feb 2024 13:31:29 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Catch exception instead of shutting down in date processor (#4108) (#4117)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 12 Feb 2024 16:56:42 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 0841ac17ae18a76610842be3c0ff3b0b0dc1e453)

* __Updates jline to 3.25.0 to resolve CVE-2023-50572. (#4020) (#4029)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 30 Jan 2024 07:50:41 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 8f0268bb4ac891467133096154acf42c39fd5aca)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Cancel the existing grok task when a timeout occurs. Resolves #4026 (#4027) (#4028)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 29 Jan 2024 16:28:03 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 76163969d6f030719f0a32dfdc2c4f4253dadd51)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Mark the EventHandle as transient in the JacksonEvent to fix a serialization error with peer forwarding. Resolves #3981. (#3983) (#3987)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 19 Jan 2024 08:56:41 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 564a749c8fec9a8c0cb7ec71d52fa0fc83760101)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Release events that are not routed to any sinks (#3959) (#3966)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 17 Jan 2024 12:07:11 -0800
    
    
    * Release events that are not routed to any sinks
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed a bug in the code that&#39;s causing the test failures
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to determine unrouted events after all routing is done
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Add test yaml files
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
    (cherry picked from commit f21937adc96e87e2dc932348b9f609afbf18f94c)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Updates wiremock to 3.3.1. This also involves changing the groupId to org.wiremock which is the new groupId as of 3.0.0. (#3969) (#3971)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 17 Jan 2024 11:51:49 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit e0ed91c11d9942867f89c29c08b37b52d2ce2652)

* __Version bump to 2.6.2 (#3946)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 12 Jan 2024 14:13:14 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add your public modifier back to one of the AbstractBuffer constructors to attempt to fix the build. (#3947) (#3948)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 11 Jan 2024 10:45:05 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 677643df66dc0cf62091586d8bb9a3417030bd5a)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Corrects the bufferUsage metric by making it equal to the difference between the bufferCapacity and the available permits in the semaphore. Adds a new capacityUsed metric which tracks the actual capacity used by the semaphore which blocks. Resolves #3936. (#3937) (#3940)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 10 Jan 2024 15:17:41 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit d61b0c5d18210db202700a8a08ebcf5f6631768d)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix for [BUG] Data Prepper is losing connections from S3 pool (#3836) (#3844)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 14:00:00 -0800
    
    
    * Fix for [BUG] Data Prepper is losing connections from S3 pool
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed CheckStyle errors
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    (cherry picked from commit f9be56a65562e4e3b4906bc02d93e2fe5c9d4928)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Fix Null Pointer Exception in KeyValue Processor (#3927) (#3932)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 9 Jan 2024 13:59:45 -0800
    
    
    * Fix Null Pointer Exception in KeyValue Processor
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Added a test case
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    (cherry picked from commit 35a69489c2f8621c8aa258ddd8dda105cd67a9e4)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Add 4xx aggregate metric and shard progress metric for dynamodb source (#3913) (#3921)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 5 Jan 2024 14:42:44 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit e6df3eb2cd46ebd13dd1c7b808d288c2f3d6ee51)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates Armeria to 1.26.4. This also updates io.grpc to 1.58.0 which has a slight breaking changing. This is fixed by explicitly adding io.grpc:grpc-inprocess to the build. (#3915) (#3917)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 5 Jan 2024 12:18:16 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit a243182f214583379249526f55215ae5b36d72d3)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates opensearch library to 1.3.14. And run integration test against 2.11.1 and 1.3.14 as well. Resolves #3837. (#3838) (#3862)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 2 Jan 2024 14:12:22 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit df2bde6cc4d752013dc9a6f9266f651b43668b23)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Require Mozilla Rhino 1.7.12 to fix SNYK-JAVA-ORGMOZILLA-1314295. (#3839) (#3842)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 19 Dec 2023 14:16:12 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit e09900a144753645be41cca7e1f618966e02ea58)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __rebasing to latest (#3846) (#3853)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 13 Dec 2023 09:11:55 -0800
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    (cherry picked from commit f9d9e978bec8aad2dba6f7bf41f5ed07e9d68a3f)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;


