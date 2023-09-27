
* __CVE fixes (#3385) (#3392)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 27 Sep 2023 22:14:02 +0530
    
    EAD -&gt; refs/heads/2.4, refs/remotes/upstream/2.4
    * CVE fixes
    CVE-2022-36944, WS-2023-0116, CVE-2021-39194, CVE-2023-3635,
    CVE-2023-36479, CVE-2023-40167
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Fix WS-2023-0236
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 5fdf95fa368cbf6a51aef44135e8e909d9fc58f9)

* __Improve logging for failed documents in the OpenSearch sink (#3387) (#3389)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 27 Sep 2023 21:05:28 +0530
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 63695e9f3c12afcd31e723d60dfb014e1af84000)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates commons-compress to 1.24.0 which fixes CVE-2023-42503. As part of this change, I updated the Apache commons projects to use the Gradle version catalog to keep versions in sync. Resolves #3347. (#3371) (#3388)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 27 Sep 2023 20:53:46 +0530
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit bcaaf1e30f1cda63d4e35224830db67b5210218c)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Rebased to latest (#3364) (#3372)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 27 Sep 2023 01:05:27 +0530
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    (cherry picked from commit 542b4517896f1f074a32575b9ee98fb737065ee2)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Fix issue of skipping new partitions/indices for the opensearch source (#3319) (#3383)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 27 Sep 2023 01:04:12 +0530
    
    
    Fix issue where the source coordinator would skip creating partitions for new
    items for the os source
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    
    ---------
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 778e9c7e0366da9310130a1e895c9390e50582c1)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bump org.hibernate.validator:hibernate-validator in /data-prepper-core (#2974) (#3369)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 20 Sep 2023 11:38:59 -0700
    
    
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
    (cherry picked from commit 759a8a45b62ed75ff254b8e5929dac15dc7e6318)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-codec:commons-codec (#2968) (#3370)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 20 Sep 2023 11:04:56 -0700
    
    
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
    (cherry picked from commit 47a875d17328494f07870ce8480da1ba4096d210)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Armeria 1.25.2 (#3351) (#3366)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 20 Sep 2023 11:04:08 -0700
    
    
    Updates Armeria to 1.25.2. This also removes a Gradle resolution strategy which
    fixes some dependencies to specific versions. Instead, use a dependency version
    requirement which allows for using newer versions. Resolves #3069.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit a016b7a2d848c80f3956f0a03603fe68ab562224)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates Trace Analytics sample appliction to run again (#3348) (#3353)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 20 Sep 2023 06:38:07 -0700
    
    
    Get the Trace Analytics sample app running again. This includes version updates
    for dependencies and some corrections from the previous PR which started using
    Temurin which brought in Ubuntu in the image. Adds GitHub Actions to verify
    that the trace-analytics example apps can still build Docker images.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 6e2942d0d5c55f2a02ab990b40ccbc39f51e3ae5)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the trace analytics sample app to run using the latest Spring Boot - 3.1.3. Also updates to using JDK 17 which is required, along with moving to the Temurin Docker image as the OpenJDK Docker image is deprecated. (#3343) (#3346)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 18 Sep 2023 08:24:38 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit c95eb92f98f46fd01b3979dfe7c09e32e7bb62ed)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __BUG: Stop S3 source on InterruptedException (#3331) (#3345)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 15 Sep 2023 14:06:40 -0500
    
    
    Stop S3 source on InterruptedException
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit d55fb736b5c1348a8d5dcb40d4180cec68ae6ae9)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix NPE in s3 scan partition supplier (#3317) (#3323)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 14 Sep 2023 12:00:02 -0700
    
    
    Fix potential NPE in s3 scan partition supplier
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit f61338a6aa2ff9838002ce2573e41572c60811f1)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates Bouncy Castle to 1.76. This moves the dependency into the version catalog and starts using the jdk18on series as Data Prepper requires Java 11 as a minimum anyway. (#3302) (#3307)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 8 Sep 2023 12:33:44 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit fc48b0e013a0e68c62803711322b9e4d9d2eba03)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump grpcio in /release/smoke-tests/otel-span-exporter (#2984) (#3315)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 7 Sep 2023 14:57:19 -0700
    
    
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
    (cherry picked from commit cd194c167233287b9be7fc83699a925cc6e44409)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates to Gradle 8.3; fixes deprecated Gradle behavior (#3269) (#3300)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 7 Sep 2023 14:56:52 -0700
    
    
    Updates to Gradle 8.3, including fixing deprecated behavior. Resolves #3267
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit db10b9ef2c887dccf21e6c4be63d42ce9ad021a9)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump certifi in /release/smoke-tests/otel-span-exporter (#3062) (#3314)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 7 Sep 2023 14:56:32 -0700
    
    
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
    (cherry picked from commit 33b59371d385de3a77090e4ab2de45b2c15138d3)
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Reduce sleep times in BlockingBufferTests to speed up unit tests. (#3221) (#3287)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 1 Sep 2023 10:39:17 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit e845966602bef5ce52d195a10012f46026b62740)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Checkstyle to the latest version - 10.12.3 - to attempt to remove Guava vulnerability. (#3276) (#3286)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 31 Aug 2023 08:36:08 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 14ec2e40df136ccec3afd12975e42aa382a51ffd)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes some unnecessary dependencies in the S3 sink and Parquet codecs (#3275) (#3283)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 31 Aug 2023 08:35:57 -0700
    
    
    Removes some unnecessary dependencies in the S3 sink and Parquet codec
    projects. Updating the Parquet version to 1.13.1 consistently. Exclude HDFS
    client.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 005f2aa5b4e60202b6de4c1bf80f34eccac38fac)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes Maxmind license keys from test URLs. (#3270) (#3285)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 29 Aug 2023 11:46:11 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit f70ee73bc341b71a33f2d550299a8ed98e8fe245)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds Data Prepper 2.4.0 changelog. (#3223) (#3273)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 28 Aug 2023 12:35:37 -0700
    
    
    Adds Data Prepper 2.4.0 changelog.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit c4f75c83afcffc15fb432aabf71c563586b28845)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;


