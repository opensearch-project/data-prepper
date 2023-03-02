
* __Add release notes for 2.1.0 (#2354)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 2 Mar 2023 13:52:16 -0600
    
    EAD -&gt; refs/heads/change-log-2.1, refs/remotes/upstream/main, refs/remotes/origin/main, refs/remotes/origin/HEAD, refs/heads/main
    * Add release notes for 2.1.0
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added backoff for SQS to reduce logging (#2326)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 2 Mar 2023 12:14:20 -0600
    
    
    

* __Removed default service endpoint for otel sources (#2346)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 2 Mar 2023 11:38:54 -0600
    
    
    * Removed default service endpoint for otel sources
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Replace the java.util.* allowed pattern in the ObjectInputFilter with specific classes which are commonly used in Data Prepper. (#2351)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 2 Mar 2023 09:53:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Explicitly set the GitHub Actions thumbprint to resolve #2343. Updated the AWS CDK as well. (#2345)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 1 Mar 2023 15:40:05 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated version to 2.2 on main (#2342)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 1 Mar 2023 13:27:15 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Generated THIRD-PARTY file for 062ae95 (#2344)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 1 Mar 2023 13:26:20 -0600
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: asifsmohammed
    &lt;asifsmohammed@users.noreply.github.com&gt;

* __Use an ObjectInputFilter to serialize allow deserialization of only certain objects in peer-to-peer connections. Additionally, it refactors some application configurations to improve integration testing. Fixes #2310. (#2311)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 1 Mar 2023 10:06:52 -0600
    
    efs/heads/changelog-2.1
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-core (#2333)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Mar 2023 09:47:42 -0600
    
    
    Bumps org.apache.logging.log4j:log4j-bom from 2.19.0 to 2.20.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core from 3.21.0 to 3.24.2 (#2331)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Mar 2023 09:47:06 -0600
    
    
    Bumps org.assertj:assertj-core from 3.21.0 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.junit.jupiter:junit-jupiter-api from 5.9.0 to 5.9.2 (#2332)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Mar 2023 09:46:26 -0600
    
    
    Bumps
    [org.junit.jupiter:junit-jupiter-api](https://github.com/junit-team/junit5)
    from 5.9.0 to 5.9.2.
    - [Release notes](https://github.com/junit-team/junit5/releases)
    - [Commits](https://github.com/junit-team/junit5/compare/r5.9.0...r5.9.2)
    
    ---
    updated-dependencies:
    - dependency-name: org.junit.jupiter:junit-jupiter-api
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Support for path in OTel sources (#2297)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 28 Feb 2023 19:57:38 -0600
    
    
    Initial commit for OTel trace path changes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;

* __Fix grok processor to not create a new record (#2325)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 28 Feb 2023 19:56:04 -0600
    
    
    * Fix grok processor to not create a new record
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkStyleMain  failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump org.springframework:spring-context in /data-prepper-expression (#2223)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Feb 2023 18:27:04 -0600
    
    efs/heads/otel-paths
    Bumps
    [org.springframework:spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.springframework:spring-context in /data-prepper-core (#2214)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Feb 2023 16:52:20 -0600
    
    
    Bumps
    [org.springframework:spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump io.spring.dependency-management from 1.0.11.RELEASE to 1.1.0 (#2106)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Feb 2023 16:48:25 -0600
    
    
    Bumps io.spring.dependency-management from 1.0.11.RELEASE to 1.1.0.
    
    ---
    updated-dependencies:
    - dependency-name: io.spring.dependency-management
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.fasterxml.jackson.datatype:jackson-datatype-jdk8 (#2217)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Feb 2023 16:47:36 -0600
    
    
    Bumps com.fasterxml.jackson.datatype:jackson-datatype-jdk8 from 2.14.1 to
    2.14.2.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jdk8
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added path support for HTTP source (#2277)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 27 Feb 2023 15:26:58 -0600
    
    
    * Added path support for HTTP source
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated Data Prepper base image for e2e tests (#2269)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 27 Feb 2023 15:08:13 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Loop until interrupted in SqsWorker (#2306)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 24 Feb 2023 00:10:26 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Performance Improvement - Avoid event copying for the first sub-pipeline (#2290)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 23 Feb 2023 22:08:47 -0600
    
    
    Avoid event copying for the first sub-pipeline
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump slf4j-simple from 1.7.36 to 2.0.6 (#2112)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 22 Feb 2023 10:39:21 -0600
    
    
    Bumps [slf4j-simple](https://github.com/qos-ch/slf4j) from 1.7.36 to 2.0.6.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.36...v_2.0.6)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-simple
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Upgrades opentelemetry dependencies (#2288)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Tue, 21 Feb 2023 21:16:58 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Fix error message printed when OpenSearch Sink fails to initialize (#2296)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 21 Feb 2023 14:38:36 -0600
    
    
    Fix error message printed when OpenSearch Sink fails to initialize
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __adding backwards compatibility support in versioning check (#2295)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 21 Feb 2023 14:13:50 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __[2263] adding versioning property to pipeline yaml configuration (#2292)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 21 Feb 2023 09:08:39 -0600
    
    
    * [2263] adding versioning property to pipeline yaml configuration
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;
    
    * adding hashCode support and providing documentation for updating the version
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Updates to opensearch-java 2.2.0 (#2287)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 20 Feb 2023 15:18:55 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Replace an unnecessary builder pattern in trace raw processor with a factory method. This is a little simpler code and it reduces an object creation. (#2271)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Feb 2023 14:34:21 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Create DataPrepper server after pipeline initialization (#2284)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 17 Feb 2023 11:12:08 -0600
    
    
    * Create DataPrepper HttpServer after pipeline initialization
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove injection provider implementation
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Refactor PeerForwarderReceiveBuffer to extend AbstractBuffer to pick up metric logic (#2286)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 17 Feb 2023 10:48:29 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Add more metrics to OTEL metrics source (#2283)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 17 Feb 2023 10:24:30 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Make batching queue depth configurable, fill in some missing documentation around batching (#2278)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 17 Feb 2023 10:24:06 -0600
    
    
    * Make batching queue depth configurable, fill in some missing documentation
    around batching
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix validation message
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Actually fix validation message
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Update to opensearch 1.3.8. Resolves #2192, #2193 (#2285)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Feb 2023 15:56:36 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __OTel Trace Source: Logging and integration tests (#2262)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Feb 2023 11:12:26 -0600
    
    
    Modified some of the logging for the OTel Trace source. Added integration tests
    which actually verify that gRPC requests operate as expected. Print out SLF4J
    logs from tests to stdout to help with debugging.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Created untriaged issue workflow. (#2276)__

    [Daniel (dB.) Doubrovkine](mailto:dblock@amazon.com) - Wed, 15 Feb 2023 13:15:53 -0600
    
    
    Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Long int fix (#2265)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 14 Feb 2023 16:13:20 -0600
    
    
    * OpenSearchSink should close open files before retrying initialization
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Minor fixes to data prepper plugins documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix Long Integer comparisons
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added more tests for long integer comparisons
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __CVE: updated guava version (#2254)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 14 Feb 2023 15:08:18 -0600
    
    
    * CVE: updated guava version
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;
    
    * Updated guava to use catalog
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;

* __Update to Armeria 1.22.1 which fixes #2206. (#2274)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Feb 2023 13:36:21 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix: e2e tests and added documentation (#2267)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 13 Feb 2023 14:40:27 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Removes some unnecessary methods from ServiceMapRelationship and adds additional tests to verify behavior. (#2200)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Feb 2023 13:45:31 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Created a metric for the overall JVM memory usage - both heap and non-heap. (#2266)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Feb 2023 13:24:48 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes index_type convention (#2261)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Fri, 10 Feb 2023 14:47:20 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Minor fixes to data prepper plugins documentation (#2260)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 10 Feb 2023 13:39:12 -0600
    
    
    * OpenSearchSink should close open files before retrying initialization
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Minor fixes to data prepper plugins documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __ENH: peer forwarding codec and model (#2256)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 10 Feb 2023 09:19:51 -0600
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Use the Gradle version catalog for common software versions to the extent that it can replace the versionMap. (#2253)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 9 Feb 2023 13:49:11 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __OpenSearchSink should close open files before retrying initialization (#2255)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 9 Feb 2023 11:15:19 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Added trace peer forwarder doc (#2245)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 8 Feb 2023 18:10:26 -0600
    
    
    * Added trace peer forwarder doc
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactor to remove stream in RemotePeerForwarder as micro optimization (#2250)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 8 Feb 2023 18:09:17 -0600
    
    
    * Refactor to remove stream in RemotePeerForwarder as micro optimization
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove unused import
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Bump org.assertj:assertj-core (#2221)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 16:01:58 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump certifi in /release/smoke-tests/otel-span-exporter (#2063)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 15:51:47 -0600
    
    
    Bumps [certifi](https://github.com/certifi/python-certifi) from 2021.10.8 to
    2022.12.7.
    - [Release notes](https://github.com/certifi/python-certifi/releases)
    -
    [Commits](https://github.com/certifi/python-certifi/compare/2021.10.08...2022.12.07)
    
    
    ---
    updated-dependencies:
    - dependency-name: certifi
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.google.guava:guava in /data-prepper-plugins/aggregate-processor (#2219)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 15:50:29 -0600
    
    
    Bumps [com.google.guava:guava](https://github.com/google/guava) from 10.0.1 to
    23.0.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/compare/v10.0.1...v23.0)
    
    ---
    updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#2226)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 15:42:11 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.springframework:spring-test in /data-prepper-expression (#2224)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 8 Feb 2023 14:53:37 -0600
    
    
    Bumps
    [org.springframework:spring-test](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Moved Random Cut Forest libraries to the Anomaly Detection processor. (#2252)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 8 Feb 2023 11:44:14 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Sink doInitialize() should be able throw exception to stop execution (#2249)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 8 Feb 2023 09:53:30 -0600
    
    
    * Sink doInitialize() should be able throw exception to stop execution
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Sink doInitialize() should be able throw exception to stop execution - fixed
    code coverage failures
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Consistent AWS Pipeline Configurations - #2184 (#2248)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 7 Feb 2023 15:58:59 -0600
    
    
    * Consistent AWS Pipeline Configurations - #2184
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Consistent AWS Pipeline Configurations - updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Consistent AWS Pipeline Configurations  - fixed documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Made some changes to the :release:archives:linux:linuxTar task to reduce dependencies and get a better build time when building multiple times. (#2240)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 7 Feb 2023 14:30:30 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added SDK client metrics and metric filters (#2232)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 7 Feb 2023 14:04:10 -0600
    
    
    * Added SDK client metrics and metric filters
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Improved how the PipelineConnector copies events by using a new static method to perform the copy. For JacksonEvent, this performs a deepCopy() which appears to be more efficient than the old process. (#2241)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 7 Feb 2023 10:06:18 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use snake case for all configurations #2203 (#2243)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 6 Feb 2023 17:30:14 -0600
    
    
    * Use snake case for all configurations #2203
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add back LinkedBlockingQueue::poll to avoid busy wait (#2246)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 6 Feb 2023 14:12:32 -0600
    
    
    * Add back LinkedBlockingQueue::poll to avoid busy wait
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Drop poll timeout when delay=0 to 5 millis
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Use Armeria&#39;s BlockingTaskExecutor to configure thread names (#2235)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Feb 2023 09:32:47 -0600
    
    
    Use Armeria&#39;s BlockingTaskExecutor to configure thread name
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update default values in buffer documentation (#2233)__

    [JannikBrand](mailto:jannik.brand@sap.com) - Sat, 4 Feb 2023 14:04:56 -0600
    
    
    

* __The DefaultEventMetadata almost always creates an empty attributes. This uses the static ImmutableMap.of() which uses a shared instance underneath so that new objects are not created each time. (#2239)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 3 Feb 2023 13:21:31 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __The RecordMetadata default metadata is used by most records and it creates a new instance each time. This is an immutable class, so just return the same metadata instance for all. (#2238)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 3 Feb 2023 12:47:25 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Abstract sink should not create a new retry thread everytime initialization fails (#2231)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 2 Feb 2023 10:25:41 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add Support for retry when Sink fails to initialize  (#2198)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 1 Feb 2023 16:39:50 -0600
    
    
    * Remove opensearch availability dependence - Issue #936
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - issue #936
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - added number of retries
    to dataprepper execute
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Addressed review
    comments. Fixed test failures
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Addressed review
    comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Fixes for failing tests
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Fixes for failing tests
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Fixes for code coverage
    failures
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - Modified to check for
    retryable exception
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add support for retry when Sink fails to initialize - addressed review
    comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __DynamicIndexTemplate can cause NPE that shuts down pipeline - Issue #2210 (#2211)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 1 Feb 2023 15:15:01 -0600
    
    
    * DynamicIndexTemplate can cause NPE that shuts down pipeline - Issue #2210
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * DynamicIndexTemplate can cause NPE that shuts down pipeline - addressed
    review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * DynamicIndexTemplate can cause NPE that shuts down pipeline - addressed
    review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * DynamicIndexTemplate can cause NPE that shuts down pipeline - fixed build
    failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add missing metrics for Opensearch Sink #2168 (#2205)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 1 Feb 2023 15:10:21 -0600
    
    
    * Add missing metrics for Opensearch Sink #2168
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- updated the documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- modified documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add missing metrics for Opensearch Sink -- fixed build failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Perform serialization sequentially, block for each batch of forwards (#2228)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 1 Feb 2023 11:04:43 -0600
    
    
    * Perform serialization sequentially, block for each batch of forwards
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add test for mixed future results
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Rename forwardRecords to forwardBatchedRecords to avoid naming duplication
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add missing log statement
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Removed getObjectAttributes API call to check object size (#2179)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 1 Feb 2023 10:16:49 -0600
    
    
    * Removed getObjectAttributes API call to check object size
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump org.springframework:spring-core in /data-prepper-expression (#2222)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 21:00:33 -0600
    
    
    Bumps
    [org.springframework:spring-core](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#2225)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:46:44 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/http-source (#2227)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:46:18 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-trace-source (#2212)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:45:50 -0600
    
    
    Bumps org.assertj:assertj-core from 3.23.1 to 3.24.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.springframework:spring-test in /data-prepper-core (#2215)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:41:19 -0600
    
    
    Bumps
    [org.springframework:spring-test](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.springframework:spring-core in /data-prepper-core (#2216)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 31 Jan 2023 20:40:24 -0600
    
    
    Bumps
    [org.springframework:spring-core](https://github.com/spring-projects/spring-framework)
    from 5.3.23 to 5.3.25.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.23...v5.3.25)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Implement batching for peer forwarder request documents (#2197)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Tue, 31 Jan 2023 16:42:25 -0600
    
    
    * Implement batching for peer forwarder request documents
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add configurable forwarding_batch_timeout for low-traffic scenarios
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Slight refactors and add unit tests for batching
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Increase YAML deserialization size
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Refactor for clarity and flush all available batches on each iteration
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix typo in FORWARDING
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Use getOrDefault when checking last flushed time
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Integration test for a pipeline with multiple process workers (#2017)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 31 Jan 2023 14:29:38 -0600
    
    
    Created an integration test for a pipeline with multiple process workers.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add clientTimeout to peer forwarder configuration, optimize CPF seria… (#2190)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Tue, 31 Jan 2023 11:53:15 -0600
    
    
    * Add clientTimeout to peer forwarder configuration, optimize CPF serialization
    
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix miss in rename and use lower write timeout rather than higher request
    timeout
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add missing private
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Bump hibernate-validator in /data-prepper-core (#1854)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Jan 2023 10:29:58 -0600
    
    
    Bumps [hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 7.0.5.Final to 8.0.0.Final.
    - [Release notes](https://github.com/hibernate/hibernate-validator/releases)
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/main/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/7.0.5.Final...8.0.0.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jackson-datatype-jdk8 from 2.13.3 to 2.14.1 in /data-prepper-api (#2101)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Jan 2023 10:25:40 -0600
    
    
    Bumps jackson-datatype-jdk8 from 2.13.3 to 2.14.1.
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.datatype:jackson-datatype-jdk8
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Support OpenTelementry Logs (#1372)__

    [kmssap](mailto:100778246+kmssap@users.noreply.github.com) - Sat, 28 Jan 2023 09:21:54 -0600
    
    
    * Introduce Support for OpenTelemetry Logs
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix Unused Import Checkstyle Error
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Refactor commit for new architecture
    
    - Move mapping from processor to source
    - Move mapping logic to OtelProtoCodec
    - Enhance metrics of LogsGrpcService
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Remove Otel Logs Processor
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Fix JavaDoc Version
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Improve documentation of metrics
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    * Remove Otel logs processor from settings.gradle
     Signed-off-by: Kai Sternad &lt;ksternad@sternad.de&gt;
    
    ---------
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Signed-off-by: Kai Sternad
    &lt;ksternad@sternad.de&gt;
    Co-authored-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    
    Co-authored-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten
    Schnitter &lt;k.schnitter@sap.com&gt;
    Co-authored-by: Kai Sternad
    &lt;ksternad@sternad.de&gt;

* __Optimize buffer reads, allow delay to be 0 (#2189)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 27 Jan 2023 17:11:49 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __ENH: implement custom LogEventPatternConverter for desensitization on Data Prepper logging (#2188)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 27 Jan 2023 16:27:09 -0600
    
    
    * ADD: SensitiveArgumentMaskingConverter
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Generated THIRD-PARTY file for bd60dcc (#2201)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 27 Jan 2023 14:30:58 -0600
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: asifsmohammed
    &lt;asifsmohammed@users.noreply.github.com&gt;

* __Updated OpenSearch version to 1.3.7 (#2191)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 27 Jan 2023 09:24:13 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Signed-off-by: Asif
    Sohail Mohammed &lt;mdasifsohail7@gmail.com&gt;

* __Fix Data Prepper to not terminate on invalid open telemetry metric/trace data (#2176)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 26 Jan 2023 19:03:00 -0600
    
    
    * Rebased to latest and removed unnecessary System.out.println
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified the test to send one valid record and one invalid record, as per the
    review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add configurations and metrics to OTelTraceRawProcessor (#2164)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 26 Jan 2023 19:01:47 -0600
    
    
    Updated the OTelTraceRawProcessor to use the new plugin configuration model.
    Added two plugin metrics to tracking the usage of two of the collections used
    in this processor.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Don&#39;t block on forwarding request response, populate records that failed to forward in local CPF buffer (#2175)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 26 Jan 2023 17:26:14 -0600
    
    
    * Handle peer forwarding in the background
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Rework tests to pass with new implementation
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Small tweaks to fix the diff
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Refactor to use Consumer&lt;AggregateHttpResponse&gt; and ExecutorService for
    background forwarding work
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add unit tests for processFailedRequestsLocally
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add more unit tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Move requestsReceivedFromPeers to HttpService to avoid gauge usage
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Coming full circle + an ExecutorService
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add missing private modifier
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Reuse existing clientThreadCount parameter, cleanup unused code
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix log message
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Add support to convert metrics to json strings without flattening attributes - issue #2146 (#2163)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 26 Jan 2023 15:45:02 -0600
    
    
    Add support to convert metrics to json strings without flattening attributes
    field - issue #2146
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __ENH: add and populate markers (#2180)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 26 Jan 2023 11:31:25 -0600
    
    
    * ENH: add and populate markers
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Fixes links to use org/opensearch from com/amazon now that this is the correct the package name. (#2186)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 25 Jan 2023 08:29:54 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added AWS STS header override configurations for OpenSearch sink/S3 source (#1898)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 24 Jan 2023 17:17:13 -0600
    
    
    Added AWS STS header override configurations for the OpenSearch sink and the
    Amazon S3 source. Resolves #1888.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __ENH: add buffer records overflow metrics (#2170)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 24 Jan 2023 09:51:27 -0600
    
    
    * ENH: add buffer records overflow metrics
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Bump decode-uri-component in /release/staging-resources-cdk (#2064)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 23 Jan 2023 19:49:14 -0600
    
    
    Bumps
    [decode-uri-component](https://github.com/SamVerschueren/decode-uri-component)
    from 0.2.0 to 0.2.2.
    - [Release
    notes](https://github.com/SamVerschueren/decode-uri-component/releases)
    -
    [Commits](https://github.com/SamVerschueren/decode-uri-component/compare/v0.2.0...v0.2.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: decode-uri-component
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __New aggregate action - percent sampler (#2096)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 23 Jan 2023 17:36:31 -0600
    
    
    Percent Sampler aggregate action
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Implemented a heap-based circuit breaker (#2155)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Jan 2023 17:11:16 -0600
    
    
    Implemented a heap-based circuit breaker. This circuit breaker will prevent
    entry buffers from accepting events after the heap usage reaches a specified
    value. This checks for heap usage in a background thread and updates the state,
    which the buffer will then use to determine if the circuit breaker is open or
    closed. This also signals to the JVM to start a GC when the threshold is
    reached. Resolves #2150.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Test against multiple OTel version - Issue #1963 (#2154)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 23 Jan 2023 15:08:18 -0600
    
    
    * Test against multiple OTel version - Issue #1963
    Signed-off-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna Kondaka
    &lt;krishkdk@amazon.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#2161)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Sat, 21 Jan 2023 14:48:03 -0600
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.18 to
    1.12.22.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.18...byte-buddy-1.12.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#2160)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 20 Jan 2023 11:11:23 -0600
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.20 to
    1.12.22.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.20...byte-buddy-1.12.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __New Aggregate Action - Event Rate Limiter (#2090)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 19 Jan 2023 17:28:12 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#2100)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 19 Jan 2023 16:22:01 -0600
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.18 to
    1.12.20.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.18...byte-buddy-1.12.20)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added s3 support in Opensearch sink  (#2121)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 19 Jan 2023 16:21:32 -0600
    
    
    * Added implementation of s3 support in Opensearch sink
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added batch delay to CPF configuration (#2159)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 19 Jan 2023 13:05:26 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix for null pointer exception in remote peer forwarding (fix for issue 2123) (#2124)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 18 Jan 2023 20:56:57 -0600
    
    
    * Fix for null pointer exception in remote peer forwarding (fix for issue
    #2123)
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments to add a counter and not skip when an
    identification key is missing
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Modified to increment the counter only when all
    identification keys are missing
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added &#39;final&#39; to the local variable
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added a test with all missing keys
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Adds ScheduledExecutorService for Polling the RSS feed (#2140)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Wed, 18 Jan 2023 15:10:08 -0600
    
    
    * Adds ScheduledExecutor Service and runnable task
    Signed-off-by: Shivani
    Shukla &lt;sshkamz@amazon.com&gt;
    
    

* __Combined two integration tests for conditional routes into one. Also fixed a bug in the tests where the data was not sent to the correct sink. (#2061)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 17 Jan 2023 21:04:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add e2etest for testing log metrics (#2127)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 13 Jan 2023 15:19:16 -0600
    
    
    * Add e2etest for testing log metrics
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments.
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Adds count metrics to the service_map_stateful processor (#2130)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 12 Jan 2023 15:17:03 -0600
    
    
    Adds new metrics to the service_map_stateful processor. These count the number
    of items in the collections used by the service map. These do not have byte
    sizes, but use object counts.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump guava from 10.0.1 to 23.0 in /data-prepper-plugins/opensearch (#2050)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 11 Jan 2023 12:07:17 -0600
    
    
    Bumps [guava](https://github.com/google/guava) from 10.0.1 to 23.0.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/compare/v10.0.1...v23.0)
    
    ---
    updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump json5 from 2.2.0 to 2.2.3 in /release/staging-resources-cdk (#2119)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 11 Jan 2023 12:06:05 -0600
    
    
    Bumps [json5](https://github.com/json5/json5) from 2.2.0 to 2.2.3.
    - [Release notes](https://github.com/json5/json5/releases)
    - [Changelog](https://github.com/json5/json5/blob/main/CHANGELOG.md)
    - [Commits](https://github.com/json5/json5/compare/v2.2.0...v2.2.3)
    
    ---
    updated-dependencies:
    - dependency-name: json5
     dependency-type: indirect
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated MAINTAINERS.md to match recommended opensearch-project format. (#2117)__

    [Daniel (dB.) Doubrovkine](mailto:dblock@dblock.org) - Thu, 5 Jan 2023 14:58:26 -0600
    
    
    Signed-off-by: dblock &lt;dblock@amazon.com&gt;
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Update OTelProtoCodec for InstrumentationLibrary to InstrumentationScope rename (#2114)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 4 Jan 2023 09:40:06 -0600
    
    
    * Add support for ScopeSpans migration to otel-trace-raw-processor
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove unused import
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Run spotless
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Update slf4j-simple; update log4j-slf4j-impl to log4j-slf4j2-impl (#2113)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 3 Jan 2023 17:16:03 -0600
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __MOD: fix typos (#2084)__

    [Shanelle Marasigan](mailto:39988782+rmarasigan@users.noreply.github.com) - Tue, 3 Jan 2023 15:51:49 -0600
    
    
    Signed-off-by: Russianhielle Marasigan &lt;russianhielle@gmail.com&gt;

* __Created a GitHub Action to generate the Third Party report. (#2033)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 22 Dec 2022 15:01:37 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __updating S3 source documentation to include all codecs (#2091)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 22 Dec 2022 16:04:08 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Provide a type conversion / cast processor #2010 (#2020)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 22 Dec 2022 15:50:01 -0600
    
    
    * Provide a type conversion / cast processor #2010
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Histogram Aggregate Action - Added duration and fixed end time in the aggregated output (#2085)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 22 Dec 2022 12:20:08 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix CVE-2022-41881, CVE-2021-21290 and CVE-2022-41915 (#2093)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 21 Dec 2022 16:36:04 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix: CVE-2022-3509, CVE-2022-3510 (#2079)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 19 Dec 2022 10:14:22 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add robust retry strategy to AcmClients (#2082)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 16 Dec 2022 19:29:07 -0600
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Updated info logs to debug level (#2083)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Dec 2022 16:35:28 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix: CVE-2022-36944 (#2080)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Dec 2022 15:59:07 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Histogram aggregate action (#2078)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 16 Dec 2022 11:22:23 -0800
    
    
    * Add Histogram aggregation action
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Updated jackson bom dependency (#2068)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 15 Dec 2022 14:44:59 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Anomaly detector (#2058)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 15 Dec 2022 16:28:28 -0600
    
    
    * Add support for anomaly detection in the pipeline with new anomaly detector
    processor
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add retry strategy to StsClient used for sigv4 auth against OpenSearch sinks (#2069)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 15 Dec 2022 11:26:50 -0600
    
    
    Add retry strategy to STS client used for sigv4 auth
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Parse RSS feed URL items and convert Item to Event (#2073)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Wed, 14 Dec 2022 13:16:46 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    
    * Adds Jackson Jdk8Module to enable usage of Optional in Rss Item model
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    

* __Fix: Updated parse json processor documentation (#2071)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 14 Dec 2022 08:47:12 -0800
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Count Aggregate Action - fix aggregation temporality (#2067)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 8 Dec 2022 17:21:41 -0600
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add support for count aggregate action (#2034)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 7 Dec 2022 12:51:11 -0600
    
    
    Add support for count aggregate action with raw and otel_metrics output format
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Setup boilerplate for RSS Source Plugin (#2062)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Wed, 7 Dec 2022 12:44:37 -0600
    
    
    * Adds boilerplate config and code for rss source
    Signed-off-by: Shivani
    Shukla &lt;sshkamz@amazon.com&gt;
    
    * Adds RSS Source plugin class
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;
    
    * Adds Document Event Type
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    
    * Adds Document interface and JacksonDocument class
    Signed-off-by: Shivani
    Shukla &lt;sshkamz@amazon.com&gt;
    
    * Adds a simple unit test checking default for pollingFrequency
    Signed-off-by:
    Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Bump slf4j-simple in /data-prepper-logstash-configuration (#2053)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 2 Dec 2022 14:04:27 -0600
    
    
    Bumps [slf4j-simple](https://github.com/qos-ch/slf4j) from 1.7.36 to 2.0.5.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.36...v_2.0.5)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-simple
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump slf4j-api from 1.7.36 to 2.0.5 (#2057)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 2 Dec 2022 14:00:34 -0600
    
    
    Bumps [slf4j-api](https://github.com/qos-ch/slf4j) from 1.7.36 to 2.0.5.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.36...v_2.0.5)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-api
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jsonassert from 1.5.0 to 1.5.1 in /data-prepper-api (#2043)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 1 Dec 2022 17:51:03 -0600
    
    
    Bumps [jsonassert](https://github.com/skyscreamer/JSONassert) from 1.5.0 to
    1.5.1.
    - [Release notes](https://github.com/skyscreamer/JSONassert/releases)
    -
    [Changelog](https://github.com/skyscreamer/JSONassert/blob/master/CHANGELOG.md)
    
    -
    [Commits](https://github.com/skyscreamer/JSONassert/compare/jsonassert-1.5.0...jsonassert-1.5.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.skyscreamer:jsonassert
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-raw-prepper (#2042)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 1 Dec 2022 17:50:08 -0600
    
    
    Bumps protobuf-java-util from 3.21.9 to 3.21.10.
    
    ---
    updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Router integration tests to verify that conditional pipeline routes work as expected. (#1988)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 1 Dec 2022 15:51:52 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the OpenSearch libraries to 1.3.6 and run the integration tests against 1.3.6 instead of 1.3.5. Fixes #2022 (#2030)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 30 Nov 2022 12:57:39 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removes double brace initialization and includes this as a checkstyle rule to prevent future use. (#2035)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Nov 2022 16:16:14 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Implemented additional metrics for the S3 source.  (#2028)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 22 Nov 2022 14:25:20 -0600
    
    
    Implemented additional metrics for the S3 source. Resolves #2024
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use the same compression engines for GZIP and NONE in the AUTOMATIC compression engine. Fixes #2026. (#2027)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 21 Nov 2022 11:15:05 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Log the number of messages received from the SQS queue, including a count of the number of messages that will need to be processed. Also, include logging of deletes at the debug level. (#2011)__

    [David Venable](mailto:dlv@amazon.com) - Sat, 19 Nov 2022 13:06:23 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add PluginMetrics in the Auth Plugin for Http, OTel and Metrics Source (#2023)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 18 Nov 2022 21:27:15 -0600
    
    
    * Add PluginMetrics in the Auth Plugin for Http, OTel and Metrics Source
     Issue: https://github.com/opensearch-project/data-prepper/issues/2007
    
    Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    
    * Added unit test for verifying PipelineDescription in DefaultPluginFactory
    class and fixed review comments on unit test
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Unit test update
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Unit test update
     Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
     Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    Signed-off-by: Dinu John
    &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add when condition to aggregate processor (#2018)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 18 Nov 2022 15:31:54 -0600
    
    
    * Add when condition to aggregate processor
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add when condition to aggregate processor - addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add when condition to aggregate processor - Fixed check style test errors
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Dynamic Index Name in OpenSearch sink  - Resolves #1459 (#1999)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 17 Nov 2022 20:36:47 -0600
    
    
    * Dynamic Index Name in OpenSearch sink #1459
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink #1459 -- updated documentation
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink #1459 -- fixed a bug and increased test
    coverage
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments to add more tests and re-design index manager to
    accommodate dynamic indexes
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added tests for DynamicIndexManager
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink #1459 -- changed cache weigher to have
    constant value
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink #1459 -- addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink - Resolves Issue #1459 -- addressed
    review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink - Fixed checkSytleMain issues in
    opensearch
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Dynamic Index Name in OpenSearch sink - Fixed spotlessJavaCheck issues in
    opensearch
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __updated samples for dataperpper 2 (#2019)__

    [Arunachalam Lakshmanan](mailto:arnlaksh@amazon.com) - Thu, 17 Nov 2022 10:02:31 -0600
    
    
    * updated samples for dataperpper 2
     Signed-off-by: Arun Lakshmanan &lt;arnlaksh@amazon.com&gt;
    
    * updated samples for dataperpper 2
     Signed-off-by: Arun Lakshmanan &lt;arnlaksh@amazon.com&gt;
     Signed-off-by: Arun Lakshmanan &lt;arnlaksh@amazon.com&gt;

* __Add IntelliJ&#39;s .iml file extension to the .gitignore. (#2016)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 16 Nov 2022 14:15:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-raw-prepper (#2001)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 11 Nov 2022 09:58:40 -0600
    
    
    Bumps protobuf-java-util from 3.19.4 to 3.21.9.
    
    ---
    updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Data Prepper Core integration tests (#1949)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 10 Nov 2022 09:50:03 -0600
    
    
    Creates an integration test source set for data-prepper-core and adds a small
    framework for running integration tests on data-prepper-core functionality.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding Chase Engelbrecht to the MAINTAINERS.md. (#2005)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 9 Nov 2022 09:55:59 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding Hai Yan to the maintainers. (#2004)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 8 Nov 2022 16:13:31 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump opensearch-java from 1.0.0 to 2.1.0 (#1733)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 8 Nov 2022 08:17:20 -0600
    
    
    Bumps [opensearch-java](https://github.com/opensearch-project/opensearch-java)
    from 1.0.0 to 2.1.0.
    - [Release
    notes](https://github.com/opensearch-project/opensearch-java/releases)
    -
    [Commits](https://github.com/opensearch-project/opensearch-java/compare/v1.0.0...v2.1.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.opensearch.client:opensearch-java
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-trace-raw-prepper (#2003)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 8 Nov 2022 08:15:44 -0600
    
    
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
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump awaitility in /data-prepper-plugins/otel-trace-raw-prepper (#2002)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 8 Nov 2022 08:15:12 -0600
    
    
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
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Support remaining OpenTelemetry Metrics proto spec features (#1335)__

    [kmssap](mailto:ksternad@sternad.de) - Fri, 4 Nov 2022 10:58:14 -0500
    
    
    * Bump OTEL proto version
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Support OTEL ScopeMetrics
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add support for OTEL schemaUrl
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add exemplars to metrics plugin
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add metrics flags
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add support for Exponential Histogram
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add config switch for histogram bucket calculation
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Refactor Otel Metrics Proto
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Change config property to snake_case
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix JavaDoc
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Remove Clock from tests
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Change config parameters
    
    - Introduce allowed max scale
    - Invert histogram calculation params
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Address review comments
    
    - Remove unused import, breaking Checkstyle
    - Change Exponential Histogram filter
    - Add lenient to some Mockito calls
    - Clarify metrics processor documentation
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix OtelMetricsRawProcessorConfigTest
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Change ExponentialHistogram Bucket Calculation
    
    - Precompute all possible bucket bounds
    - Consider negative offset
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix e2e otel dependency coordinates
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo
    &lt;tomas.longo@sap.com&gt;
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix dependency coordinate for otel
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
     Signed-off-by: Kai Sternad &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Kai Sternad
    &lt;kai.sternad@sap.com&gt;
    Co-authored-by: Tomas Longo &lt;tomas.longo@sap.com&gt;
    
    Co-authored-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1993)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 2 Nov 2022 20:22:12 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.17 to
    1.12.18.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.17...byte-buddy-1.12.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1992)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 2 Nov 2022 19:20:41 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.17 to
    1.12.18.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.17...byte-buddy-1.12.18)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Fixes for issues #1456 and #1458 - support for complex document ID and routing ID (#1966)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 2 Nov 2022 18:57:46 -0500
    
    
    * Fixes for issues #1456 and #1458 - support for complex document ID and
    routing ID
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixes for issues #1456 and #1458 - updated README.md and added unit tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixes for issues #1456 and #1458 - addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixes for issues #1456 and #1458 - fixed check style build failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Add OpenSearch e2e test to github workflows
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Deleted e2e test for OpenSearch
    DocumentId/RoutingField testing
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Remove e2e test entry for removed e2e test
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments and added tests for fromStringAndOptionals method
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkstyle failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Update a few logs that were dumping the actual customer logs and traces into the DataPrepper logs on failure. Logging the request sizes instead, to avoid customer data being logged (#1989)__

    [Deep Datta](mailto:18663532+deepdatta@users.noreply.github.com) - Mon, 31 Oct 2022 14:30:12 -0500
    
    
    Signed-off-by: Deep Datta &lt;deedatta@amazon.com&gt;

* __Updated release notes and change log with spring change (#1981)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 27 Oct 2022 17:02:02 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Created release notes for Data Prepper 2.0.1. (#1969)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 27 Oct 2022 13:28:47 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added release notes for 1.5.2 (#1976)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 27 Oct 2022 12:44:57 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added change log for 1.5.2 (#1977)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 27 Oct 2022 12:28:10 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added the change log for 2.0.1. (#1968)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 27 Oct 2022 10:38:17 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Expressions with null (#1946)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 25 Oct 2022 15:00:55 -0500
    
    
    * fix for issue #1136 Add null support to DataPrepperExpressions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * fix for issue #1136 Add null support to DataPrepperExpressions - added more
    tests
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * fix for issue #1136 Add null support to DataPrepperExpressions - updated docs
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments - added test cases and updated document
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Reduce smoke test timeout to 8 minutes from 30 minutes. These tests tend to pass within 3 minutes in my personal GitHub branch. So this leaves quite a bit of buffer time. It helps speed up retrying failures from flaky tests. (#1956)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Oct 2022 09:48:44 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Attempt to reduce flakiness in RandomStringSourceTests by using awaitility. Split tests into two. JUnit 5. (#1921)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Oct 2022 09:48:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Run smoke tests against OpenSearch 1.3.6. (#1955)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Oct 2022 06:47:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Python grpcio 1.50.0 in smoke tests to reduce time to run. (#1954)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 21 Oct 2022 06:46:43 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Delete s3:TestEvent objects and log them when they are found in the SQS queue. Resolves #1924. (#1939)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 20 Oct 2022 12:56:56 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add ExecutorService to DataPrepperServer (#1948)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 20 Oct 2022 11:18:27 -0500
    
    
    * Add ExecutorService to DataPrepperServer
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Shutdown executor service after stopping server
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Updated k8s manifest to suit Data Prepper 2.0 (#1928)__

    [Rafael Gumiero](mailto:rafael.gumiero@gmail.com) - Wed, 19 Oct 2022 17:04:42 -0500
    
    
    * Updated new paths for pepelines/config and new processor name
     Signed-off-by: Rafael Gumiero &lt;rafael.gumiero@gmail.com&gt;
    
    * Updated image version
     Signed-off-by: Rafael Gumiero &lt;rafael.gumiero@gmail.com&gt;
    
    * Moved Peer Forwarder to config file
     Signed-off-by: Rafael Gumiero &lt;rafael.gumiero@gmail.com&gt;
     Signed-off-by: Rafael Gumiero &lt;rafael.gumiero@gmail.com&gt;

* __Require protobuf-java-util 3.21.7 to fix #1891 (#1938)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 19 Oct 2022 12:35:33 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bug Fix: S3 source key  (#1926)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 19 Oct 2022 11:39:56 -0500
    
    
    * Fix: S3 source key bug fix
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Jackson 2.13.4.2 (#1925)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 18 Oct 2022 16:52:21 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Refactors the Data Prepper CLI argument parsing into data-prepper-main. Added an interface for the parts of DataPrepperArgs that client classes really need. (#1920)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 14 Oct 2022 10:03:06 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix PipelineConnector to duplicate the events (#1897)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 12 Oct 2022 16:32:00 -0500
    
    
    * Fix string mutate processors to duplicate the events
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix string mutate processors to duplicate the events - made changes as per
    David&#39;s suggestions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed unnecessary changes leftover from 1st commit
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified PipelineConnector to duplicate JacksonSpan type events too. Added
    testcases in PipelineConnectorTest
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comment and added a new testcase for JacksonSpan withData()
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comment and added parallel pipeline test to github/workflows
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * fixed workflow failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Simple duration regex did not allow for 0s or 0ms (#1910)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 11 Oct 2022 10:58:40 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updated the release notes for 2.0.0 (#1911)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 10 Oct 2022 17:46:26 -0500
    
    
    Updated the release notes for 2.0.0
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the change log for 2.0.0 with most recent changes. (#1909)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 10 Oct 2022 17:33:31 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update dev version to 2.1.0-SNAPSHOT (#1904)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 10 Oct 2022 13:34:46 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Increase the default buffer configurations by 25. Capacity to 12,800 and batch size to 200. (#1906)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 10 Oct 2022 11:59:38 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Conditional routing documentation (#1894)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 10 Oct 2022 10:35:15 -0500
    
    
    Add documentation for conditional routing. Resolves #1890
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added change log (#1901)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 7 Oct 2022 20:52:38 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Adds a stack-trace to failures from OpenSearch to help with debugging issues. (#1899)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 7 Oct 2022 18:32:26 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


