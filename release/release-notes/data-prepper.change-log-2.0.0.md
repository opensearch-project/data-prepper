
* __Increase the default buffer configurations by 25. Capacity to 12,800 and batch size to 200. (#1906) (#1907)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 10 Oct 2022 12:10:16 -0500
    
    EAD -&gt; refs/heads/2.0, refs/remotes/upstream/2.0, refs/remotes/origin/2.0
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 29a4a9750c5448cc7ecf27246f6565fb483fa724)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Conditional routing documentation (#1894) (#1905)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 10 Oct 2022 10:40:13 -0500
    
    
    Add documentation for conditional routing. Resolves #1890
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 11dd3a01b6e4f7f86e348a8dc5f9a205074e0f22)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds a stack-trace to failures from OpenSearch to help with debugging issues. (#1899) (#1902)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 10 Oct 2022 09:27:52 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit bd8a7fa6950f954737c2a9c77875eaaa7b735872)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Added change log (#1901) (#1903)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Sat, 8 Oct 2022 12:56:59 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 6f0b7cf04b4f7a13603d0fdad956848b6d90ca55)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update gradle version to 2.0.0 (#1900)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 7 Oct 2022 18:28:50 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updated protobuf dependency (#1896)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 7 Oct 2022 11:45:19 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updates Trace Analytics documentation for Data Prepper 2.0 (#1748)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 7 Oct 2022 11:37:52 -0500
    
    
    * Updates Trace Analytics examples to set the record_type to event in our
    documentation. Also includes a short guide to migrating to Data Prepper 2.0 for
    Trace Analytics.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Updated the documentation for the OTel Trace Source to show how to run on
    either Data Prepper 2.0 or 1.x.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Data Prepper 2.0.0 Release notes  (#1895)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 7 Oct 2022 11:25:06 -0500
    
    
    * Updated release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated release notes
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Addressed feedback
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __MAINT: rename grok match metrics (#1893)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 6 Oct 2022 17:11:30 -0500
    
    
    Signed-off-by: Qi Chen &lt;qchea@amazon.com&gt;
     Signed-off-by: Qi Chen &lt;qchea@amazon.com&gt;

* __Added core peer forwarder e2e tests (#1866)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 6 Oct 2022 11:22:53 -0500
    
    
    * Added e2e tests for core peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Using the file sink is causing build failures in valid_multiple_sinks_with_routes.yml. Use stdout instead. (#1889)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 6 Oct 2022 10:42:51 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __adding bufferUsage to BlockingBuffer plugin (#1882)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 6 Oct 2022 10:05:50 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Implementation of conditional routing of sinks (#1832)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Oct 2022 18:25:16 -0500
    
    
    Implementation of conditional routing of sinks.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Core peer forwarder performance inprovement (#1880)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 5 Oct 2022 11:39:33 -0500
    
    
    * Updated call to setPeerClientPool
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fixes file sink to work with multiple threads (#1842)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Oct 2022 11:04:23 -0500
    
    
    * This fixes an issue with file sink. Each processor thread would overwrite the
    other threads which prevents it from working as expected. Now, it keeps a
    Writer open as long as it is running. It will flush on each output() call.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Updated test files to use stdout instead of file sink since the file sink now
    will attempt to write to the file on construction.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Additional test to improve code coverage.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the bin/data-prepper script to use &#34;readlink -f&#34; instead of &#34;realpath&#34; since this is available by default on more systems. (#1883)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Oct 2022 11:02:07 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Apache AWS client exclusively to fix AWS Java SDK (#1877)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Oct 2022 10:34:54 -0500
    
    
    Fixes Data Prepper with AWS services by setting the Java system property.
    Resolves #1876
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated http source documentation (#1875)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 5 Oct 2022 10:32:36 -0500
    
    
    * Updated http source documentation
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump com.diffplug.spotless from 6.7.1 to 6.11.0 (#1884)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Oct 2022 10:29:01 -0500
    
    
    Bumps com.diffplug.spotless from 6.7.1 to 6.11.0.
    
    ---
    updated-dependencies:
    - dependency-name: com.diffplug.spotless
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.22 to 5.3.23 in /data-prepper-core (#1885)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Oct 2022 10:22:07 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Removed peer forwarder processor plugin (#1874)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 5 Oct 2022 10:02:15 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1851)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Oct 2022 09:50:47 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.16 to
    1.12.17.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.16...byte-buddy-1.12.17)
    
    
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

* __Bump micrometer-bom from 1.9.3 to 1.9.4 (#1858)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Oct 2022 09:20:31 -0500
    
    
    Bumps [micrometer-bom](https://github.com/micrometer-metrics/micrometer) from
    1.9.3 to 1.9.4.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.9.3...v1.9.4)
    
    
    ---
    updated-dependencies:
    - dependency-name: io.micrometer:micrometer-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated ssl to true by default for core peer forwarder (#1835)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 4 Oct 2022 10:29:23 -0500
    
    
    * Updated ssl to true by default
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Do not fail all GitHub Actions when other similar ones fail to help reduce the number of builds we have to re-run from flaky tests. (#1879)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 4 Oct 2022 09:46:13 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Jackson 2.13.4. Resolves #1839 (#1871)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 4 Oct 2022 08:28:17 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the OTel Metrics plugins to the org.opensearch.dataprepper package. (#1872)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 4 Oct 2022 08:18:20 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added metrics documentation (#1873)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 4 Oct 2022 00:12:59 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1850)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:16:46 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.14 to
    1.12.17.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.14...byte-buddy-1.12.17)
    
    
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

* __Bump spring-core from 5.3.22 to 5.3.23 in /data-prepper-core (#1855)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:16:05 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.22 to 5.3.23 in /data-prepper-expression (#1862)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:13:44 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.22 to 5.3.23 in /data-prepper-expression (#1860)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:13:27 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.22 to 5.3.23 in /data-prepper-core (#1856)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:13:04 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.22 to 5.3.23 in /data-prepper-expression (#1861)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:11:15 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.22 to 5.3.23.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.22...v5.3.23)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump log4j-bom from 2.18.0 to 2.19.0 in /data-prepper-core (#1853)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:10:00 -0500
    
    
    Bumps log4j-bom from 2.18.0 to 2.19.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump log4j-bom from 2.18.0 to 2.19.0 in /data-prepper-expression (#1864)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 18:09:31 -0500
    
    
    Bumps log4j-bom from 2.18.0 to 2.19.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Use exec command to run application (#1847)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 3 Oct 2022 17:49:33 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __BUG FIX: Core peer forwarder trace pipeline bug (#1865)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 3 Oct 2022 16:49:42 -0500
    
    
    * Fix: Core peer forwarder trace pipeline bug
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump jackson-databind from 2.13.3 to 2.13.4 (#1857)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 3 Oct 2022 16:18:54 -0500
    
    
    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.13.3 to
    2.13.4.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates log4j configurations to use the org.opensearch.dataprepper package. Updates documentation to this package as well to help clarify expected outcomes for users. Contributes toward #344. (#1841)__

    [David Venable](mailto:dlv@amazon.com) - Sat, 1 Oct 2022 13:57:21 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add fingerprint trust store implementation to PeerForwarderHttpServer (#1848)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Fri, 30 Sep 2022 14:27:25 -0500
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Update plugin names to use processor (#1838)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 30 Sep 2022 10:30:24 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added additional metrics to peer forwarder (#1836)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 30 Sep 2022 09:22:54 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Support evaluating conditional expressions in multiple threads by using a ThreadLocal ParseTreeParser. Resolves #1189 (#1840)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 18:39:17 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Armeria to 1.19.0 from 1.16.0. (#1837)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 14:43:51 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Decompose archive smoke-tests (#1808)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 13:18:56 -0500
    
    
    * Decompose the archive smoke tests by having the different combinations
    available as command-line arguments. This allows us to split up the release
    GitHub Action to test each archive file independently of each other.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Updated the release smoke test strategy to use specific inclusions.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Update release/smoke-tests/README.md
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Co-authored-by: Hai Yan &lt;8153134+oeyh@users.noreply.github.com&gt;
    
    * Other README corrections.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    Co-authored-by: Hai Yan
    &lt;8153134+oeyh@users.noreply.github.com&gt;

* __Replace example pipeline yaml with a README (#1830)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 29 Sep 2022 12:21:07 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updates otel-proto-common to org.opensearch.dataprepper. (#1834)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 12:07:18 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates data-prepper-api to the org.opensearch.dataprepper package. (#1833)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 29 Sep 2022 09:01:34 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added metrics for peer forwarder (#1812)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 23:15:56 -0500
    
    
    * Added existing metrics to peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactor Processor shutdown behavior to wait until buffers are empty before preparing for shutdown (#1792)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 28 Sep 2022 22:49:54 -0500
    
    
    * Add drainTimeout to PeerForwarderConfiguration, refactor ProcessWorker
    shutdown behavior
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Corrected the way that PipelineModel deserializes the route property. It was previously using a setter approach and used a different property name in the constructor. (#1825)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Sep 2022 18:36:24 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bug fix: peer forwarder not decorating processors (#1831)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 17:21:44 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated third party file (#1828)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 16:06:50 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update examples (#1796)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 28 Sep 2022 14:40:15 -0500
    
    
    * Update examples for directory change
    * Fix analytics sample app
    * Exclude javax.json dependency from antlr4
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updated the PipelineModel to return a SinkModel for sinks (#1802)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Sep 2022 11:31:24 -0500
    
    
    Updated the PipelineModel to return a SinkModel for sinks. This includes fields
    for conditional routing.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated default peer forwarder port (#1823)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 09:48:43 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Change CMD to exec form in Dockerfile (#1822)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 28 Sep 2022 09:38:53 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix: Peer forwarder exception with single thread annotated processors (#1821)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 28 Sep 2022 09:36:51 -0500
    
    
    * Fix: Peer forwarder exception with single thread annotated processors
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Addressed feedback
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated the package for most of the remaining plugins to org.opensearch.dataprepper. This excludes the metrics plugins, proto-commons, and the classic peer-forwarder plugin. Contributes toward #344. (#1815)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 27 Sep 2022 22:05:00 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add sslVerifyFingerprintOnly flag to PeerForwarderConfiguration (#1818)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Tue, 27 Sep 2022 22:04:39 -0500
    
    
    * Add sslVerifyFingerprintOnly flag to PeerForwarderConfiguration
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Move getFingerprint() functionality into Certificate model, add tests for
    getFingerprint()
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add PeerForwarderConfiguration tests for sslFingerprintVerificationOnly
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Wire isSslFingerprintVerificationOnly into PeerForwarderClientFactory, add
    tests for it
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add documentation for sslFingerprintVerificationOnly
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add PeerForwarder_ClientServer integration tests for
    sslFingerprintVerificationOnly
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix jdk version check in script (#1819)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 27 Sep 2022 21:18:09 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added demo cert and key files (#1813)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 27 Sep 2022 18:41:50 -0500
    
    
    * Added default cert and key files
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated file names
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added README.md
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated config files
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Make HTTP source request timeout transparent (#1814)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 27 Sep 2022 15:43:54 -0500
    
    
    * Clean up the usage of request_timeout; update tests and readme
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Address review comments
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Updated the package for trace analytics plugins to org.opensearch.dataprepper. (#1810)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 27 Sep 2022 09:45:48 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the package for data-prepper-test-common to org.opensearch.dataprepper. (#1809)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 27 Sep 2022 09:45:27 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Parse JSON Processor: README, more testing, support for JSON Pointer (#1696)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Mon, 26 Sep 2022 18:36:57 -0500
    
    
    Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __S3 Event Consistency via metadata and JSON changes (#1803)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 26 Sep 2022 17:30:06 -0500
    
    
    Support a configurable key for S3 metadata and make this base key s3/ by
    default. Moved the output of JSON from S3 objects into the root of the Event
    from the message key. Resolves #1687
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added documentation for core peer forwarder (#1797)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 26 Sep 2022 09:31:32 -0500
    
    
    * Added documentation for core peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Implement AggregateProcessor shutdown behavior (#1794)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 26 Sep 2022 08:39:16 -0500
    
    
    * Implement AggregateProcessor shutdown behavior
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Move branching on isShuttingDown to existing loop
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add missing imports
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Remove bound from nextLong
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Refactor test case to support either order of map iteration
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Add isShuttingDown flag to conclude group check in
    AggregateActionSynchronizer
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Rename isShuttingDown flag to forceConclude, add test for groups size == 1
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Bump protobuf in /examples/trace-analytics-sample-app/sample-app (#1801)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Sat, 24 Sep 2022 09:39:19 -0500
    
    
    Bumps [protobuf](https://github.com/protocolbuffers/protobuf) from 3.15.6 to
    3.18.3.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/main/generate_changelog.py)
    
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.15.6...v3.18.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: protobuf
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump protobuf in /release/smoke-tests/otel-span-exporter (#1800)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 23 Sep 2022 17:59:49 -0500
    
    
    Bumps [protobuf](https://github.com/protocolbuffers/protobuf) from 3.19.1 to
    3.19.5.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/main/generate_changelog.py)
    
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.19.1...v3.19.5)
    
    
    ---
    updated-dependencies:
    - dependency-name: protobuf
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updates to several plugins to move to the org.opensearch.dataprepper package (#1787)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 22 Sep 2022 14:52:32 -0700
    
    
    Updates many plugins to the org.opensearch package: csv, date, log-generator,
    mutate, aggregate, drop, grok, http, key-value. #344
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix ignored Logstash config file when loading from directory (#1788)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 22 Sep 2022 14:27:42 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix otel config (#1790)__

    [bryan-aguilar](mailto:46550959+bryan-aguilar@users.noreply.github.com) - Thu, 22 Sep 2022 10:35:57 -0700
    
    
    Signed-off-by: Bryan Aguilar &lt;bryaag@amazon.com&gt;

* __updated aws-cdk to version ^2.42.1 (#1793)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Thu, 22 Sep 2022 10:29:13 -0500
    
    
    Fix cve-2022-36067

* __Update docs to reflect recent changes on directory structure (#1786)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 20 Sep 2022 13:42:34 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Removed io.pebbletemplates dependency (#1784)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Mon, 19 Sep 2022 16:50:57 -0500
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Refactor GrokPrepper shutdown behavior to prevent ExecutorService from terminating prematurely (#1776)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 19 Sep 2022 15:22:10 -0500
    
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Adds the Data Prepper icon to the start of the README.md file. (#1783)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Sep 2022 15:17:41 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the OpenSearch sink to use the org.opensearch.dataprepper package name. Contributes to #344. (#1780)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Sep 2022 10:37:16 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds the Data Prepper horizontal icon with auto light/dark mode as an SVG. (#1782)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 19 Sep 2022 10:33:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added Java time module to object mapper (#1779)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sat, 17 Sep 2022 19:14:43 -0500
    
    
    * Added Java time module to object mapper
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added new object mapper for peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Added qualifier
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Support hostname verification in core peer-forwarding. Provide the ability to disable hostname verification. Generate test certificates with a SAN and document how to do it. (#1778)__

    [David Venable](mailto:dlv@amazon.com) - Sat, 17 Sep 2022 14:53:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Integration testing against latest OpenSearch versions (#1754)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Sep 2022 15:19:55 -0500
    
    
    Renamed the GitHub action for OpenSearch targets and support the latest
    versions in the 2.x series as of now. Test OpenSearch 1.3.5 as the latest
    version in that series.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support Mutual TLS authentication for Core Peer Forwarding. Resolves #1758. (#1771)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Sep 2022 15:19:03 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Create a runnable distribution from assemble task (#1774)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 16 Sep 2022 15:17:25 -0500
    
    
    * Configure install task and add to assemble task
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Load pipeline configurations from directory (#1760)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 16 Sep 2022 14:11:41 -0500
    
    
    * Load one or more pipeline configs from directory
    * Add back the support for command line arguments to specify configs
    * Update docker for smoke tests
    * Update relevant documentation
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix broken PeerForwardingProcessingDecoratorTest by casting object as Processor. (#1777)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Sep 2022 14:10:19 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added receive records to peer forwarder (#1761)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Sep 2022 10:58:22 -0500
    
    
    * Added receive records to peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Implement RequiresPeerForwarding interface (#1767)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 16 Sep 2022 10:14:24 -0500
    
    
    * Aggregate processor implements RequiresPeerForwarding interface
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactor PluginDurationDeserializer to DataPrepperDurationDeserializer, add processorShutdownTimeout to DataPrepperConfiguration, use processorShutdownTimeout from DataPrepperConfiguration in Pipeline (#1757)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 15 Sep 2022 14:06:10 -0500
    
    
    * Refactor PluginDurationDeserializer to DataPrepperDurationDeserializer, add
    processorShutdownTimeout to DataPrepperConfiguration, use
    processorShutdownTimeout from DataPrepperConfiguration in Pipeline
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Updated config defaults and validations for peer forwarder (#1768)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 15 Sep 2022 13:52:55 -0500
    
    
    * Updated config defaults and validations
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Inject PeerForwarderServer in to DataPrepper class (#1764)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 15 Sep 2022 10:25:24 -0500
    
    
    * Injected peer forwarder server in to DataPrepper
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Peer Forwarder client-server integration testing and related fixes (#1753)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 14 Sep 2022 09:08:00 -0500
    
    
    Created an integration test which verifies that a PeerForwarderClient can
    forward events to a PeerForwarderServer. This revealed several bugs which are
    fixed in this PR. Additionally, client exceptions are thrown as exceptions
    rather than incorrect status codes.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated Opensearch rest high-level client (#1755)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 13 Sep 2022 15:27:15 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Load data prepper configurations from home directory (#1737)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 13 Sep 2022 14:01:32 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added Local peer forwarder and updated http servoce class to write to… (#1750)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 13 Sep 2022 13:09:20 -0500
    
    
    * Added Local peer forwarder and updated http service class to write to buffer
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Allows the plugin framework to correctly load classes which inherit from PluginSetting. (#1752)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 12 Sep 2022 16:35:12 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Get PeerForwarder from a PeerForwarderProvider (#1717)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 9 Sep 2022 14:04:35 -0500
    
    
     Refactored the PeerForwarder class such that a PeerForwarderProvider is
    available as the primary means of obtaining a PeerForwarder object. This can
    yield a dynamic approach to PeerForwarder which can provide solutions for when
    peer forwarding is not needed or not configured. Includes making PeerForwarder
    an interface and supplying a package-protected RemotePeerForwarder for
    configurations requiring peer-forwarding.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added server proxy for Peer Forwarder Server (#1738)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 9 Sep 2022 13:22:22 -0500
    
    
    * Added server proxy
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump gradle-license-report from 1.17 to 2.1 (#1555)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Sep 2022 12:30:04 -0500
    
    
    Bumps gradle-license-report from 1.17 to 2.1.
    
    ---
    updated-dependencies:
    - dependency-name: com.github.jk1:gradle-license-report
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy from 1.12.14 to 1.12.16 (#1743)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 9 Sep 2022 12:27:39 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.14 to
    1.12.16.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.14...byte-buddy-1.12.16)
    
    
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

* __Update Data Prepper to build using JDK 11 making this the minimum sup… (#1739)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 9 Sep 2022 08:52:45 -0500
    
    
    Update Data Prepper to build using JDK 11 making this the minimum supported
    Java version. Resolves #1422. Includes updated documentation.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump guava from 31.0.1-jre to 31.1-jre (#1734)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 8 Sep 2022 20:43:31 -0500
    
    
    Bumps [guava](https://github.com/google/guava) from 31.0.1-jre to 31.1-jre.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Update trace analytics sample app pipeline.yaml to include event record type for otel_trace_source (#1741)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 8 Sep 2022 16:51:12 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Update HTTP source ssl validations (#1740)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 8 Sep 2022 16:46:37 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update Docker images to use JDK 17 (#1727)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 8 Sep 2022 12:45:02 -0500
    
    
    * Update Data Prepper docker to use Temurin jdk17-alpine
    * Update jdk in tar.gz archive file to use Temurin jdk-17
    * Update trace analytics dev sample app and EMF monitoring dev sample
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Renamed router to route in the PipelineModel (#1721)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 8 Sep 2022 10:43:10 -0500
    
    
    Renamed router to route in the PipelineModel. Improved the tests by including
    tests that validate the route properties.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1712)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Sep 2022 22:02:17 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.13 to
    1.12.14.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.13...byte-buddy-1.12.14)
    
    
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

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1711)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Sep 2022 20:15:26 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.12 to
    1.12.14.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.12...byte-buddy-1.12.14)
    
    
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

* __Bump hibernate-validator from 7.0.4.Final to 7.0.5.Final (#1715)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Sep 2022 20:14:52 -0500
    
    
    Bumps [hibernate-validator](https://github.com/hibernate/hibernate-validator)
    from 7.0.4.Final to 7.0.5.Final.
    - [Release notes](https://github.com/hibernate/hibernate-validator/releases)
    -
    [Changelog](https://github.com/hibernate/hibernate-validator/blob/7.0.5.Final/changelog.txt)
    
    -
    [Commits](https://github.com/hibernate/hibernate-validator/compare/7.0.4.Final...7.0.5.Final)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.hibernate.validator:hibernate-validator
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time from 2.10.14 to 2.11.1 (#1714)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Sep 2022 19:26:46 -0500
    
    
    Bumps [joda-time](https://github.com/JodaOrg/joda-time) from 2.10.14 to 2.11.1.
    
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.10.14...v2.11.1)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Deletes PrepperStateTest which is no longer valid after #1707 (#1732)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 7 Sep 2022 18:10:25 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Moved plugins commons projects to org.opensearch.dataprepper package. (#1726)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 7 Sep 2022 16:54:24 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added apache client for creating s3 and acm client (#1731)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 7 Sep 2022 16:43:44 -0500
    
    
    * Added apache client for creating s3 and acm client
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Remove deprecated prepper plugin type (#1707)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 7 Sep 2022 15:52:43 -0500
    
    
    * Replace prepper with processor in yaml samples
    * Replace prepper with processor in code comments
    * Remove Prepper interface and related usage
    * Change PepperState to ProcessorState
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added the SinkModel class for conditional routing on Sinks (#1681)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 7 Sep 2022 10:10:41 -0500
    
    
    Added the SinkModel class which allows applying conditional routes on sinks
    without allowing them on other plugin types.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added peer forwarder http server (#1705)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 6 Sep 2022 12:48:35 -0500
    
    efs/remotes/oeyh/main
    * Added peer forwarder server
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Run OpenSearch sink integration tests only when the plugin changes (#1706)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 6 Sep 2022 11:17:32 -0500
    
    
    Configured the OpenSearch integration tests to run only when the OpenSearch
    sink plugin is actually changed or the root Gradle project changes.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Acm private key password is nullable (#1719)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 6 Sep 2022 10:02:45 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Update the e2e test to the org.opensearch package name. Partially resolves #344 (#1703)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 2 Sep 2022 14:32:55 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Maintenance: remove support for non event data model (#1689)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Fri, 2 Sep 2022 09:33:03 -0500
    
    
    * MAINT: remove support for OTLP data transport
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Fix: CVE-2020-36518, CVE-2022-24823 (#1704)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 1 Sep 2022 10:58:09 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added custom exceptions and code refactoring (#1698)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 31 Aug 2022 15:13:48 -0500
    
    
    * Added custom exceptions and code refactoring
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Health check auth bug (#1695)__

    [David Powers](mailto:ddpowers@amazon.com) - Wed, 31 Aug 2022 14:21:58 -0500
    
    
    * Add more robust username/password check in order to allow auth-free health
    checks
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Allow specific HTTP methods for core API endpoints (#1697)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 31 Aug 2022 13:27:00 -0500
    
    
    Allows only specific HTTP methods on core API endpoints; Updates docs and unit
    tests
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __#1513: Updating antlr versions. (#1700)__

    [Jeff Zemerick](mailto:13176962+jzonthemtn@users.noreply.github.com) - Wed, 31 Aug 2022 11:42:31 -0500
    
    
    Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;

* __Remove deprecated configurations for OpenSearch sink (#1690)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 29 Aug 2022 09:23:40 -0500
    
    
    * Remove deprecated raw and service_map flags from index config
    * Update unit tests, integration tests, and README.md for OpenSearchSink
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Added Peer Forwarder Client (#1677)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 26 Aug 2022 17:08:36 -0500
    
    
    * Added Peer Forwarder Client
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add CSV Codec to S3 Source IT &amp; add CSV Processor Integration Tests (#1683)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Fri, 26 Aug 2022 15:11:58 -0500
    
    
    * Added CSV Codec to S3 Source integration tests &amp; added CSV Processor
    integration tests
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __ParseJsonProcessor initial implementation (#1688)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Fri, 26 Aug 2022 10:51:49 -0500
    
    
    * Initial implementation of parse_json processor
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Go back to using bash in the bin/data-prepper, it is a bash script and does not work on all machines using &#39;sh&#39;. Update the tarball smoke tests to work with the new directory structure. Additionally, changed the tarball smoke tests to create their own image rather than overwriting the normal Data Prepper image. (#1694)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 26 Aug 2022 10:25:52 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added CsvInvalidEvents metric to CSV Processor (#1684)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Fri, 26 Aug 2022 10:18:32 -0500
    
    
    * Added CsvInvalidEvent metric to CSV Processor
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Updated spring boot version in examples package (#1691)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 25 Aug 2022 10:03:44 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactored the PipelineParser and PipelineConfiguration to use the PipelineDataFlowModel for deserializing the pipeline YAML. This makes the deserialization consistent with serialization. It is also necessary for the upcoming conditional routing on sinks work. (#1680)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Aug 2022 13:46:15 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Prepper plugins to use Processor (#1686)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 23 Aug 2022 09:51:20 -0500
    
    
    * Update prepper to processor for StringPrepper
    * Update prepper to processor for GrokPrepper
    * Update prepper to processor for OTelTraceGroupPrepper
    * Update prepper to processor for OTelTraceRawPrepper
    * Update prepper to processor for NoOpPrepper
    * Update prepper to processor in readme files
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Created the data-prepper-main project. Updated the tar.gz to pull in all the lib files. Updated the data-prepper script to handle the new installation. Updated end-to-end tests as well. Updated the Data Prepper Docker image to use the Linux distribution including the bin/data-prepper script. (#1682)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 23 Aug 2022 09:07:23 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added ACM and S3 certificate support for HTTP source (#1678)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 22 Aug 2022 15:24:40 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Some code clean-up to Pipeline and added unit tests to PipelineTests for publishToSinks. (#1674)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 22 Aug 2022 13:51:12 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added the router to the Data Prepper pipeline model. (#1666)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Aug 2022 12:51:22 -0500
    
    
    Added the router to the Data Prepper pipeline model.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes another incorrect package related to HashRing. (#1676)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Aug 2022 11:01:31 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes an incorrect package related to HashRing. (#1675)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 18 Aug 2022 10:04:20 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the package name to org.opensearch for data-prepper-core. (#1671)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 17 Aug 2022 18:32:52 -0500
    
    
    Updated the package name to org.opensearch for data-prepper-core. Updated the
    Java main class package name to org.opensearch.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added certificate provider,  peer client pool, hash ring and client factory (#1663)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 17 Aug 2022 10:02:40 -0500
    
    
    * Added certificate provider, hash ring, peer client
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added logstash config conversion for csv processor &amp; added explanatio… (#1659)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Wed, 17 Aug 2022 09:55:19 -0500
    
    
    * Added logstash config conversion for csv processor &amp; added explanation of
    boolean to LogstashValueType javadoc
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Removes the data-prepper-benchmarks project which is not in use. (#1662)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 15 Aug 2022 11:13:53 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Build with Gradle 7.5.1, the current latest version. (#1667)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 15 Aug 2022 11:08:32 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Health check auth bug (#1625)__

    [David Powers](mailto:ddpowers@amazon.com) - Fri, 12 Aug 2022 16:40:57 -0500
    
    
    Add auth free health check with a configuration option
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Support loading plugins from both org.opensearch.dataprepper and com.amazon.dataprepper to help migrate to the new package name. (#1661)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 12 Aug 2022 12:26:41 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump micrometer-bom from 1.9.0 to 1.9.3 (#1646)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 11 Aug 2022 20:34:13 -0500
    
    
    Bumps [micrometer-bom](https://github.com/micrometer-metrics/micrometer) from
    1.9.0 to 1.9.3.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.9.0...v1.9.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: io.micrometer:micrometer-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jakarta.validation-api from 3.0.1 to 3.0.2 (#1647)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 11 Aug 2022 20:33:22 -0500
    
    
    Bumps
    [jakarta.validation-api](https://github.com/eclipse-ee4j/beanvalidation-api)
    from 3.0.1 to 3.0.2.
    - [Release notes](https://github.com/eclipse-ee4j/beanvalidation-api/releases)
    -
    [Commits](https://github.com/eclipse-ee4j/beanvalidation-api/compare/3.0.1...3.0.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: jakarta.validation:jakarta.validation-api
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added javadocs to csv processor &amp; codec, added README to csv processor (#1658)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Thu, 11 Aug 2022 16:21:20 -0500
    
    
    * Added javadocs to csv processor &amp; codec, added README to csv processor
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
    
    * Addressed David&#39;s feedback &amp; rewrote quote_character description
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Created the data-prepper script for the new directory structure (#1655) (#1656)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Aug 2022 15:14:55 -0500
    
    
    Created the data-prepper script from the data-prepper-tar-install script. This
    now sits in the bin/ directory of the directory structure. Moved the
    data-prepper-core library to the lib/ directory.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch (#1652)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 11 Aug 2022 14:41:50 -0500
    
    
    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.12.10 to
    1.12.13.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.10...byte-buddy-1.12.13)
    
    
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

* __Removed the deprecated type property on DataPrepperPlugin. Resolves #1657. (#1660)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Aug 2022 14:33:58 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1552)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 10 Aug 2022 17:13:27 -0500
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.12.10 to
    1.12.12.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.10...byte-buddy-1.12.12)
    
    
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

* __Bump bcpkix-jdk15on in /data-prepper-plugins/otel-metrics-source (#1559)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 10 Aug 2022 17:12:51 -0500
    
    
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
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump junit-jupiter-engine from 5.8.2 to 5.9.0 (#1649)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 10 Aug 2022 17:12:23 -0500
    
    
    Bumps [junit-jupiter-engine](https://github.com/junit-team/junit5) from 5.8.2
    to 5.9.0.
    - [Release notes](https://github.com/junit-team/junit5/releases)
    - [Commits](https://github.com/junit-team/junit5/compare/r5.8.2...r5.9.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.junit.jupiter:junit-jupiter-engine
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Remove dependency on AWS S3 SDKv1 (#1628)__

    [Jeff Zemerick](mailto:13176962+jzonthemtn@users.noreply.github.com) - Wed, 10 Aug 2022 16:45:20 -0500
    
    
    * #1562: Working toward removing dependency.
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;
    
    * #1562: Removing unneeded parts. Changing date processing.
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;
    
    * #1562: Adding file header and comment.
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;
    
    * #1562: Removing sysout and removing public from class.
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;
     Signed-off-by: jzonthemtn &lt;jzemerick@opensourceconnections.com&gt;

* __Updated pipeline configurations and documentation to prefer the OpenSearch index_type configuration over the trace_analytics_raw and trace_analytics_service_map booleans. The latter two configurations are deprecated and will be removed in 2.0. (#1650)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 10 Aug 2022 13:10:14 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Implemented CSVCodec for S3 Source, config &amp; unit tests (#1644)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Tue, 9 Aug 2022 15:40:28 -0500
    
    
    * Implemented CSVCodec for S3 Source, config &amp; unit tests
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
    
    * Addressed Travis&#39;s feedback
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
    
    * Addressed David&#39;s feedback &amp; fixed NPE if header is null
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;
    
    * Renamed all instances of CSVProcessor to CsvProcessor (and offshoots like
    CsvProcessorConfig)
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Added ACM for SLL and Discovery Mode Configurations (#1645)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 9 Aug 2022 15:39:50 -0500
    
    
    * Added ACM for SLL and Discovery Mode Configurations
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Enhanced Newline Codec w optional header_destination to add first lin… (#1640)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Tue, 9 Aug 2022 15:38:47 -0500
    
    
    * Enhanced Newline Codec w optional header_destination to add first line as
    header to outgoing events
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Bump log4j-bom from 2.17.2 to 2.18.0 in /data-prepper-expression (#1637)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 13:42:01 -0500
    
    
    Bumps log4j-bom from 2.17.2 to 2.18.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump log4j-bom from 2.17.2 to 2.18.0 in /data-prepper-core (#1634)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 13:41:36 -0500
    
    
    Bumps log4j-bom from 2.17.2 to 2.18.0.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib-common in /data-prepper-plugins/mapdb-prepper-state (#1630)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 13:33:19 -0500
    
    
    Bumps [kotlin-stdlib-common](https://github.com/JetBrains/kotlin) from 1.7.0 to
    1.7.10.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.7.10/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.7.0...v1.7.10)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib-common from 1.7.0 to 1.7.10 (#1635)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 13:32:42 -0500
    
    
    Bumps [kotlin-stdlib-common](https://github.com/JetBrains/kotlin) from 1.7.0 to
    1.7.10.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.7.10/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.7.0...v1.7.10)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump junit-jupiter-api from 5.7.0 to 5.9.0 (#1626)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:23:23 -0500
    
    
    Bumps [junit-jupiter-api](https://github.com/junit-team/junit5) from 5.7.0 to
    5.9.0.
    - [Release notes](https://github.com/junit-team/junit5/releases)
    - [Commits](https://github.com/junit-team/junit5/compare/r5.7.0...r5.9.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.junit.jupiter:junit-jupiter-api
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.21 to 5.3.22 in /data-prepper-core (#1633)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:22:44 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.21 to 5.3.22 in /data-prepper-core (#1631)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:15:38 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.21 to 5.3.22 in /data-prepper-expression (#1638)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:15:11 -0500
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-core from 5.3.21 to 5.3.22 in /data-prepper-expression (#1639)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:14:50 -0500
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.21 to 5.3.22 in /data-prepper-expression (#1636)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 9 Aug 2022 10:06:41 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.21 to 5.3.22 in /data-prepper-core (#1632)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 8 Aug 2022 13:33:46 -0500
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.21 to 5.3.22.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.21...v5.3.22)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib in /data-prepper-plugins/mapdb-prepper-state (#1629)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 8 Aug 2022 13:32:28 -0500
    
    
    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.7.0 to
    1.7.10.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.7.10/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.7.0...v1.7.10)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added buffer for core peer forwarder (#1641)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sun, 7 Aug 2022 22:14:41 -0500
    
    
    * Added buffer for core peer forwarder
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add @JsonProperty to workers and readBatchDelay in PipelineModel (#1642)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 5 Aug 2022 16:03:33 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Implemented CSV Processor w unit tests, added validation that delimit… (#1627)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Wed, 3 Aug 2022 10:26:28 -0500
    
    
    * Implemented CSV Processor w unit tests, added validation that delimiter and
    quote char are different
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __#1457: Adding bulk create option. (#1561)__

    [Jeff Zemerick](mailto:13176962+jzonthemtn@users.noreply.github.com) - Sat, 30 Jul 2022 11:11:45 -0500
    
    
    Signed-off-by: jzonthemtn &lt;jeff.zemerick@mtnfog.com&gt;

* __Added core peer forwarder config (#1621)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 29 Jul 2022 12:04:55 -0500
    
    
    * Added core peer forwarder config
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Created CSVProcessor skeleton &amp; CSVProcessorConfig (#1620)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Thu, 28 Jul 2022 13:47:12 -0500
    
    
    * Created CSVProcessor skeleton &amp; wrote CSVProcessorConfig
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Add buffer PluginModel to PipelineModel, require 1 source and at least 1 sink (#1611)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 27 Jul 2022 15:37:33 -0500
    
    
    * Add buffer PluginModel to PipelineModel, require 1 source and at least 1 sink
    
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updated PipelineParser to create decorator for stateful processors (#1598)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 20 Jul 2022 12:55:44 -0500
    
    
    * Updated PipelineParser to create decorator for stateful processors
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added Core Peer Forwarder decorator and interface (#1592)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 18 Jul 2022 21:07:17 -0500
    
    
    * Added Core Peer Forwarder decorator and interface
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated integration tests with GZIP compression tests (#1577)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 18 Jul 2022 15:33:50 -0500
    
    
    * Updated integration tests with GZIP compression tests
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Addressed PR feedback
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Wrote unit tests for LogGeneratorSource, updated gradle.build dependencies (#1588)__

    [Finn](mailto:67562851+finnroblin@users.noreply.github.com) - Sat, 16 Jul 2022 12:03:55 -0500
    
    
    * Wrote unit tests for LogGeneratorSource, updated dependencies
     Signed-off-by: Finn Roblin &lt;finnrobl@amazon.com&gt;

* __Updated trace analytics doc URL&#39;s (#1583)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 13 Jul 2022 10:06:38 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Remove deprecated PluginFactory classes #1584 (#1585)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 11 Jul 2022 21:05:04 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump aws-java-sdk-s3 from 1.12.220 to 1.12.257 (#1586)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 11 Jul 2022 13:46:22 -0500
    
    
    Bumps [aws-java-sdk-s3](https://github.com/aws/aws-sdk-java) from 1.12.220 to
    1.12.257.
    - [Release notes](https://github.com/aws/aws-sdk-java/releases)
    - [Changelog](https://github.com/aws/aws-sdk-java/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/aws/aws-sdk-java/compare/1.12.220...1.12.257)
    
    ---
    updated-dependencies:
    - dependency-name: com.amazonaws:aws-java-sdk-s3
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Create Log Generator Source with common apache log type, move ApacheLogFaker class from e2e-test to log-generator-source (#1548)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 11 Jul 2022 12:23:04 -0500
    
    
    Signed-off-by: graytaylor0 &lt;tylgry@amazon.com&gt;

* __Added the CHANGELOG for 1.5.1 (#1580)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 8 Jul 2022 11:44:46 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added release notes for Data Prepper 1.5.1. (#1576)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 8 Jul 2022 11:09:58 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix: Updated GZipCompressionEngine to use GzipCompressorInputStream (#1570)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 7 Jul 2022 19:21:02 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add QueryParameters option to AWS CloudMap-based peer discovery (#1560)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 7 Jul 2022 11:56:10 -0500
    
    
    * Implement QueryParameters filter option for AWS CloudMap peer forwarder
    discovery
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Small fixes related to import statements and unit tests. Update README
    wording to be a bit more specific
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Adding some asserts to unit tests
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Randomize map generation in AwsCloudMapPeerListProvider and add test case for
    QueryParameters missing from PluginSetting in
    AwsCloudMapPeerListProvider_CreateTest
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix: Added exception handling for S3 processing (#1551)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 5 Jul 2022 11:03:55 -0500
    
    
    * Fix: Added exception handling for S3 processing and updated poll delay
    condition
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated the sample OTel Collector to the current latest version: 0.54.0. (#1545)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 30 Jun 2022 09:45:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Enable HTTP Health Check for OTelTraceSource and OTelMetricsSource. (#1547)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Wed, 29 Jun 2022 11:11:48 -0500
    
    
    * Enable HTTP Health Check for OTelTraceSource and OTelMetricsSource.
    * Updated Readme file and added unit test for configurations
     Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    

* __Version bump to 2.0.0-SNAPSHOT on the main branch. (#1534)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 24 Jun 2022 11:28:06 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes a bug in the SqsWorkerIT where an NPE occurs. Updated the README.md to include the new steps. (#1538)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 24 Jun 2022 11:27:54 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added the auto-generated CHANGELOG for Data Prepper 1.5.0 (#1535)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 23 Jun 2022 11:12:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


