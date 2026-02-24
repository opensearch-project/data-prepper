
* __otel_apm_service_map: Added support for deriving remote service and remote operation (#6539) (#6565)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 24 Feb 2026 14:53:53 -0800
    
    EAD -&gt; refs/heads/2.14, refs/remotes/upstream/2.14
    * otel_apm_service_map: Added support for deriving remote service and remote
    operation
    
    
    
    * Addressed review comments
    
    
    
    * Addressed review comments
    
    
    
    * Fixed license header check failures
    
    
    
    ---------
    
    
    (cherry picked from commit fa41484fb49279a5f99c8161de9cd1147041344e)
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt; Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Prepare release 2.14.0 (#6563)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 23 Feb 2026 13:02:58 -0800
    
    
    Signed-off-by: github-actions[bot]
    &lt;41898282+github-actions[bot]@users.noreply.github.com&gt; Co-authored-by:
    dlvenable &lt;293424+dlvenable@users.noreply.github.com&gt;

* __Otel logs source http service (#6250)__

    [Tomas](mailto:tlongo@sternad.de) - Fri, 20 Feb 2026 09:45:57 +0100
    
    
    Introduce HTTP/protobuf and HTTP/JSON support for OTel Logs source. Adds
    endpoint to receive OTLP data over HTTP. Aligns with similar support for
    OTelTraceSource.
    
    * [WIP] Integrate http service and make sure it works properly
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Integrate grpc and http service into a single server
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Extract tests that assert grpc requests
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Fix return value of http service
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Re-introduce unframed request for the grpc service
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Add E2E test
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Add E2E test for gRPC
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Add test for unframed requests
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Add e2e test for unframed requests
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Add e2e test for protobuf requests
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Fix media type for protobuf payload
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Update license headers
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Adhere to config when it comes to chose a codec
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Inject OtelProtoCodec into ArmeriaHttpService
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    ---------
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;

* __Fix potential NPE in scroll worker (#6541)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 19 Feb 2026 13:34:33 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Unified Node/NodeOperationDetail model for APM service map processor (#6536)__

    [Vamsi Manohar](mailto:reddyvam@amazon.com) - Wed, 18 Feb 2026 18:59:08 -0800
    
    
    * Refactor to unified Node/NodeOperationDetail with SERVICE_MAP_V2 events
    
    - Replace Service/ServiceConnection/ServiceOperationDetail with unified
    Node/NodeOperationDetail model
    - Node adds type field for future entity types (database, queue, etc)
    - Operation simplified to name + attributes
    - NodeOperationDetail single entity with dual hash fields (nodeConnectionHash,
    operationConnectionHash)
    - CLIENT-span-primary emission: CLIENT spans emit full NodeOperationDetail,
    leaf SERVER spans for services with no outgoing calls
    - Update eventType to SERVICE_MAP_V2 for new index pattern
    otel-v2-apm-service-map
    
    Co-Authored-By: Claude Opus 4.6 &lt;noreply@anthropic.com&gt; Signed-off-by: Vamsi
    Manohar &lt;reddyvam@amazon.com&gt;
    
    * Refactor model to unified Node/NodeOperationDetail with CLIENT-primary
    emission
    
    Refactors the APM service map processor to use a unified model structure 
    replacing Service/ServiceConnection/ServiceOperationDetail with
    Node/NodeOperationDetail. Implements CLIENT-span-primary emission algorithm to
    eliminate duplicate events.
    
    Model Changes:
    - Node.java: Unified service entity with type field for future extensibility
    - NodeOperationDetail: Single entity for both topology and operation events
    - Operation: Simplified to name + attributes structure
    - CLIENT-primary algorithm: CLIENT spans emit complete events using decoration
    data
    
    OpenSearch Integration:
    - Added OTEL_APM_SERVICE_MAP index type following PR #6435 patterns
    - Index template with dynamic mappings for groupByAttributes and operation
    attributes
    - ISM policies for automated rollover (10gb/24h)
    - Updated IndexManagerFactory, IndexConstants, IndexConfiguration
    
    Configuration:
    - Changed eventType from SERVICE_MAP_V2 to SERVICE_MAP
    - Updated README with proper index_type: otel-v2-apm-service-map configuration
    - Coexists with legacy service_map processor using different index patterns
    
    Co-Authored-By: Claude Opus 4.6 &lt;noreply@anthropic.com&gt; Signed-off-by: Vamsi
    Manohar &lt;reddyvam@amazon.com&gt;
    
    * Update README with algorithm docs and remove standalone MD files
    
    - Merged NodeOperationDetail algorithm documentation into processor README
    - Updated output events section to reflect unified Node/NodeOperationDetail
    model
    - Added detailed metrics generation documentation
    - Removed standalone node-operation-detail-algorithm.md from tracking
    - Added algorithm design MD files to .gitignore
    
    Co-Authored-By: Claude Opus 4.6 &lt;noreply@anthropic.com&gt; Signed-off-by: Vamsi
    Manohar &lt;reddyvam@amazon.com&gt;
    
    * Fix index-template version to use composable template format
    
    The index-template/ copy needs the &#34;template&#34; wrapper for the modern OpenSearch
    composable index template API, while the root copy uses the V1 legacy format.
    Previously both were identical (V1 format).
    
    Co-Authored-By: Claude Opus 4.6 &lt;noreply@anthropic.com&gt; Signed-off-by: Vamsi
    Manohar &lt;reddyvam@amazon.com&gt;
    
    * Clean up .gitignore and optimize index templates
    
    - Remove local-only entries from .gitignore (algorithm MDs, claude.md)
    - Collapse 12 dynamic templates to 6 using wildcard path matching
    - Remove ignore_above restrictions from all keyword fields
    - Fix index-template/ copy to use composable template format
    
    Co-Authored-By: Claude Opus 4.6 &lt;noreply@anthropic.com&gt; Signed-off-by: Vamsi
    Manohar &lt;reddyvam@amazon.com&gt;
    
    * Remove eventType from index templates and fix test issues
    
    - Remove eventType field from both index templates (V1 and composable)
     eventType is pipeline routing metadata, not part of NodeOperationDetail model
    - Remove broken testWindowProcessingWithInterruptedException test
     Test doesn&#39;t work with @DataPrepperPluginTest annotation due to override
    restrictions
    
    Addresses review feedback from @kkondaka
    
    Co-Authored-By: Claude Opus 4.6 &lt;noreply@anthropic.com&gt; Signed-off-by: Vamsi
    Manohar &lt;reddyvam@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Vamsi Manohar &lt;reddyvam@amazon.com&gt; Co-authored-by: Claude Opus
    4.6 &lt;noreply@anthropic.com&gt;

* __Added support of newline in output codec (#6423)__

    [Subrahmanyam-Gollapalli](mailto:subrahmanyam.gollapalli@freshworks.com) - Wed, 18 Feb 2026 16:17:41 -0600
    
    
    * Added support of newline in the output codec
    
    Signed-off-by: Subrahmanyam-Gollapalli &lt;subrahmanyam.gollapalli@freshworks.com&gt;
    
    * config to write empty events
    
    Signed-off-by: Subrahmanyam-Gollapalli &lt;subrahmanyam.gollapalli@freshworks.com&gt;
    
    * updated licence header
    
    Signed-off-by: Subrahmanyam-Gollapalli &lt;subrahmanyam.gollapalli@freshworks.com&gt;
    
    ---------
    
    Signed-off-by: Subrahmanyam-Gollapalli &lt;subrahmanyam.gollapalli@freshworks.com&gt;

* __Reducing Dockerfile image size greatly by consolidating layers, running chown on copy to avoid duplicate data, running dnf clean, and removing dnf cache. (#6520)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 18 Feb 2026 07:18:52 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the license headers for various Gradle build files. (#6534)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 17 Feb 2026 14:01:53 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the license headers for the data-prepper-core project. (#6532)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 17 Feb 2026 12:29:12 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the license headers for the performance-test project. (#6531)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 17 Feb 2026 12:29:04 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add otel-apm-service-map processor (#6479)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 17 Feb 2026 08:01:54 -0800
    
    
    Add otel-apm-service-map processor
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt; Co-authored-by: Santhosh
    Gandhe &lt;1909520+san81@users.noreply.github.com&gt;, Neeraj Kumar
    &lt;kneeraj@amazon.com&gt;

* __Add additional metrics for otel logs source and kafka buffer producer (#6512)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 16 Feb 2026 18:13:54 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Adds release notes and change log for Data Prepper 2.13.1. (#6525)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 16 Feb 2026 13:51:46 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __fix: change eventbridge notification file size from int to long (#6497)__

    [Leila Moussa](mailto:leila.farah.moussa@gmail.com) - Mon, 16 Feb 2026 13:11:45 -0800
    
    
    Signed-off-by: LeilaMoussa &lt;leila.farah.moussa@gmail.com&gt;

* __Use a Timer for the sinkRequestLatency metric so that Micrometer reports it with correct units. Adds a new method to record with time units and retains the existing method for double. Update usage of this as well. (#6510)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 16 Feb 2026 11:14:18 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates Rhino to 1.7.15.1. Fixes CVE-2025-66453. (#6519)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 16 Feb 2026 10:46:48 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Netty to 4.1.131. Resolves CVE-2025-67735, CVE-2025-59419. (#6518)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 16 Feb 2026 10:46:39 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use a Timer for the sqsSinkRequestLatency metric so that Micrometer reports it with correct units. (#6513)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 16 Feb 2026 10:45:23 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Corrects Kafka buffer metrics related to write timeouts and write time elapsed. This is solved by implementing writeBytes in AbstractBuffer and adding a doWriteBytes method that throws by default. This keeps the default behavior of throwing, but with correct metric reporting. (#6506)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 Feb 2026 10:27:23 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __When running smoke tests verify the correct architecture. Update GitHub Actions to run for Docker smoke tests. (#6504)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 Feb 2026 09:24:11 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates license headers for core build files. (#6500)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 Feb 2026 09:05:56 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Additional documentation for browsing Data Prepper snapshots online. (#6495)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 Feb 2026 09:05:32 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.wiremock:wiremock in /data-prepper-plugins/s3-source (#6302)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:12:58 -0800
    
    
    Bumps [org.wiremock:wiremock](https://github.com/wiremock/wiremock) from 3.13.1
    to 3.13.2.
    - [Release notes](https://github.com/wiremock/wiremock/releases)
    - [Commits](https://github.com/wiremock/wiremock/compare/3.13.1...3.13.2)
    
    --- updated-dependencies:
    - dependency-name: org.wiremock:wiremock
     dependency-version: 3.13.2
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.projectlombok:lombok in /data-prepper-plugins/opensearch (#6107)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:11:51 -0800
    
    
    Bumps [org.projectlombok:lombok](https://github.com/projectlombok/lombok) from
    1.18.38 to 1.18.42.
    -
    [Changelog](https://github.com/projectlombok/lombok/blob/master/doc/changelog.markdown)
    -
    [Commits](https://github.com/projectlombok/lombok/compare/v1.18.38...v1.18.42)
    
    --- updated-dependencies:
    - dependency-name: org.projectlombok:lombok
     dependency-version: 1.18.42
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.zendesk:mysql-binlog-connector-java from 0.29.2 to 0.30.1 (#6036)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:10:41 -0800
    
    
    Bumps
    [com.zendesk:mysql-binlog-connector-java](https://github.com/osheroff/mysql-binlog-connector-java)
    from 0.29.2 to 0.30.1.
    -
    [Changelog](https://github.com/osheroff/mysql-binlog-connector-java/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/osheroff/mysql-binlog-connector-java/commits)
    
    --- updated-dependencies:
    - dependency-name: com.zendesk:mysql-binlog-connector-java
     dependency-version: 0.30.1
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.github.luben:zstd-jni in /data-prepper-plugins/common (#6215)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:09:16 -0800
    
    
    Bumps [com.github.luben:zstd-jni](https://github.com/luben/zstd-jni) from
    1.5.7-4 to 1.5.7-6.
    - [Commits](https://github.com/luben/zstd-jni/compare/v1.5.7-4...v1.5.7-6)
    
    --- updated-dependencies:
    - dependency-name: com.github.luben:zstd-jni
     dependency-version: 1.5.7-6
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.jayway.jsonpath:json-path-assert (#6218)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:08:59 -0800
    
    
    Bumps
    [com.jayway.jsonpath:json-path-assert](https://github.com/jayway/JsonPath) from
    2.6.0 to 2.10.0.
    - [Release notes](https://github.com/jayway/JsonPath/releases)
    - [Changelog](https://github.com/json-path/JsonPath/blob/master/changelog.md)
    -
    [Commits](https://github.com/jayway/JsonPath/compare/json-path-2.6.0...json-path-2.10.0)
    
    --- updated-dependencies:
    - dependency-name: com.jayway.jsonpath:json-path-assert
     dependency-version: 2.10.0
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump js-yaml from 3.14.1 to 3.14.2 in /release/staging-resources-cdk (#6276)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:08:10 -0800
    
    
    Bumps [js-yaml](https://github.com/nodeca/js-yaml) from 3.14.1 to 3.14.2.
    - [Changelog](https://github.com/nodeca/js-yaml/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/nodeca/js-yaml/compare/3.14.1...3.14.2)
    
    --- updated-dependencies:
    - dependency-name: js-yaml
     dependency-version: 3.14.2
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-trace-source (#6454)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:07:15 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.27.3 to 3.27.7.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.27.3...assertj-build-3.27.7)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-version: 3.27.7
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#6456)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:06:44 -0800
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.18.3 to 1.18.4.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.18.3...byte-buddy-1.18.4)
    
    --- updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-version: 1.18.4
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#6455)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:06:25 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.27.3 to 3.27.7.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.27.3...assertj-build-3.27.7)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-version: 3.27.7
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/http-source (#6459)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:06:12 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.27.3 to 3.27.7.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.27.3...assertj-build-3.27.7)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-version: 3.27.7
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.httpcomponents.client5:httpclient5 (#6461)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:05:56 -0800
    
    
    Bumps
    [org.apache.httpcomponents.client5:httpclient5](https://github.com/apache/httpcomponents-client)
    from 5.5 to 5.6.
    -
    [Changelog](https://github.com/apache/httpcomponents-client/blob/master/RELEASE_NOTES.txt)
    -
    [Commits](https://github.com/apache/httpcomponents-client/compare/rel/v5.5...rel/v5.6)
    
    --- updated-dependencies:
    - dependency-name: org.apache.httpcomponents.client5:httpclient5
     dependency-version: &#39;5.6&#39;
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.maven:maven-artifact in /data-prepper-plugins/opensearch (#6457)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:05:42 -0800
    
    
    Bumps org.apache.maven:maven-artifact from 3.9.11 to 3.9.12.
    
    --- updated-dependencies:
    - dependency-name: org.apache.maven:maven-artifact
     dependency-version: 3.9.12
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#6463)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:04:36 -0800
    
    
    Bumps software.amazon.awssdk:auth from 2.39.5 to 2.41.19.
    
    --- updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-version: 2.41.19
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.json:json in /data-prepper-plugins/avro-codecs (#6465)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:03:31 -0800
    
    
    Bumps [org.json:json](https://github.com/douglascrockford/JSON-java) from
    20250517 to 20251224.
    - [Release notes](https://github.com/douglascrockford/JSON-java/releases)
    -
    [Changelog](https://github.com/stleary/JSON-java/blob/master/docs/RELEASES.md)
    - [Commits](https://github.com/douglascrockford/JSON-java/commits)
    
    --- updated-dependencies:
    - dependency-name: org.json:json
     dependency-version: &#39;20251224&#39;
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-logs-source (#6469)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:02:59 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.27.3 to 3.27.7.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.27.3...assertj-build-3.27.7)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-version: 3.27.7
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.apptasticsoftware:rssreader in /data-prepper-plugins/rss-source (#6468)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:02:37 -0800
    
    
    Bumps [com.apptasticsoftware:rssreader](https://github.com/w3stling/rssreader)
    from 3.7.0 to 3.12.0.
    - [Release notes](https://github.com/w3stling/rssreader/releases)
    - [Commits](https://github.com/w3stling/rssreader/compare/v3.7.0...v3.12.0)
    
    --- updated-dependencies:
    - dependency-name: com.apptasticsoftware:rssreader
     dependency-version: 3.12.0
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core in /data-prepper-plugins/otel-proto-common (#6467)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 13 Feb 2026 07:02:22 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.27.3 to 3.27.7.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.27.3...assertj-build-3.27.7)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-version: 3.27.7
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __fix: Dedup failure metrics (#6501)__

    [chrisale000](mailto:alchrisk@amazon.com) - Wed, 11 Feb 2026 15:49:50 -0600
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __Add derived server-side attributes to OTel trace raw processor (#6481)__

    [Vamsi Manohar](mailto:g.vamsimanoharreddy@gmail.com) - Tue, 10 Feb 2026 10:03:14 -0800
    
    
    This commit adds functionality to automatically derive server-side attributes 
    for OpenTelemetry traces based on span characteristics:
    
    - Created OTelSpanDerivationUtil utility class to handle attribute derivation
    logic
    - Added support for deriving error, fault, throttle, and generic name
    attributes
    - Integrated derivation logic into OTelTraceRawProcessor with configurable
    enable/disable
    - Added comprehensive test coverage including unit tests and test resources
    - Updated .gitignore and other configuration files
    
    Key features:
    - Automatic error detection from span status and HTTP status codes
    - Fault detection for 5xx HTTP responses and error status codes
    - Throttle detection for 429 HTTP status codes
    - Generic name flagging for spans with generic operation names
    - Environment and operation attribute handling
    
    Signed-off-by: Vamsi Manohar &lt;reddyvam@amazon.com&gt;

* __fix: Add atomic counter for reliable partition count tracking in DimensionalTimeSliceCrawler (#6449)__

    [chrisale000](mailto:alchrisk@amazon.com) - Mon, 9 Feb 2026 16:00:47 -0600
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __refactor(source-crawler): enable direct injection of VendorAPIMetricsRecorder (#6444)__

    [chrisale000](mailto:alchrisk@amazon.com) - Mon, 9 Feb 2026 15:44:58 -0600
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __Distribute ARM archives. This copies the ARM archives during the promote archives step of the Jenkins job for promotion. (#6498)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 9 Feb 2026 12:33:25 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Release ARM artifacts and run smoke tests for them (#6499)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 9 Feb 2026 11:56:00 -0800
    
    
    Run smoke tests for ARM artifacts, both the Docker image and the archives.
    Includes Java 21 smoke tests both for ARM and x86. Corrects the release build
    to push the Docker image. Some modernization to GHA plugins.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix OTEL trace source broken by PR 5322 (#6494)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Sun, 8 Feb 2026 09:27:49 -0800
    
    
    * Fix OTEL trace source broken by PR 5322
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified HttpService to use appropriate Decoder base on output_format
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Test the opensearch sink against OpenSearch 3. (#6491)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 6 Feb 2026 07:19:47 -0800
    
    
    Use complicated password for OpenSearch 3.x and beyond.
    
    Addresses some issues found during testing where test would fail or get stuck
    by closing resources better and adding test timeouts. Move waiting on the
    OpenSearch cluster into the OpenSearchIT class.
    
    Updated the test names for the GitHub Actions. Clarified the steps with a wait
    period. Updated some actions versions.
    
    Resolves #6485.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __refactor: make s3 sqs shutdown configurable (#6489)__

    [Leila Moussa](mailto:leila.farah.moussa@gmail.com) - Thu, 5 Feb 2026 13:48:00 -0800
    
    
    Signed-off-by: LeilaMoussa &lt;leila.farah.moussa@gmail.com&gt;

* __Flush remaining data to S3 during shutdown (#6424)__

    [Subrahmanyam-Gollapalli](mailto:subrahmanyam.gollapalli@freshworks.com) - Thu, 5 Feb 2026 11:18:42 -0800
    
    
    flush remaining data to S3 during shutdown
    
    Signed-off-by: Subrahmanyam-Gollapalli &lt;subrahmanyam.gollapalli@freshworks.com&gt;

* __GitHub Action to prepare a branch for the next release. (#6487)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 5 Feb 2026 08:56:21 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Remove SingleThread annotation from Drop Events Processor (#6417)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 4 Feb 2026 13:51:58 -0800
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Update werkzeug and protobuf versions for sample-app (CVE-2026-21860 and CVE-2025-66221) (#6476)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 3 Feb 2026 15:07:52 -0800
    
    
    * Update werkzeug and protobuf versions for sample-app (CVE-2026-21860 and
    CVE-2025-66221)
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed build failure by replacing dash with flask
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed build failure by updating opentelemetry versions
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump org.assertj:assertj-core (#6460)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 2 Feb 2026 06:55:12 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.27.3 to 3.27.7.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.27.3...assertj-build-3.27.7)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-version: 3.27.7
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.assertj:assertj-core (#6458)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 2 Feb 2026 06:55:00 -0800
    
    
    Bumps [org.assertj:assertj-core](https://github.com/assertj/assertj) from
    3.27.3 to 3.27.7.
    - [Release notes](https://github.com/assertj/assertj/releases)
    -
    [Commits](https://github.com/assertj/assertj/compare/assertj-build-3.27.3...assertj-build-3.27.7)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-version: 3.27.7
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.awaitility:awaitility (#6466)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 2 Feb 2026 06:54:17 -0800
    
    
    Bumps [org.awaitility:awaitility](https://github.com/awaitility/awaitility)
    from 4.2.0 to 4.3.0.
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.2.0...awaitility-4.3.0)
    
    --- updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-version: 4.3.0
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Enable Mend Renovate in Whitesource settings (#6445)__

    [Craig Perkins](mailto:cwperx@amazon.com) - Fri, 30 Jan 2026 14:50:28 -0800
    
    
    Signed-off-by: Craig Perkins &lt;cwperx@amazon.com&gt;

* __add unit test for jackson dependency edge case (#6421)__

    [Kennedy Onyia](mailto:145404406+kennedy-onyia@users.noreply.github.com) - Fri, 30 Jan 2026 11:57:59 -0600
    
    
    Signed-off-by: Kennedy Onyia &lt;kennedy.onyia@gmail.com&gt;

* __Adding streaming support for lambda pluggin (#6273)__

    [Manan Rajotia](mailto:31757918+mananrajotia@users.noreply.github.com) - Thu, 29 Jan 2026 11:24:35 -0800
    
    
    Streaming response support for lambda plugin
    
    Signed-off-by: Manan Rajotia &lt;rajotia@amazon.com&gt;

* __Runs peer-forward and log analytics end-to-end tests on both ARM and x86 architectures. Updates these projects to use the current architecture Linux distribution when running. Include the ARM runner in the GitHub Actions matrix strategy. Combines the two GitHub Actions for peer-forwarder into a single GitHub Action. (#6414)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Jan 2026 15:39:39 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Give the license header check GitHub Action permissions to write to issues and pull requests. The check is running fine, but the comments are failing to actually post. (#6438)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Jan 2026 15:39:12 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add optional timeout configurations for AWS Lambda plugin (#6413)__

    [ashrao94](mailto:55301835+ashrao94@users.noreply.github.com) - Wed, 28 Jan 2026 14:04:56 -0600
    
    
    * Add optional timeout configurations for AWS Lambda plugin
    
    - Add api_call_attempt_timeout configuration for per-attempt timeouts
    - Make read_timeout optional (only applied when specified)
    - Add comprehensive unit tests for timeout configurations
    - Maintain backward compatibility with existing configurations
    - Follow AWS SDK timeout hierarchy best practices
    
    Both timeout parameters are now optional and only configured when explicitly
    set, allowing users to fine-tune timeout behavior for their Lambda functions.
    
    Signed-off-by: Aiswarya Sadananda Rao &lt;aiswarao@amazon.com&gt;
    
    * Add license header to ClientOptionsTest.java
    
    - Add required OpenSearch Contributors license header
    - Fixes license header violation in new test file
    
    Signed-off-by: Aiswarya Sadananda Rao &lt;aiswarao@amazon.com&gt;
    
    * Remove unused Duration import from ClientOptionsTest
    
    Signed-off-by: Aiswarya Sadananda Rao &lt;aiswarao@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Aiswarya Sadananda Rao &lt;aiswarao@amazon.com&gt; Co-authored-by:
    Aiswarya Sadananda Rao &lt;aiswarao@amazon.com&gt;

* __Adds additional thread synchronization in the AggregateProcessor to prevent duplicate or orphaned aggregate groups. (#6439)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Jan 2026 11:46:03 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Minor Spotless fix (#6440)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 28 Jan 2026 11:04:59 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __maint: remove myself from maintainers (#6434)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 28 Jan 2026 11:00:39 -0800
    
    
    maint: remove myself from maintainers maint: add Qi Chen into Emeritus
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add parameter to acknowledge group events on conclude immediately, and a parameter to disable group acknowledgments in aggregate processor (#6430)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 27 Jan 2026 11:48:55 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __feat: Add configurable lease interval for crawler source (#6432)__

    [chrisale000](mailto:alchrisk@amazon.com) - Mon, 26 Jan 2026 18:14:09 -0600
    
    
    This change adds support for configurable lease interval in the crawler source
    plugin, allowing users to customize the leader scheduler&#39;s lease interval
    instead of using a hardcoded value.
    
    Changes:
    - Added getLeaseInterval() method to CrawlerSourceConfig interface with
     default value of 1 minute
    - Modified CrawlerSourcePlugin to use the configurable lease interval
     from the source configuration
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __refactor(metrics): migrate buffer/retry metrics to unified VendorAPIMetricsRecorder (#6428)__

    [chrisale000](mailto:alchrisk@amazon.com) - Mon, 26 Jan 2026 17:56:41 -0600
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __Synchronization fix for aggregate processor and aggregate event handles when attaching events to the aggregate group. (#6431)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 26 Jan 2026 16:48:45 -0600
    
    
    There is a possible synchronization issue in the aggregate processor. It
    currently calls attachToEventAcknowledgementSet on the aggregate group outside
    of any locks. It is possible that one thread gets this group. Then thread two
    gets the closes the group. If thread 1 then attaches the event to that group,
    thread 2 may still reset it. The solution is to move
    attachToEventAcknowledgementSet into the locks.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Data Prepper developer documentation for debugging and using Maven artifacts. (#6427)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 26 Jan 2026 14:10:07 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __feat: Add intelligent subscription management and gated metrics for M365 (#6401)__

    [chrisale000](mailto:alchrisk@amazon.com) - Fri, 23 Jan 2026 15:54:14 -0600
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __Add thread-safe synchronization to startUpdatingOwnershipForShard (#6426)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Fri, 23 Jan 2026 15:52:38 -0600
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Fixes a false reporting bug for the invalidEventHandles counter (#6420)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 22 Jan 2026 15:29:07 -0800
    
    
    Fixes a bug with the invalidEventHandles counter in the PipelineRunner. This
    metric was being counted for any event that is not a default event (ie. for
    aggregate events). This would happen even if there is no need to discard the
    event. This change should count this when aggregate events should be released
    but are not. We probably need some deeper investigation into how we can
    properly release aggregate events. But, for now this metric will be more
    accurate.
    
    Also improves some code to reduce unnecessary variables, use final modifiers,
    and better legibility.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __remove json creator annotation from no-arg constructor in SinkForwardConfig (#6419)__

    [Kennedy Onyia](mailto:145404406+kennedy-onyia@users.noreply.github.com) - Thu, 22 Jan 2026 15:58:16 -0600
    
    
    Signed-off-by: Kennedy Onyia &lt;kennedy.onyia@gmail.com&gt;

* __Adding functionality to read from specific timestamps for KDS source (#6415)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Tue, 20 Jan 2026 12:14:35 -0800
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Add metadata for document version to OpenSearch source (#6416)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 20 Jan 2026 11:32:15 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Some clean up on the PrometheusSink class. There were several unused code paths. (#6412)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Jan 2026 07:54:15 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support building and releasing Docker multi-architecture images (#6411)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 16 Jan 2026 05:53:44 -0800
    
    
    Support building Docker multi-architecture images and releasing these in the
    GitHub Actions release project. Continues to build the local architecture with
    the existing docker release task. Resolves #6405, #6410.
    
    Also stops using the Palatir Docker plugin and uses Docker buildx directly.
    Resolves #5313.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add kafka buffer backward compatibility test (#6406)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 15 Jan 2026 16:57:12 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Handling mysql decimal data types with precision 19 or higher (#6369)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Thu, 15 Jan 2026 14:20:23 -0800
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Add read timeout configuration for AWS Lambda plugin (#6408)__

    [ashrao94](mailto:55301835+ashrao94@users.noreply.github.com) - Thu, 15 Jan 2026 12:03:37 -0800
    
    
    - Add read_timeout field to ClientOptions with default 60s
    - Configure NettyNioAsyncHttpClient with read timeout
    - Update README with client configuration examples
    - Enables configurable read timeout for Lambda function calls
    
    Signed-off-by: Aiswarya Sadananda Rao &lt;aiswarao@amazon.com&gt; Co-authored-by:
    Aiswarya Sadananda Rao &lt;aiswarao@amazon.com&gt;

* __Implement handling strategy for retryable vs non-retryable exceptons in workerPartition (#6270)__

    [Vecheka](mailto:cvecheka07@gmail.com) - Thu, 15 Jan 2026 10:35:50 -0800
    
    
    Signed-off-by: Vecheka Chhourn &lt;vecheka@amazon.com&gt;

* __create s3 common module (#6404)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Wed, 14 Jan 2026 08:27:28 -0800
    
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Rename out_of_order_window to out_of_order_time_window (#6398)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 13 Jan 2026 14:41:32 -0800
    
    
    * Rename out_of_order_window to out_of_order_time_window
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix PrometheusSinkServiceTest
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Support building ARM archive files locally using the Gradle build. Resolves #2571. (#6403)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 13 Jan 2026 14:10:49 -0800
    
    
    This adds new scripts for running on ARM and includes the ARM architecture in
    the Gradle release build for linux. Additionally, it updates the smoke tests
    script to be able to run against different architectures in order to test the
    changes. Updates the README.md files as well.
    
    Also updates the license headers for files in the related projects.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Disabling a DDB source coordination integration test (#6328)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 13 Jan 2026 10:55:44 -0800
    
    
    One of our new DDB source coordination integration tests is failing on GitHub.
    This change attempts to fix that by 1) including a sleep between writing events
    to ensure that they have different timestamps; and 2) waiting for the GSI to
    reach eventual consistency. In the end I disabled it.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __add partition timing metrics to LeaderOnlyTokenCrawler (#6299)__

    [Nathan Wand](mailto:wandna@amazon.com) - Fri, 9 Jan 2026 13:34:02 -0800
    
    
    add partition timing metrics to LeaderOnlyTokenCrawler 

* __Update sample app urlib3 from 2.6.0 to 2.6.3 to fix CVE-2026-21441 (#6399)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 8 Jan 2026 15:22:56 -0800
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Optimized RestClient Tests using customized Retry Handler (#6359)__

    [eatulban](mailto:eatulban@amazon.com) - Thu, 8 Jan 2026 11:19:08 -0800
    
    
    Signed-off-by: eatulban &lt;eatulban@amazon.com&gt;

* __Onboarding new maven snapshots publishing to s3 (data-prepper) (#6246)__

    [Peter Zhu](mailto:zhujiaxi@amazon.com) - Thu, 8 Jan 2026 06:55:43 -0800
    
    
    Signed-off-by: Peter Zhu &lt;zhujiaxi@amazon.com&gt;

* __Expand necessary OpenSearch permissions for data prepper (#6024)__

    [Stehlík Lukáš](mailto:28645591+stelucz@users.noreply.github.com) - Wed, 7 Jan 2026 15:22:50 -0800
    
    
    Signed-off-by: Lukas Stehlik &lt;stehlik.lukas@gmail.com&gt;

* __Update simple_pipelines.md (#6274)__

    [Sabarinathan Subramanian](mailto:22836306+sabarinathan590@users.noreply.github.com) - Wed, 7 Jan 2026 14:53:20 -0800
    
    
    reponse message updated
    
    Signed-off-by: Sabarinathan Subramanian
    &lt;22836306+sabarinathan590@users.noreply.github.com&gt;

* __Added logging for No indices matched (#6342)__

    [Utkarsh Agarwal](mailto:126544832+Utkarsh-Aga@users.noreply.github.com) - Wed, 7 Jan 2026 14:50:26 -0800
    
    
    Signed-off-by: Utkarsh Agarwal &lt;126544832+Utkarsh-Aga@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy-agent in /data-prepper-plugins/opensearch (#6378)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Jan 2026 14:48:41 -0800
    
    
    Bumps [net.bytebuddy:byte-buddy-agent](https://github.com/raphw/byte-buddy)
    from 1.17.8 to 1.18.3.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.17.8...byte-buddy-1.18.3)
    
    --- updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-version: 1.18.3
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#6377)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Jan 2026 14:48:32 -0800
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.17.8 to 1.18.3.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.17.8...byte-buddy-1.18.3)
    
    --- updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-version: 1.18.3
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.commons:commons-text (#6382)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Jan 2026 14:47:34 -0800
    
    
    Bumps [org.apache.commons:commons-text](https://github.com/apache/commons-text)
    from 1.14.0 to 1.15.0.
    -
    [Changelog](https://github.com/apache/commons-text/blob/master/RELEASE-NOTES.txt)
    -
    [Commits](https://github.com/apache/commons-text/compare/rel/commons-text-1.14.0...rel/commons-text-1.15.0)
    
    --- updated-dependencies:
    - dependency-name: org.apache.commons:commons-text
     dependency-version: 1.15.0
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Fix multiple javadoc warnings to reduce build log clutter (#6364)__

    [chrisale000](mailto:alchrisk@amazon.com) - Wed, 7 Jan 2026 14:42:35 -0800
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __Adding Subscription Metrics to Metric Recorder and Onboarding M365 to Auth Metrics from Metrics Recorder (#6363)__

    [chrisale000](mailto:alchrisk@amazon.com) - Wed, 7 Jan 2026 14:42:20 -0800
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __Adding Partition Execution Logging for DimensionalTimeSliceCrawler (#6362)__

    [chrisale000](mailto:alchrisk@amazon.com) - Wed, 7 Jan 2026 14:41:54 -0800
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __Prefer org.lgz4 artifact over at.yawk.lz4 (#6395)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Wed, 7 Jan 2026 12:33:31 -0800
    
    
    Undo a version change caused by dependabot
    
    Signed-off-by: Santhosh Gandhe &lt;1909520+san81@users.noreply.github.com&gt;
    

* __Bump org.lz4:lz4-java in /data-prepper-plugins/mapdb-processor-state (#6312)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Jan 2026 08:15:18 -0800
    
    
    Bumps org.lz4:lz4-java from 1.8.0 to 1.8.1.
    
    --- updated-dependencies:
    - dependency-name: org.lz4:lz4-java
     dependency-version: 1.8.1
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-core (#6376)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Jan 2026 08:12:58 -0800
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.25.1 to 2.25.3.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.25.1...rel/2.25.3)
    
    --- updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-version: 2.25.3
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-expression (#6379)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 7 Jan 2026 08:12:49 -0800
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.25.1 to 2.25.3.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.25.1...rel/2.25.3)
    
    --- updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-version: 2.25.3
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Fixes the license headers in all files in data-prepper-api. (#6393)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 6 Jan 2026 14:03:24 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __GitHub Action to verify that newly added files have the license header. (#6392)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 6 Jan 2026 14:03:13 -0800
    
    
    This includes Python scripts for validation as well as a GitHub Action that
    runs them and comments on PRs if license headers are missing.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updating the copyright headers on a batch of plugins. (#6390)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 6 Jan 2026 14:03:01 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Prometheus Sink: Fix setting DLQ pipeline, add NOISY marker for logs (#6388)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Mon, 5 Jan 2026 10:27:33 -0800
    
    
    

* __Support otel metrics source with partition keys when persistent buffer is used (#6373)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Fri, 2 Jan 2026 16:56:08 -0800
    
    
    * Support otel metrics source with partition keys when persistent buffer is
    used
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Optimized  splitExportMetricsServiceRequestByKeys
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add support for passing sts headers in kafka source (#6375)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Fri, 2 Jan 2026 13:02:47 -0800
    
    
    * Add support for passing sts headers in kafka source
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Add test to cover valid header use case
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Minor code change for passing override config
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Add validation for tests
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    * Increase the test coverage.
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt; Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Add metric tracking time between poll calls for kafka consumer (#6372)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 31 Dec 2025 13:51:26 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Address comments from PR6370 (#6371)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 30 Dec 2025 14:00:09 -0800
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix Data Prepper router to send records through routing strategy before sending to the sinks (#6370)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 30 Dec 2025 00:04:23 -0800
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Rebased to latest to resolve conflicts (#6365)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Mon, 22 Dec 2025 17:03:41 -0800
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Metric Centralization through Dependency Injection (#6354)__

    [chrisale000](mailto:alexchristensen11131997@gmail.com) - Thu, 18 Dec 2025 14:55:11 -0800
    
    
    This change centralizes metrics creation and management by implementing 
    dependency injection for metrics in the Office365 source plugin and other
    remaining components. This ensures consistent metrics handling across the
    codebase.
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt; Co-authored-by:
    Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __Make CWL retry indefinitely for retryable errors when no DLQ configured (#6355)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 17 Dec 2025 15:14:21 -0800
    
    
    * Make CWL retry indefinitely for retryable errors when no DLQ configured
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added tests
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fixed PrometheusSinkBufferWriter getBuffer() to return non-duplicate and sorted time series (#6358)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 17 Dec 2025 14:31:54 -0800
    
    
    * Fixed PrometheusSinkBufferWriter getBuffer() to return non-duplicate and
    sorted timeseries
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed CheckStyle error
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Add forward_to support to opensearch sink (#6349)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 16 Dec 2025 14:42:17 -0800
    
    
    * Add forward_to support to opensearch sink
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Added integration test
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Remove usage of buffer accumulator from Kafka custom consumer (#6357)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 16 Dec 2025 06:49:29 -0800
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Refactor Retry Handler To Move Into Source Crawler Package (#6275)__

    [eatulban](mailto:eatulban@amazon.com) - Mon, 15 Dec 2025 17:13:36 -0600
    
    
    Signed-off-by: eatulban &lt;eatulban@amazon.com&gt;

* __Remove experimental lable for M365 (#6351)__

    [Vecheka](mailto:vecheka@amazon.com) - Mon, 15 Dec 2025 13:17:46 -0600
    
    
    Signed-off-by: Vecheka Chhourn &lt;vecheka@amazon.com&gt;

* __Do not clear offsets after failure to commit offsets due to rebalance exception (#6346)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 15 Dec 2025 08:44:05 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Adds Kiro and Visual Studio Code directories to the .gitignore file. Some reorganization of this file. (#6353)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 12 Dec 2025 12:14:39 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update dependency urllib3 to v2.6.0 (#6345)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Thu, 11 Dec 2025 16:59:59 -0800
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Fixes the trace-analytics-sample-app project and updates it. (#6350)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 11 Dec 2025 10:00:19 -0800
    
    
    Use Gradle 9.2.1, the current latest. Update to Spring Boot 4.0.0. Updated to
    Java 21. Use a more fixed Docker image when building to avoid future build
    failures - always Gradle 9 and JDK 21.
    
    Also, updates or adds copyright headers.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Confluence and CloudWatch and multiple other failing tests fix (#6348)__

    [Santhosh Gandhe](mailto:1909520+san81@users.noreply.github.com) - Wed, 10 Dec 2025 11:16:33 -0800
    
    
    Making the tests less flaky. More reliable. Avoiding possible Out of memory
    issue with large pay load generation.

* __Enable cross-region writes in the S3 sink. (#6323)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 10 Dec 2025 11:11:44 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Eclipse Temurin by default in the tarball smoke test. Updates to the documentation for running smoke tests to reference Eclipse Temurin. (#6296)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 10 Dec 2025 08:24:12 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Centralize Metrics, Create MetricHelper Unit Tests, and Add M365 Logging (#6338)__

    [chrisale000](mailto:alexchristensen11131997@gmail.com) - Tue, 9 Dec 2025 14:30:04 -0600
    
    
    Signed-off-by: Alexander Christensen &lt;alchrisk@amazon.com&gt; Co-authored-by:
    Alexander Christensen &lt;alchrisk@amazon.com&gt;

* __set retry time interval configurable, increase the http client read timeout (#6320)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Thu, 4 Dec 2025 15:25:45 -0600
    
    
    * set retry time interval configurable and increase the http client read
    timeout
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    * address comments
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Bump org.wiremock:wiremock in /data-prepper-plugins/opensearch (#6308)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 4 Dec 2025 06:49:10 -0800
    
    
    Bumps [org.wiremock:wiremock](https://github.com/wiremock/wiremock) from 3.10.0
    to 3.13.2.
    - [Release notes](https://github.com/wiremock/wiremock/releases)
    - [Commits](https://github.com/wiremock/wiremock/compare/3.10.0...3.13.2)
    
    --- updated-dependencies:
    - dependency-name: org.wiremock:wiremock
     dependency-version: 3.13.2
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-validator:commons-validator in /data-prepper-core (#6310)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 4 Dec 2025 06:46:07 -0800
    
    
    Bumps
    [commons-validator:commons-validator](https://github.com/apache/commons-validator)
    from 1.10.0 to 1.10.1.
    -
    [Changelog](https://github.com/apache/commons-validator/blob/master/RELEASE-NOTES.txt)
    -
    [Commits](https://github.com/apache/commons-validator/compare/rel/commons-validator-1.10.0...rel/commons-validator-1.10.1)
    
    --- updated-dependencies:
    - dependency-name: commons-validator:commons-validator
     dependency-version: 1.10.1
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#6315)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 4 Dec 2025 06:44:35 -0800
    
    
    Bumps software.amazon.awssdk:auth from 2.32.13 to 2.39.5.
    
    --- updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-version: 2.39.5
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __PrometheusTimeSeries performance fixes (#6316)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 3 Dec 2025 22:18:59 -0800
    
    
    * PrometheusTimeSeries performance fixes
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkStyle error
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Increase acknowledgment set timeout for opensearch source (#6291)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 3 Dec 2025 14:07:59 -0600
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Filesource compression support (#5255)__

    [Joël Marty](mailto:134835+joelmarty@users.noreply.github.com) - Wed, 3 Dec 2025 06:37:11 -0800
    
    
    Add support for compressed files in FileSource
    
    Signed-off-by: Joël Marty &lt;jmarty@twilio.com&gt; Signed-off-by: Joël Marty
    &lt;134835+joelmarty@users.noreply.github.com&gt;

* __Increase SAAS WorkerScheduler WorkerPartition AcknowledgementSet timeout to 2 hours (#6298)__

    [wjyao0316](mailto:88009805+wjyao0316@users.noreply.github.com) - Wed, 26 Nov 2025 11:05:06 -0800
    
    
    *What?**
    
    This commit incerases SAAS worker partition AcknowledgementSet timeout from 20
    seconds to 2 hours.
    
    **Why?**
    
    20 seconds is not enough for finish processing each worker partition. Increase
    it to infinite high to ensure each worker partition has enough time for
    processing.
    
    Signed-off-by: Wenjie Yao &lt;wjyao@amazon.com&gt; Co-authored-by: Wenjie Yao
    &lt;wjyao@amazon.com&gt;

* __Run the release build against Eclipse Temurin instead of the old OpenJDK Docker images. (#6293)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 24 Nov 2025 15:37:19 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump js-yaml in /testing/aws-testing-cdk (#6280)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 21 Nov 2025 07:57:18 -0800
    
    
    Bumps  and [js-yaml](https://github.com/nodeca/js-yaml). These dependencies
    needed to be updated together.
    
    Updates `js-yaml` from 4.1.0 to 4.1.1
    - [Changelog](https://github.com/nodeca/js-yaml/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/nodeca/js-yaml/compare/4.1.0...4.1.1)
    
    Updates `js-yaml` from 3.14.1 to 3.14.2
    - [Changelog](https://github.com/nodeca/js-yaml/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/nodeca/js-yaml/compare/4.1.0...4.1.1)
    
    --- updated-dependencies:
    - dependency-name: js-yaml
     dependency-version: 4.1.1
     dependency-type: indirect
    - dependency-name: js-yaml
     dependency-version: 3.14.2
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Various improvements to the 2.13 release notes. (#6289)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 20 Nov 2025 10:18:13 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __DataPrepper 2.13.0 Changelog (#6287)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 20 Nov 2025 08:44:57 -0800
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Release notes for version 2.13.0 (#6286)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 20 Nov 2025 08:44:01 -0800
    
    
    Release notes for version 2.13.0
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Bump net.bytebuddy:byte-buddy-agent in /data-prepper-plugins/opensearch (#6217)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Nov 2025 13:58:05 -0800
    
    
    Bumps [net.bytebuddy:byte-buddy-agent](https://github.com/raphw/byte-buddy)
    from 1.17.6 to 1.17.8.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.17.6...byte-buddy-1.17.8)
    
    --- updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-version: 1.17.8
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#6219)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Nov 2025 13:57:33 -0800
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.17.6 to 1.17.8.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.17.6...byte-buddy-1.17.8)
    
    --- updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-version: 1.17.8
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __main branch to Data Prepper 2.14 (#6283)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 19 Nov 2025 13:19:57 -0800
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;


