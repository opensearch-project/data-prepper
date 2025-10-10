## 2021-12-15 Version 1.2.0

---

* __Log4j 2.16.0 backport to Data Prepper 1.2.0 (#742)__

  [David Venable](mailto:dlv@amazon.com) - Tue, 14 Dec 2021 09:15:43 -0600

  efs/remotes/origin/1.2, refs/heads/1.2
  * Updated to log4j-core 2.15.0. Require this version from transitive
    dependencies. (#731)
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

  * Updated to log4j-core 2.16.0 (#740)
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Performance improvements for custom key validation in Event, and replacement with StringUtils.isNumeric (#728)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 13 Dec 2021 20:13:14 -0600


    Remove regex validation for JacksonEvent checkKey and replace with custom
    validation. Replace isNumeric with StrungUtils.isNumeric. Added max key length
    of 2048 (#725)
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __[1.2] Add yum update to data prepper to consume all pkg updates (#715)__

  [Peter Zhu](mailto:zhujiaxi@amazon.com) - Fri, 10 Dec 2021 13:56:16 -0500


    Signed-off-by: Peter Zhu &lt;zhujiaxi@amazon.com&gt;

* __Create a new GrokPrepper instance for each worker thread by annotating it with @SingleThread. Each GrokPrepper needs a single thread for timeouts. Now, each processor thread will have its own extra thread for these timeouts. This should prevent contention when running with a timeout. (#708)__

  [David Venable](mailto:dlv@amazon.com) - Tue, 7 Dec 2021 12:57:14 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Set the version to Data Prepper 1.2.0. (#669)__

  [David Venable](mailto:dlv@amazon.com) - Tue, 30 Nov 2021 14:58:47 -0600

  efs/remotes/origin/1.2, refs/heads/1.2
  Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update data prepper build script with more setups (#671)__

  [Peter Zhu](mailto:zhujiaxi@amazon.com) - Tue, 30 Nov 2021 15:55:51 -0500


    Signed-off-by: Peter Zhu &lt;zhujiaxi@amazon.com&gt;

* __Disable armeria headers (#663)__

  [Steven Bayer](mailto:sbayer55@gmail.com) - Tue, 30 Nov 2021 11:52:31 -0600


    * Disable Armeria server header
    * Converted related tests to assert using Hamcrest testing framework
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Use Netty 4.1.68 which fixes two CVEs: CVE-2021-37136 and CVE-2021-37137. See https://netty.io/news/2021/09/09/4-1-68-Final.html (#661)__

  [David Venable](mailto:dlv@amazon.com) - Mon, 29 Nov 2021 09:49:36 -0600

  efs/remotes/origin/1.2, refs/heads/1.2
  Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated documentation to use OpenSearch Dashboards. #638 (#658)__

  [David Venable](mailto:dlv@amazon.com) - Wed, 24 Nov 2021 15:34:24 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add maven local to publish repos (#635)__

  [Steven Bayer](mailto:sbayer55@gmail.com) - Wed, 24 Nov 2021 10:01:03 -0600


    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added docker helper script for publishing to container registries (#634)__

  [Steven Bayer](mailto:sbayer55@gmail.com) - Wed, 24 Nov 2021 10:00:49 -0600


    * Added docker helper script for publishing to container registries
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

    * Updated feedback in error scenarios
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

    * Change usage examples to be relevant
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Using OpenSearch, OpenSearch Dashboards, and Amazon OpenSearch Service consistently in Data Prepper documentation. (#637)__

  [David Venable](mailto:dlv@amazon.com) - Tue, 23 Nov 2021 11:00:00 -0600

  EAD -&gt; refs/heads/1.2, refs/remotes/origin/1.2
  Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removed bom dependencies from third party dependency report (#631)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Fri, 19 Nov 2021 16:36:36 -0600


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __File source improvements and change to event model (#601)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 18 Nov 2021 11:49:36 -0600


    Refactored file source, added record_type and format configuration options for
    json and plaintext, support for both Event And String
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Std out sink to use event model (#599)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 18 Nov 2021 10:31:07 -0600


    StdOutSink support for both Event and String
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Added gradle task to generate THIRD-PARTY License report (#621)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Wed, 17 Nov 2021 23:49:33 -0600


    * Added gradle task to generate THIRD-PARTY License report
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated Logstash attributes mapper to use template pattern (#617)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Wed, 17 Nov 2021 16:27:44 -0600


    * Updated Logstash attributes mapper to use template pattern
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Support unsigned Maven publication. Set groupId to org.opensearch.dataprepper (#596)__

  [David Venable](mailto:dlv@amazon.com) - Wed, 17 Nov 2021 09:54:30 -0600


    Support unsigned Maven publication. Updated the project groupId to
    org.opensearch.dataprepper. Added some documentation for publishing Maven
    artifacts to the Plugin Development guide. Supports #421
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add a new metric for OpenSearch Sink plugin: bulkRequestSizeBytes (#572)__

  [Han Jiang](mailto:jianghan@amazon.com) - Tue, 16 Nov 2021 15:34:02 -0600


    * Add a new metric for OpenSearch Sink plugin: bulkRequestSizeBytes
    
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;
    
    * Using summary as the instrumenting mechanism for collecting
    bulkRequestSizeBytes metrics
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;

* __Auth and ssl examples and warnings (#603)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Tue, 16 Nov 2021 12:58:30 -0600


    Added warnings for disabled authentication, provided examples for ssl and basic
    auth in getting started guides for log/trace analytics
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Fixed javadocs errors and updated deprecated Gradle dependency configuration (#597)__

  [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Tue, 16 Nov 2021 11:17:48 -0600


    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Refactored logstash converter mapping access modifiers (#595)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Mon, 15 Nov 2021 22:04:48 -0600


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Updated copyright headers for the root project Gradle files, and for Data Prepper API and Data Prepper Core. Contributes to #189. (#569)__

  [David Venable](mailto:dlv@amazon.com) - Mon, 15 Nov 2021 14:49:39 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Document Sink connection configurations: socket_timeout and connect_t… (#563)__

  [Han Jiang](mailto:jianghan@amazon.com) - Mon, 15 Nov 2021 14:33:51 -0600


    * Document Sink connection configurations: socket_timeout and connect_timeout
    
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;
    
    * Re-phrase timeout parameter descriptions to make them clearer to DP users.
    
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;

* __Added integration tests for the Logstash Configuration Converter (#592)__

  [David Venable](mailto:dlv@amazon.com) - Mon, 15 Nov 2021 11:49:05 -0600


    Added integration tests for the LogstashConfigConverter class. These test the
    entire conversion process by converting and comparing the results to expected
    YAML files. Includes some updates to the PipelineDataFlowModel.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Integrated logstash converter into Data Prepper core (#591)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Mon, 15 Nov 2021 10:05:19 -0600


    Integrated logstash converter into Data Prepper core
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added PipelinesDataFlowModel and serialization test (#588)__

  [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Sat, 13 Nov 2021 14:50:19 -0600


    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Fix issues with extra quotes in Logstash converted YAML files. (#587)__

  [David Venable](mailto:dlv@amazon.com) - Sat, 13 Nov 2021 14:45:10 -0600


    Fix issues with extra quotes in Logstash converted YAML files.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __ENH: integrate named captures into grok logstash mapper (#590)__

  [Qi Chen](mailto:qchea@amazon.com) - Sat, 13 Nov 2021 14:38:10 -0600


    Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __[BUG] Grok NPE fix when JacksonEvent.get() returns null (#589)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Sat, 13 Nov 2021 14:37:49 -0600


    Fixed NPE when the key is not found and event returns null
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Added configConverter which converts conf to yaml (#584)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Fri, 12 Nov 2021 21:01:03 -0600


    * Added configConverter which converts conf to yaml
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Named captures conversion to grok pattern_definitions format (#586)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 12 Nov 2021 18:56:54 -0600


    Added GrokNamedCapturesUtil Converter to change to pattern_definitions format
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Minor Gradle improvements to the Logstash configuration project to remove duplicated configurations and add consistency. (#580)__

  [David Venable](mailto:dlv@amazon.com) - Fri, 12 Nov 2021 17:05:37 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Enhancement: GrokLogstashPluginAttributesMapper (#581)__

  [Qi Chen](mailto:qchea@amazon.com) - Fri, 12 Nov 2021 16:40:44 -0600


    * ADD: GrokLogstashPluginAttributesMapper
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: copyright header
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: address PR comments
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __Fixes the main branch from a renaming that happened when two related PRs were merged around the same time. (#583)__

  [David Venable](mailto:dlv@amazon.com) - Fri, 12 Nov 2021 15:45:49 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Grpc Basic auth for Otel trace Source (#570)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 12 Nov 2021 15:27:16 -0600


    Added http_basic auth for grpc otel-trace-source, refactor otel-trace-source to
    use DataPrepperPluginConstructor
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Fixed a NullPointerException which LogstashVisitor was throwing for attributes without a value. (#582)__

  [David Venable](mailto:dlv@amazon.com) - Fri, 12 Nov 2021 15:07:39 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update PipelineModel to fix serialization (#577)__

  [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Fri, 12 Nov 2021 15:04:12 -0600


    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Log analytics getting started (#573)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 12 Nov 2021 14:42:03 -0600


     Updated demo guide for running Data Prepper through Docker, and with FluentBit
    and OpenSearch through the same docker-compose; Created first draft for Getting
    Started Guide with Log Analytics
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Support maps which are not present in the YAML, which Jackson appears to be treating differently from explicit nulls. #568 (#579)__

  [David Venable](mailto:dlv@amazon.com) - Fri, 12 Nov 2021 13:44:57 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for codeowners to repo (#578)__

  [Ryan Bogan](mailto:10944539+ryanbogan@users.noreply.github.com) - Fri, 12 Nov 2021 13:38:28 -0600


    Signed-off-by: Ryan Bogan &lt;rbogan@amazon.com&gt;

* __Added logstash mapper which maps logstash configuration to pipeline m… (#575)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Fri, 12 Nov 2021 13:12:58 -0600


    * Added logstash mapper which maps logstash configuration to pipeline model
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Document OpenSearch Sink configuration parameters number_of_shards and number_of_replicas. (#562)__

  [Han Jiang](mailto:jianghan@amazon.com) - Fri, 12 Nov 2021 12:58:59 -0600


    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;

* __Added instructions to build and run the Docker image locally. (#564)__

  [David Venable](mailto:dlv@amazon.com) - Fri, 12 Nov 2021 12:58:22 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __NPE bug fixes in the DefaultLogstashPluginAttributesMapper (#574)__

  [David Venable](mailto:dlv@amazon.com) - Fri, 12 Nov 2021 12:47:11 -0600


    Bug fixes in the DefaultLogstashPluginAttributesMapper related to null values.
    The LogstashMappingModel now returns empty maps when they are null or absent in
    the YAML. Part of #568.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __switch Path.of to Paths.get (#566)__

  [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 12 Nov 2021 10:31:17 -0600


    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Fix PrepperState javadoc (#567)__

  [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 12 Nov 2021 10:31:01 -0600


    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added LogstashPluginAttributesMapper for custom mapping (#568)__

  [David Venable](mailto:dlv@amazon.com) - Fri, 12 Nov 2021 08:01:49 -0600


    Refactored the DefaultPluginMapper to use LogstashPluginAttributesMapper as the
    mechanism for mapping plugins. This can be configure in the mapping YAML to
    allow detailed mapping configurations. #466
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Corrected the file path for Logstash mapping files. #467 (#565)__

  [David Venable](mailto:dlv@amazon.com) - Thu, 11 Nov 2021 14:20:46 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __added default mapper for mapping logstash config models to data prepper plugin model (#559)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Thu, 11 Nov 2021 13:05:17 -0600


    * added default mapper for mapping logstash config models to plugin model
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added Mapping files for supported Plugins (#535)__

  [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Thu, 11 Nov 2021 12:31:36 -0600


    Added Mapping files for supported Plugins
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;

* __Documenting ism_policy_file parameter in OpenSearch Sink plugin READM… (#553)__

  [Han Jiang](mailto:jianghan@amazon.com) - Thu, 11 Nov 2021 10:30:49 -0600


    * Documenting ism_policy_file parameter in OpenSearch Sink plugin README.md
    
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;
    
    * Re-word the description for parameter: ism_policy_file
    Signed-off-by: Han
    Jiang &lt;jianghan@amazon.com&gt;

* __Added Coding Guidance to the Developer Guide documentation. (#560)__

  [David Venable](mailto:dlv@amazon.com) - Wed, 10 Nov 2021 20:09:21 -0600


    Added Coding Guidance to the Developer Guide documentation.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Turn on authentication of core APIs by default in Docker images using the username &#39;admin&#39; and password &#39;admin&#39;. #312 (#561)__

  [David Venable](mailto:dlv@amazon.com) - Wed, 10 Nov 2021 16:46:07 -0600


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support index-type parameter (#480)__

  [Han Jiang](mailto:jianghan@amazon.com) - Wed, 10 Nov 2021 09:48:48 -0600


    * Support index-type parameter
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;
    
    * Rename IndexType attribute to be more succict and intuitive.
    Signed-off-by:
    Han Jiang &lt;jianghan@amazon.com&gt;
    
    * Add more unit tests and optimize getting value from Optional object
    
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;

* __Support HTTP Basic authentication on the core Data Prepper APIs (#558)__

  [David Venable](mailto:dlv@amazon.com) - Tue, 9 Nov 2021 20:08:05 -0600


    Support HTTP Basic authentication on the core Data Prepper APIs. This uses
    plugin support so that it can be customized if needed. Includes documentation
    for configuring or disabling authentication. #312
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Test: basic grok e2e test (#536)__

  [Qi Chen](mailto:qchea@amazon.com) - Tue, 9 Nov 2021 18:55:39 -0600


    * REF: refactoring existing trace e2e tests
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: github workflow
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * ADD: README
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * FIX: spotless
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * STY: spotless for markdown
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: address PR comments
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: directory name
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: build directory reference
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * TST: e2e grok test logic
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: add e2e basic grok github workflow
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: expose ports at create container
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * doc: README
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: use hasKey
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: remove wildcard imports
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * REF: ApacheLogFaker and its test
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * RNM: basic grok -&gt; basic log
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __Maintenance: add codecov commenter bot (#549)__

  [Qi Chen](mailto:qchea@amazon.com) - Tue, 9 Nov 2021 14:45:42 -0600


    * MAINT: jacoco report in xml, codecov config, action, badge
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * FIX: badge link and codecov.yml
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * STY: spotless
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: remove secret token
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: do not require ci to pass
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: move YAML file into .github
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __integrating event model into log ingestion plugins, updated sink to support both string and event type (#539)__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 9 Nov 2021 14:29:01 -0600


    integrating event model into log ingestion plugins, updated sink to support
    both string and event type
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Added Mapping Model class and supporting Mapping framework (#552)__

  [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Tue, 9 Nov 2021 14:07:53 -0600


    Added Mapping Model class and supporting Mapping framework
    Signed-off-by:
    Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Complete Grok Documentation (#548)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 8 Nov 2021 17:11:38 -0600


    Complete grok documentation with examples
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Warning for disabled SSL with http/otel trace source, document setting up SSL (#537)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 8 Nov 2021 10:28:01 -0600


    Warnings for http/otel trace source when SSL disabled, document setting up SSL
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Support HTTP Basic authentication on the HTTP source plugin. This uses the plugin framework to provide the authentication so that it can be customized without having to change the HTTP Source plugin itself. Resolves #464 (#545)__

  [David Venable](mailto:dlv@amazon.com) - Fri, 5 Nov 2021 13:14:18 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __added ANTLR visitor for parsing Logstash configuration (#506)__

  [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Fri, 5 Nov 2021 11:15:59 -0400


    added visitor to populate Logstash model objects using ANTLR library
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump jackson-dataformat-smile in /data-prepper-plugins/opensearch (#530)__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 4 Nov 2021 22:37:31 -0500


    Bumps
    [jackson-dataformat-smile](https://github.com/FasterXML/jackson-dataformats-binary)
    from 2.12.4 to 2.13.0.
    - [Release
    notes](https://github.com/FasterXML/jackson-dataformats-binary/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-dataformats-binary/compare/jackson-dataformats-binary-2.12.4...jackson-dataformats-binary-2.13.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.dataformat:jackson-dataformat-smile
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Grok prepper metrics (#510)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 4 Nov 2021 15:36:42 -0500


    Added grok metrics and testing for those metrics 
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Updating from Maps to Models in data-prepper-api (#473)__

  [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Thu, 4 Nov 2021 11:41:17 -0500


    * Updated from Maps to Model classes in data-prepper-api using Custom
    Serializer/Deserializer
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Bump jackson-annotations in /data-prepper-plugins/opensearch (#526)__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 3 Nov 2021 10:46:44 -0500


    Bumps [jackson-annotations](https://github.com/FasterXML/jackson) from 2.12.5
    to 2.13.0.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-annotations
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Dependabot upgrades (#520)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 3 Nov 2021 10:13:37 -0500


    Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-raw-prepper (#497)__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 3 Nov 2021 09:41:30 -0500


    Bumps [protobuf-java-util](https://github.com/protocolbuffers/protobuf) from
    3.18.1 to 3.19.1.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/master/generate_changelog.py)
    
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.18.1...v3.19.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump awaitility in /data-prepper-plugins/otel-trace-raw-prepper (#495)__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 3 Nov 2021 09:40:45 -0500


    Bumps [awaitility](https://github.com/awaitility/awaitility) from 4.1.0 to
    4.1.1.
    - [Release notes](https://github.com/awaitility/awaitility/releases)
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.1.0...awaitility-4.1.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Maintenance: refactoring existing e2e tests on trace data ingestion (#512)__

  [Qi Chen](mailto:qchea@amazon.com) - Tue, 2 Nov 2021 20:28:06 -0500


    * REF: refactoring existing trace e2e tests
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: github workflow
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * ADD: README
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * FIX: spotless
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * STY: spotless for markdown
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: address PR comments
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: directory name
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: build directory reference
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __Expanded plugin constructor capabilities (#481)__

  [David Venable](mailto:dlv@amazon.com) - Tue, 2 Nov 2021 17:46:35 -0500


    Support more constructors for plugins by adding the
    DataPrepperPluginConstructor annotation. This is the preferred constructor. If
    no other constructor is available for a plugin, use the no-op constructor.
    Updated the HTTPSource plugin to use this capability to receive both a
    configuration model and PluginMetrics via the constructor. For a single
    parameter, un-annotated constructor in plugins, the only supported parameter is
    once again PluginSetting.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump reflections from 0.10.1 to 0.10.2 in /data-prepper-core (#488)__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 2 Nov 2021 16:15:33 -0500


    Bumps [reflections](https://github.com/ronmamo/reflections) from 0.10.1 to
    0.10.2.
    - [Release notes](https://github.com/ronmamo/reflections/releases)
    - [Commits](https://github.com/ronmamo/reflections/compare/0.10.1...0.10.2)
    
    ---
    updated-dependencies:
    - dependency-name: org.reflections:reflections
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump reflections from 0.10.1 to 0.10.2 in /data-prepper-plugins/common (#487)__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 2 Nov 2021 16:04:33 -0500


    Bumps [reflections](https://github.com/ronmamo/reflections) from 0.10.1 to
    0.10.2.
    - [Release notes](https://github.com/ronmamo/reflections/releases)
    - [Commits](https://github.com/ronmamo/reflections/compare/0.10.1...0.10.2)
    
    ---
    updated-dependencies:
    - dependency-name: org.reflections:reflections
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __implementing trace models and extending event implementation (#477)__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 2 Nov 2021 11:23:04 -0500


    * implementing trace models and extending event implementation
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-source__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 2 Nov 2021 11:01:52 -0500


    Bumps [protobuf-java-util](https://github.com/protocolbuffers/protobuf) from
    3.18.0 to 3.19.1.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/master/generate_changelog.py)
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.18.0...v3.19.1)
    
    --- updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Added Project Resources to the README.md. This is loosely based on OpenSearch&#39;s Project Resources.__

  [David Venable](mailto:dlv@amazon.com) - Tue, 2 Nov 2021 11:00:56 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Updated the MAINTAINERS.md__

  [David Venable](mailto:dlv@amazon.com) - Mon, 1 Nov 2021 16:51:35 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Log ingestion demo guide (#485)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 1 Nov 2021 11:17:07 -0500


    Added log ingestion demo guide
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Bump awaitility in /data-prepper-plugins/peer-forwarder__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 1 Nov 2021 10:37:56 -0500


    Bumps [awaitility](https://github.com/awaitility/awaitility) from 4.1.0 to
    4.1.1.
    - [Release notes](https://github.com/awaitility/awaitility/releases)
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.1.0...awaitility-4.1.1)
    
    --- updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Ssl by default for Data Prepper server through Docker (#476)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 29 Oct 2021 16:59:18 -0500


    Added default data-prepper-config with tls for docker, documented how users can
    create their own tls setup with data prepper
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Enable OpenSearch Sink to go through a forward http proxy to communicate with an OpenSearch or ODFE server. (#479)__

  [Han Jiang](mailto:jianghan@amazon.com) - Thu, 28 Oct 2021 15:56:16 -0500


    * Enable OpenSearch Sink to go through a forward http proxy to communicate with
    a OpenSearch or ODFE server.
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;
    
    * Add a check on proxy server port number
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;
    
    * Have a default value for ConnectionConfiguration.Builder&#39;s proxy attribute to
    avoid NPE
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;
    
    * Add a new unit test to cover a proxy host string with an explicit scheme:
    http
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;

* __Allow plugins to define configuration classes.__

  [David Venable](mailto:dlv@amazon.com) - Thu, 28 Oct 2021 11:37:21 -0500

  efs/remotes/personal/main
  Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __add reference to jsonPointer__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 26 Oct 2021 10:00:20 -0500


    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;


* __refactoring jackson event to support JsonPointer format, resolves #450__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 26 Oct 2021 10:00:20 -0500


    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;


* __Added a short guide to migrating to OpenSearch Data Prepper from Open Distro Data Prepper.__

  [David Venable](mailto:dlv@amazon.com) - Tue, 26 Oct 2021 09:15:29 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __moved packages to org.opensearch.dataprepper from com.amazon.dataprepper__

  [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 25 Oct 2021 12:55:15 -0500


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;


* __added static method for builder and null checks__

  [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 25 Oct 2021 12:55:15 -0500


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;


* __updated model classes to be immutable using builder pattern__

  [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 25 Oct 2021 12:55:15 -0500


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;


* __added javadocs to logstash model classes__

  [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 25 Oct 2021 12:55:15 -0500


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;


* __removed build.gradle and updated class names__

  [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 25 Oct 2021 12:55:15 -0500


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;


* __added logstash model for converter__

  [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 25 Oct 2021 12:55:15 -0500


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;


* __implementing log model as part of #436 (#463)__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Fri, 22 Oct 2021 13:34:34 -0500


    * implementing log model as part of #436
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __For custom index type, create ISM policy on OpenSearch if ism_policy_file parameter is set. (#433)__

  [Han Jiang](mailto:jianghan@amazon.com) - Thu, 21 Oct 2021 10:44:51 -0500


    * For custom index type, create ISM policy on OpenSearch if ism_policy_file
    parameter is set.
     Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;
    
    * Remove index template from test-custom-index-policy-file.json so IT can pass
    
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;
    
    * To be backward compatible, drop ISM template from policy file if the field is
    found invalid.
    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;

* __To be backward compatible, drop ISM template from policy file if the field is found invalid. Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;__

  [Han Jiang](mailto:jianghan@amazon.com) - Thu, 21 Oct 2021 10:43:44 -0500




* __Remove index template from test-custom-index-policy-file.json so IT can pass Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;__

  [Han Jiang](mailto:jianghan@amazon.com) - Thu, 21 Oct 2021 10:43:44 -0500




* __For custom index type, create ISM policy on OpenSearch if ism_policy_file parameter is set.__

  [Han Jiang](mailto:jianghan@amazon.com) - Thu, 21 Oct 2021 10:43:44 -0500


    Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;


* __Merge in v1.1.x. This resolves #350 and resolves #357.__

  [David Venable](mailto:dlv@amazon.com) - Wed, 20 Oct 2021 18:39:14 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Added support for timeout millis, refactored unit tests to createObje… (#449)__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 20 Oct 2021 15:54:24 -0500


    * Added support for timeout millis, refactored unit tests to
    createObjectUnderTest structure
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;
    
    * remove redundant e.printStackTrace() and pass lambda directly to
    runWithTimeout
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;
    
    * Update README to include timeoutmillis, removed timeoutMillis from
    awaitTermination
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Updated existing plugins to use the new pluginTypeForDocGen property in the DataPrepperPlugin annotation. Resolves
  #322.__

  [David Venable](mailto:dlv@amazon.com) - Wed, 20 Oct 2021 13:51:53 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __addressing pr feedback__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 19 Oct 2021 14:28:17 -0500


    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;


* __provides first implementation of event models and closes #434__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 19 Oct 2021 14:28:17 -0500


    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;


* __Fix setting__

  [Marek Kadek](mailto:kadek.marek@gmail.com) - Tue, 19 Oct 2021 12:35:36 -0500


    Signed-off-by: Marek Kadek &lt;kadek.marek@gmail.com&gt;


* __Updated the last few projects which used ElasticSearch. With this commit, no Gradle projects should be using ElasticSearch. Use OpenSearch instead of Elastic in Java files, except for in links.__

  [David Venable](mailto:dlv@amazon.com) - Mon, 18 Oct 2021 09:41:54 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Enhancement: use new buffer api in http source (#415)__

  [Qi Chen](mailto:qchea@amazon.com) - Mon, 18 Oct 2021 09:33:24 -0500




* __Bump jackson-dataformat-cbor in /data-prepper-plugins/opensearch__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 14 Oct 2021 12:06:08 -0500


    Bumps
    [jackson-dataformat-cbor](https://github.com/FasterXML/jackson-dataformats-binary)
    from 2.12.5 to 2.13.0.
    - [Release
    notes](https://github.com/FasterXML/jackson-dataformats-binary/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-dataformats-binary/compare/jackson-dataformats-binary-2.12.5...jackson-dataformats-binary-2.13.0)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.dataformat:jackson-dataformat-cbor
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump joda-time in /data-prepper-plugins/opensearch__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 14 Oct 2021 12:06:08 -0500


    Bumps [joda-time](https://github.com/JodaOrg/joda-time) from 2.10.10 to
    2.10.12.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    -
    [Changelog](https://github.com/JodaOrg/joda-time/blob/master/RELEASE-NOTES.txt)
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.10.10...v2.10.12)
    
    --- updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 14 Oct 2021 12:06:08 -0500


    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.11.18 to
    1.11.20.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.11.18...byte-buddy-1.11.20)
    
    --- updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump byte-buddy-agent in /data-prepper-plugins/opensearch__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 14 Oct 2021 12:06:08 -0500


    Bumps [byte-buddy-agent](https://github.com/raphw/byte-buddy) from 1.11.18 to
    1.11.20.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.11.18...byte-buddy-1.11.20)
    
    --- updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy-agent
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump reflections from 0.9.12 to 0.10.1 in /data-prepper-plugins/common__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 14 Oct 2021 12:06:08 -0500


    Bumps [reflections](https://github.com/ronmamo/reflections) from 0.9.12 to
    0.10.1.
    - [Release notes](https://github.com/ronmamo/reflections/releases)
    - [Commits](https://github.com/ronmamo/reflections/compare/0.9.12...0.10.1)
    
    --- updated-dependencies:
    - dependency-name: org.reflections:reflections
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump com.diffplug.spotless from 5.12.4 to 5.17.0__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 14 Oct 2021 12:06:08 -0500


    Bumps com.diffplug.spotless from 5.12.4 to 5.17.0.
    
    --- updated-dependencies:
    - dependency-name: com.diffplug.spotless
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-databind in /data-prepper-plugins/opensearch__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 14 Oct 2021 12:06:08 -0500


    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.12.5 to
    2.13.0.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump reflections from 0.9.12 to 0.10.1 in /data-prepper-core__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 14 Oct 2021 12:06:08 -0500


    Bumps [reflections](https://github.com/ronmamo/reflections) from 0.9.12 to
    0.10.1.
    - [Release notes](https://github.com/ronmamo/reflections/releases)
    - [Commits](https://github.com/ronmamo/reflections/compare/0.9.12...0.10.1)
    
    --- updated-dependencies:
    - dependency-name: org.reflections:reflections
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Spotless apply check__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 13 Oct 2021 12:54:54 -0500


    Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Updated maintainer github IDs__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 13 Oct 2021 12:54:54 -0500


    Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Fixed Spotless Violation__

  [Lane Holloway](mailto:laneholl@amazon.com) - Wed, 13 Oct 2021 09:20:42 -0500


    Signed-off-by: Lane Holloway &lt;laneholl@amazon.com&gt;


* __Updating MAINTAINERS.md to match who is working on the project.__

  [Lane Holloway](mailto:laneholl@amazon.com) - Wed, 13 Oct 2021 09:20:42 -0500


    Signed-off-by: Lane Holloway &lt;laneholl@amazon.com&gt;


* __reducing coupling with jackson on our model interface__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 12 Oct 2021 19:56:02 -0500


    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;


* __addressing review comments, adding @since, adjusting return types and increasing dependency versions__

  [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 12 Oct 2021 19:56:02 -0500


    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;


* __new internal model interfaces - #405__

  [cmanning09](mailto:cmanning09@users.noreply.github.com) - Tue, 12 Oct 2021 19:56:02 -0500


    Signed-off-by: cmanning09 &lt;cmanning09@users.noreply.github.com&gt;


* __Use a Function to determine the number of instances to load in PluginFactory. Changed PluginProvider to return Optional&lt;Class&gt; instead of null. Added TODO to test constructor.__

  [David Venable](mailto:dlv@amazon.com) - Tue, 12 Oct 2021 16:46:37 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Updated the JavaDocs for DataPrepperPlugin and PluginType so that they outline the migration path.__

  [David Venable](mailto:dlv@amazon.com) - Tue, 12 Oct 2021 16:46:37 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Added the new Plugin class design, and use these classes to generate the pipeline. Deprecated the old plugin classes. Fixed up some tests which were being touched anyway. #322__

  [David Venable](mailto:dlv@amazon.com) - Tue, 12 Oct 2021 16:46:37 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Set the version to 1.2.0-SNAPSHOT since our next release on this branch will be 1.2.0.__

  [David Venable](mailto:dlv@amazon.com) - Tue, 12 Oct 2021 16:37:12 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Make inner classes static so their members are not exposed to the enclosing classes. Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;__

  [Han Jiang](mailto:jianghan@amazon.com) - Tue, 12 Oct 2021 13:09:01 -0500




* __updating jaeger-hotrod readme with OpenSearch migration missed in #356__

  [cmanning09](mailto:cmanning09@users.noreply.github.com) - Tue, 12 Oct 2021 09:01:46 -0500


    Signed-off-by: cmanning09 &lt;cmanning09@users.noreply.github.com&gt;


* __Privatize IndexManager sub-classes and ISM policy management classes Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;__

  [Han Jiang](mailto:jianghan@amazon.com) - Mon, 11 Oct 2021 20:36:03 -0500




* __Bump assertj-core in /data-prepper-plugins/otel-trace-source__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 11 Oct 2021 13:28:57 -0500


    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.20.2 to
    3.21.0.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.20.2...assertj-core-3.21.0)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-trace-raw-prepper__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 11 Oct 2021 11:35:40 -0500


    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.20.2 to
    3.21.0.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.20.2...assertj-core-3.21.0)
    
    --- updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-dataformat-yaml from 2.12.5 to 2.13.0__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 11 Oct 2021 11:16:14 -0500


    Bumps
    [jackson-dataformat-yaml](https://github.com/FasterXML/jackson-dataformats-text)
    from 2.12.5 to 2.13.0.
    - [Release
    notes](https://github.com/FasterXML/jackson-dataformats-text/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-dataformats-text/compare/jackson-dataformats-text-2.12.5...jackson-dataformats-text-2.13.0)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.dataformat:jackson-dataformat-yaml
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Removed break on match boolean, updated config names for overwrite, target, and patterns_dir__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 8 Oct 2021 16:42:43 -0500


    Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __MAINT: sync os version in e2e__

  [qchea](mailto:qchea@amazon.com) - Fri, 8 Oct 2021 15:41:17 -0500

  efs/heads/testtest
  Signed-off-by: qchea &lt;qchea@amazon.com&gt;


* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-raw-prepper__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 8 Oct 2021 15:36:03 -0500


    Bumps [protobuf-java-util](https://github.com/protocolbuffers/protobuf) from
    3.17.3 to 3.18.1.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/master/generate_changelog.py)
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.17.3...v3.18.1)
    
    --- updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __README: Summary -&gt; Distribution Summary__

  [qchea](mailto:qchea@amazon.com) - Fri, 8 Oct 2021 15:15:44 -0500


    Signed-off-by: qchea &lt;qchea@amazon.com&gt;


* __MAINT: metrics name and variable names__

  [qchea](mailto:qchea@amazon.com) - Fri, 8 Oct 2021 15:15:44 -0500


    Signed-off-by: qchea &lt;qchea@amazon.com&gt;


* __DOC: Metrics in README__

  [qchea](mailto:qchea@amazon.com) - Fri, 8 Oct 2021 15:15:44 -0500


    Signed-off-by: qchea &lt;qchea@amazon.com&gt;


* __MAINT: more test cases in HTTPSourceTests__

  [qchea](mailto:qchea@amazon.com) - Fri, 8 Oct 2021 15:15:44 -0500


    Signed-off-by: qchea &lt;qchea@amazon.com&gt;


* __TST: unit tests with mock on LogHTTPServiceTest__

  [qchea](mailto:qchea@amazon.com) - Fri, 8 Oct 2021 15:15:44 -0500


    Signed-off-by: qchea &lt;qchea@amazon.com&gt;


* __ADD: all counters, timers and summary__

  [qchea](mailto:qchea@amazon.com) - Fri, 8 Oct 2021 15:15:44 -0500


    Signed-off-by: qchea &lt;qchea@amazon.com&gt;


* __Address PR comments, including patterns_dir change from regex to glob matching__

  [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 8 Oct 2021 13:46:29 -0500


    Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Small update to the bug template.__

  [Lane Holloway](mailto:laneholl@amazon.com) - Fri, 8 Oct 2021 11:54:44 -0500


    Signed-off-by: Lane Holloway &lt;laneholl@amazon.com&gt;


* __Improved the issue templates - added untriaged to Features and removed Beta from bugs.__

  [David Venable](mailto:dlv@amazon.com) - Fri, 8 Oct 2021 10:02:56 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-source__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 8 Oct 2021 08:08:45 -0500


    Bumps [protobuf-java-util](https://github.com/protocolbuffers/protobuf) from
    3.17.3 to 3.18.0.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/master/generate_changelog.py)
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.17.3...v3.18.0)
    
    --- updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump slf4j-api from 1.7.30 to 1.7.32__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 8 Oct 2021 07:51:14 -0500


    Bumps [slf4j-api](https://github.com/qos-ch/slf4j) from 1.7.30 to 1.7.32.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.30...v_1.7.32)
    
    --- updated-dependencies:
    - dependency-name: org.slf4j:slf4j-api
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Refactoring ISM related methods out of index manager using strategy pattern Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;__

  [Han Jiang](mailto:jianghan@amazon.com) - Thu, 7 Oct 2021 22:55:12 -0500




* __Maintenance: Fixed the GitHub actions for ODFE. Fixed the OpenSearch test for ODFE by reverting to making a manual request to the _cluster/health API. (#393)__

  [David Venable](mailto:dlv@amazon.com) - Thu, 7 Oct 2021 19:56:25 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump kotlin-stdlib in /data-prepper-plugins/mapdb-prepper-state__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 7 Oct 2021 17:19:45 -0500


    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.5.30 to
    1.5.31.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.5.31/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.5.30...v1.5.31)
    
    --- updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Build using OpenSearch 1.1.0, including using it as a client. Run integration tests against both OpenSearch 1.0.1 and 1.1.0. Consolidated the ODFE tests.__

  [David Venable](mailto:dlv@amazon.com) - Thu, 7 Oct 2021 17:01:20 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Bump kotlin-stdlib-common from 1.5.30 to 1.5.31__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 7 Oct 2021 16:21:20 -0500


    Bumps [kotlin-stdlib-common](https://github.com/JetBrains/kotlin) from 1.5.30
    to 1.5.31.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.5.31/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.5.30...v1.5.31)
    
    --- updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Added checkstyle to the Data Prepper project. This is currently configured with a subset of what we hope to have, along with some code changes to support the new restrictions.__

  [David Venable](mailto:dlv@amazon.com) - Thu, 7 Oct 2021 15:54:15 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Move verifyNoInteractions for break_on_match_true test__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 6 Oct 2021 17:03:05 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Added unit/integration tests for additional grok features__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 6 Oct 2021 17:03:04 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Maintenance: add default retry rule for Armeria client (#375)__

  [Qi Chen](mailto:qchea@amazon.com) - Wed, 6 Oct 2021 16:18:33 -0500


    Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __Feature: enable TLS/SSL through local filepath in http source (#359)__

  [Qi Chen](mailto:qchea@amazon.com) - Wed, 6 Oct 2021 13:37:04 -0500


    * ENH: ssl config parameters and tests
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: comment string
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * REF: server SSLCertProvider into common plugin
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * TST: ssl enabled server
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: TODO comment
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: update README
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * STY: rename ssl related parameters and variables
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * ENH: ssl config parameters and tests
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: comment string
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * REF: server SSLCertProvider into common plugin
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * TST: ssl enabled server
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: TODO comment
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: update README
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * STY: rename ssl related parameters and variables
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __Bump guava in /data-prepper-plugins/otel-trace-raw-prepper__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 6 Oct 2021 13:35:00 -0500


    Bumps [guava](https://github.com/google/guava) from 30.1.1-jre to 31.0.1-jre.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/commits)
    
    --- updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Enhancement: add throttling service (#325)__

  [Qi Chen](mailto:qchea@amazon.com) - Wed, 6 Oct 2021 09:37:29 -0500


    * FEAT: load throttling service and add test cases
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * REF: URI path
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: comment string
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: DEFAULT_LOG_INGEST_URI
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: clear up request after throttling
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * STY: spotless
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: assertThrows exception
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __Opensearch Sink: refactor all index related operations into IndexManager classes for easier future extension Signed-off-by: Han Jiang &lt;jianghan@amazon.com&gt;__

  [Han Jiang](mailto:jianghan@amazon.com) - Tue, 5 Oct 2021 18:28:18 -0500




* __Added a new GitHub workflow to check the Developer Certificate of Origin Check. #326__

  [David Venable](mailto:dlv@amazon.com) - Tue, 5 Oct 2021 15:57:20 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Maintainance: updated jaeger-hotrod example to use latest opensearch instead of opendistro for elasticsearch (#356)__

  [Arunachalam Lakshmanan](mailto:arunachalam.l@gmail.com) - Tue, 5 Oct 2021 13:23:31 -0500


    Authored-by: Arun Lakshmanan &lt;arnlaksh@amazon.com&gt;

* __Start testing additional features__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Tue, 5 Oct 2021 09:44:40 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Use Simple Pipeline instead of Tutorial.__

  [David Venable](mailto:dlv@amazon.com) - Mon, 4 Oct 2021 12:08:53 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Correcting relative paths in the docs directory.__

  [David Venable](mailto:dlv@amazon.com) - Mon, 4 Oct 2021 12:08:53 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Moved the docs/readme directory to simply docs.__

  [David Venable](mailto:dlv@amazon.com) - Mon, 4 Oct 2021 12:08:53 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Refactored the documentation to provide a Getting Started guide for Data Prepper. This change provides a strong split between getting started as a user and as a developer to clarify both processes. Additionally, I combined the Trace Analytics overview and setup into one main Trace Analytics page. With these changes, I also removed the Table of Contents from the main README.md in favor of small overviews which provide some guidance on what these different pages provide.__

  [David Venable](mailto:dlv@amazon.com) - Mon, 4 Oct 2021 12:08:53 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Use Mockito 3.* instead of Securemock 1.2 in order to be able to mock final classes.__

  [Han Jiang](mailto:jianghan@amazon.com) - Mon, 4 Oct 2021 11:49:16 -0500




* __Feature: new Buffer::writeAll API (#320)__

  [Qi Chen](mailto:qchea@amazon.com) - Mon, 4 Oct 2021 09:32:21 -0500




* __Fixed typo__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 1 Oct 2021 15:07:04 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Use singletonList for matchConfig__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 1 Oct 2021 15:03:06 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __add final, comment on null rawrequest__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 1 Oct 2021 12:01:21 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Remove anyString() in mock__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 30 Sep 2021 16:40:22 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Address more PR comments and begin README__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 30 Sep 2021 16:34:17 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Addressing PR comments__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 30 Sep 2021 11:54:11 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Cleaned up a little__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 29 Sep 2021 12:45:54 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Integration tests created that use java-grok library__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Tue, 28 Sep 2021 18:00:59 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Features: Initial http source (#309)__

  [Qi Chen](mailto:qchea@amazon.com) - Tue, 28 Sep 2021 13:22:44 -0500




* __Unit tests__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Tue, 28 Sep 2021 10:17:23 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Beginning testing of Grok Prepper class__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 27 Sep 2021 15:48:29 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Generics for PluginSetting and addressing other comments__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 22 Sep 2021 19:10:43 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Remove suppressed warnings and refactor String List check__

  [graytaylor0](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 22 Sep 2021 10:35:12 -0500


    Signed-off-by: graytaylor0 &lt;33740195+graytaylor0@users.noreply.github.com&gt;


* __Grok Prepper Configuration and Boilerplate__

  [graytaylor0](mailto:graytaylor0@gmail.com) - Tue, 21 Sep 2021 11:44:18 -0500


    Signed-off-by: graytaylor0 &lt;graytaylor0@gmail.com&gt;


* __Maintenance: specifies JUnit 5 in subprojects (#292)__

  [Qi Chen](mailto:qchea@amazon.com) - Mon, 20 Sep 2021 18:06:26 -0500


    * Maintenance: specifies JUnit 5 in subprojects except coreProjects
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;

* __Bump me.champeau.gradle.jmh from 0.5.0 to 0.5.3__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 15 Sep 2021 18:20:39 -0500


    Bumps me.champeau.gradle.jmh from 0.5.0 to 0.5.3.
    
    --- updated-dependencies:
    - dependency-name: me.champeau.gradle.jmh
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump de.undercouch.download from 4.1.1 to 4.1.2__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 14 Sep 2021 18:41:32 -0500


    Bumps de.undercouch.download from 4.1.1 to 4.1.2.
    
    --- updated-dependencies:
    - dependency-name: de.undercouch.download
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump kotlin-stdlib from 1.5.21 to 1.5.30__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 14 Sep 2021 18:37:57 -0500


    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.5.21 to
    1.5.30.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.5.21...v1.5.30)
    
    --- updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump kotlin-stdlib-common in /data-prepper-plugins/mapdb-prepper-state__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 14 Sep 2021 17:21:51 -0500


    Bumps [kotlin-stdlib-common](https://github.com/JetBrains/kotlin) from 1.5.21
    to 1.5.30.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.5.21...v1.5.30)
    
    --- updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Configure dependabot to also include the root project.__

  [David Venable](mailto:dlv@amazon.com) - Tue, 14 Sep 2021 17:15:28 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Synchronize dependency versions for Micrometer, Jackson, AWS Java SDK v1, and AWS Java SDK v2 by using Maven BOM dependencies.__

  [David Venable](mailto:dlv@amazon.com) - Tue, 14 Sep 2021 17:15:28 -0500


    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


* __Bump aws-java-sdk-s3 in /data-prepper-plugins/otel-trace-source (#275)__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 14 Sep 2021 15:43:36 -0500


    Bumps [aws-java-sdk-s3](https://github.com/aws/aws-sdk-java) from 1.12.43 to
    1.12.67.
    - [Release notes](https://github.com/aws/aws-sdk-java/releases)
    - [Changelog](https://github.com/aws/aws-sdk-java/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/aws/aws-sdk-java/compare/1.12.43...1.12.67)
    
    ---
    updated-dependencies:
    - dependency-name: com.amazonaws:aws-java-sdk-s3
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump aws-java-sdk-core in /data-prepper-plugins/opensearch__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 20:49:09 -0500


    Bumps [aws-java-sdk-core](https://github.com/aws/aws-sdk-java) from 1.12.43 to
    1.12.67.
    - [Release notes](https://github.com/aws/aws-sdk-java/releases)
    - [Changelog](https://github.com/aws/aws-sdk-java/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/aws/aws-sdk-java/compare/1.12.43...1.12.67)
    
    --- updated-dependencies:
    - dependency-name: com.amazonaws:aws-java-sdk-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump aws-java-sdk-acm in /data-prepper-plugins/peer-forwarder__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 20:44:38 -0500


    Bumps [aws-java-sdk-acm](https://github.com/aws/aws-sdk-java) from 1.12.43 to
    1.12.59.
    - [Release notes](https://github.com/aws/aws-sdk-java/releases)
    - [Changelog](https://github.com/aws/aws-sdk-java/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/aws/aws-sdk-java/compare/1.12.43...1.12.59)
    
    --- updated-dependencies:
    - dependency-name: com.amazonaws:aws-java-sdk-acm
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump httpclient in /data-prepper-plugins/opensearch__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 20:35:38 -0500


    Bumps httpclient from 4.5.10 to 4.5.13.
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump micrometer-core in /data-prepper-plugins/otel-trace-group-prepper__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 20:28:04 -0500


    Bumps [micrometer-core](https://github.com/micrometer-metrics/micrometer) from
    1.7.2 to 1.7.3.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.7.2...v1.7.3)
    
    --- updated-dependencies:
    - dependency-name: io.micrometer:micrometer-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump micrometer-registry-prometheus in /data-prepper-core__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 18:45:00 -0500


    Bumps
    [micrometer-registry-prometheus](https://github.com/micrometer-metrics/micrometer)
    from 1.6.5 to 1.7.3.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.6.5...v1.7.3)
    
    --- updated-dependencies:
    - dependency-name: io.micrometer:micrometer-registry-prometheus
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump micrometer-core in /data-prepper-plugins/service-map-stateful__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 16:33:29 -0500


    Bumps [micrometer-core](https://github.com/micrometer-metrics/micrometer) from
    1.7.2 to 1.7.3.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.7.2...v1.7.3)
    
    --- updated-dependencies:
    - dependency-name: io.micrometer:micrometer-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump micrometer-registry-cloudwatch2 in /data-prepper-core__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 16:24:45 -0500


    Bumps
    [micrometer-registry-cloudwatch2](https://github.com/micrometer-metrics/micrometer)
    from 1.7.2 to 1.7.3.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.7.2...v1.7.3)
    
    --- updated-dependencies:
    - dependency-name: io.micrometer:micrometer-registry-cloudwatch2
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump micrometer-core from 1.6.5 to 1.7.3 in /data-prepper-core__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 16:01:31 -0500


    Bumps [micrometer-core](https://github.com/micrometer-metrics/micrometer) from
    1.6.5 to 1.7.3.
    - [Release notes](https://github.com/micrometer-metrics/micrometer/releases)
    -
    [Commits](https://github.com/micrometer-metrics/micrometer/compare/v1.6.5...v1.7.3)
    
    --- updated-dependencies:
    - dependency-name: io.micrometer:micrometer-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-databind in /data-prepper-plugins/otel-trace-source__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 14:41:24 -0500


    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.12.4 to
    2.12.5.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-dataformat-yaml in /data-prepper-plugins/otel-trace-source__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 14:24:13 -0500


    Bumps
    [jackson-dataformat-yaml](https://github.com/FasterXML/jackson-dataformats-text)
    from 2.12.4 to 2.12.5.
    - [Release
    notes](https://github.com/FasterXML/jackson-dataformats-text/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-dataformats-text/compare/jackson-dataformats-text-2.12.4...jackson-dataformats-text-2.12.5)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.dataformat:jackson-dataformat-yaml
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-dataformat-yaml__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 14:12:06 -0500


    Bumps
    [jackson-dataformat-yaml](https://github.com/FasterXML/jackson-dataformats-text)
    from 2.12.4 to 2.12.5.
    - [Release
    notes](https://github.com/FasterXML/jackson-dataformats-text/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-dataformats-text/compare/jackson-dataformats-text-2.12.4...jackson-dataformats-text-2.12.5)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.dataformat:jackson-dataformat-yaml
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-databind in /data-prepper-plugins/service-map-stateful__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 13:58:32 -0500


    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.12.4 to
    2.12.5.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-dataformat-yaml from 2.12.4 to 2.12.5 in /data-prepper-core__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 13:58:11 -0500


    Bumps
    [jackson-dataformat-yaml](https://github.com/FasterXML/jackson-dataformats-text)
    from 2.12.4 to 2.12.5.
    - [Release
    notes](https://github.com/FasterXML/jackson-dataformats-text/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-dataformats-text/compare/jackson-dataformats-text-2.12.4...jackson-dataformats-text-2.12.5)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.dataformat:jackson-dataformat-yaml
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-databind in /data-prepper-plugins/otel-trace-raw-prepper__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 13:57:49 -0500


    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.12.4 to
    2.12.5.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-dataformat-yaml__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 12:14:45 -0500


    Bumps
    [jackson-dataformat-yaml](https://github.com/FasterXML/jackson-dataformats-text)
    from 2.12.4 to 2.12.5.
    - [Release
    notes](https://github.com/FasterXML/jackson-dataformats-text/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-dataformats-text/compare/jackson-dataformats-text-2.12.4...jackson-dataformats-text-2.12.5)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.dataformat:jackson-dataformat-yaml
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-databind in /data-prepper-plugins/common__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 12:14:06 -0500


    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.12.4 to
    2.12.5.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-dataformat-yaml in /data-prepper-plugins/common__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 09:28:08 -0500


    Bumps
    [jackson-dataformat-yaml](https://github.com/FasterXML/jackson-dataformats-text)
    from 2.12.4 to 2.12.5.
    - [Release
    notes](https://github.com/FasterXML/jackson-dataformats-text/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-dataformats-text/compare/jackson-dataformats-text-2.12.4...jackson-dataformats-text-2.12.5)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.dataformat:jackson-dataformat-yaml
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;

* __Bump jackson-databind in /data-prepper-plugins/otel-trace-group-prepper__

  [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 13 Sep 2021 09:27:44 -0500


    Bumps [jackson-databind](https://github.com/FasterXML/jackson) from 2.12.4 to
    2.12.5.
    - [Release notes](https://github.com/FasterXML/jackson/releases)
    - [Commits](https://github.com/FasterXML/jackson/commits)
    
    --- updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-databind
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
