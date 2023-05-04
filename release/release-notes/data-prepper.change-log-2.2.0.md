
* __Updated plugin names for otel plugins (#2526)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 20 Apr 2023 10:54:43 -0500
    
    EAD -&gt; refs/heads/2.2-change-log, refs/remotes/upstream/main, refs/remotes/origin/main, refs/remotes/origin/HEAD, refs/heads/main
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

* __Generated THIRD-PARTY file for bc75494 (#2517)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 18 Apr 2023 09:28:46 -0500
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: asifsmohammed
    &lt;asifsmohammed@users.noreply.github.com&gt;

* __Change the behavior of the CSV codec in the S3 source to fail when it is unable to parse CSV rows. Resolves #2512. (#2513)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 17 Apr 2023 21:10:26 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Allow deprecated plugin names  (#2508)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Mon, 17 Apr 2023 21:10:05 -0500
    
    
    Allow deprecated plugin names and update otel plugin names
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __removing event handle from dlq object (#2510)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Mon, 17 Apr 2023 17:59:24 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __end_to_end_acknowledgements option name change (#2486)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 17 Apr 2023 15:51:31 -0500
    
    
    Rename end_to_end_acknowledgements to acknowledgements and support alias
    acknowledgments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Update RCF  maven repository to latest (#2507)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 17 Apr 2023 14:26:12 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __FileSink updates: Remove call to initialize(), adds FileSinkConfig, test updates (#2475)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 17 Apr 2023 09:42:12 -0500
    
    
    Removes an unnecessary call to initialize() in the FileSink constructor.
    Updates FileSink to use a FileSinkConfig. Updates FileSink tests.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Properly shutdown the log_generator plugin correctly. It was preventing Data Prepper from shutting down. (#2494)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 14 Apr 2023 13:25:36 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adding iam role arn validation to s3 source and open search sink configs (#2472)__

    [roshan-dongre](mailto:roshan-dongre@users.noreply.github.com) - Fri, 14 Apr 2023 09:49:57 -0500
    
    
    * adding iam role arn validation to s3 source and open search sink configs
     Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;

* __Move backoff strategy to BufferAccumulator (#2481)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Thu, 13 Apr 2023 20:21:51 -0500
    
    
    * Move backoff strategy to BufferAccumulator
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix metric reporting for batching
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix unit test
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;

* __Fix warning message when Open Search Sink fails to initialize (#2482)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 13 Apr 2023 17:11:11 -0500
    
    
    Fix warning message when Open Search Sink fails to initialize
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Fixes an issue with the end-to-end acknowledgements where the the scheduled monitor thread holds a user thread and prevents Data Prepper from shutting down correctly. The monitor now runs in a dedicated daemon thread and the callback methods are submitted to a distinct executor service with a lower bound of available threads. Includes various test improvements as well. (#2483)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 13 Apr 2023 15:23:47 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add End to End acknowledgement support for S3 source (#2465)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 12 Apr 2023 18:30:13 -0500
    
    
    Add End to End acknowledgement support for S3 source
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fix cannot start databaseService.py with Trace Analytics Sample App (#2477) (#2478)__

    [Toby Lam](mailto:me@livekn.com) - Wed, 12 Apr 2023 18:26:05 -0500
    
    
    Signed-off-by: Toby Lam &lt;me@livekn.com&gt;

* __OpenSearchSink: add support for sending end-to-end acknowledgements (#2458)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 12 Apr 2023 16:33:59 -0500
    
    
    OpenSearchSink: add support for sending end-to-end acknowledgements
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __ENH: batching metrics with same tags in EMFLoggingMeterRegistry (#2467)__

    [Qi Chen](mailto:qchea@amazon.com) - Wed, 12 Apr 2023 15:32:01 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Add format string option to add_entries processor (#2464)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 12 Apr 2023 14:08:13 -0500
    
    
    * Add format string option to add_entries processor
    * Update config validations
    * Update README
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix: CVE-2023-20861 (#2473)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 11 Apr 2023 18:37:35 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Retry flushing of buffer on buffer TimeoutException (#2470)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 11 Apr 2023 14:24:44 -0500
    
    
    Retry flushing of buffer on buffer TimeoutException
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Address review comments from PR 2436 (#2459)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 11 Apr 2023 12:11:09 -0500
    
    
    Address review comments from PR 2436
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Added integration tests for end-to-end acknowledgements (#2442)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 10 Apr 2023 08:06:34 -0500
    
    
    * Changed the source config to end-to-end-acknowledgements and key name
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed check style errors
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Add list_to_map processor (#2453)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 7 Apr 2023 11:56:03 -0500
    
    efs/heads/s3-records-metrics
    * Add list_to_map processor plugin
     Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add support for acknowledgements to source, process worker and router strategy (#2436)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 6 Apr 2023 16:24:42 -0500
    
    
    Add support for acknowledgements to source, process worker and router strategy
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Support running tests on OpenSearch 2.6.0 (#2455)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 6 Apr 2023 15:22:35 -0500
    
    
    Fixes an issue with the tests that prevented them from running against
    OpenSearch 2.6.0. This also tries to get ahead of other possible new system
    indexes. Adds more OpenSearch versions to the list of versions to test.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __S3 select dev (#2447)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Thu, 6 Apr 2023 14:04:44 -0500
    
    
    Incorporated new yaml changes for s3 select #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;

* __Updated S3 worker logging (#2438)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 5 Apr 2023 16:39:34 -0500
    
    
    * Updated S3 worker logging
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __adding data-time patterns to key-path-prefix and creating readme for s3 dlq (#2451)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 5 Apr 2023 16:28:58 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Fix imports for AppendAggregateAction (#2452)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 5 Apr 2023 12:55:05 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Count SQS message delete failures in the S3 source (#2450)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Apr 2023 12:29:51 -0500
    
    
    Provide a new metric for when the S3 source is unable to delete an SQS message.
    Resolves #2449
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __merge aggregation (#2230)__

    [Krishnanand Singh](mailto:krishnanand91@gmail.com) - Wed, 5 Apr 2023 10:32:51 -0500
    
    
    Add new aggregate action to create aggregated Event by appending values over
    time
     Signed-off-by: KrishnanandSingh &lt;krishnanand_singh@cargill.com&gt;
    
    Signed-off-by: Krishnanand Singh &lt;krishnanand91@gmail.com&gt;

* __adding support for dlq plugins in opensearch (#2429)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 5 Apr 2023 09:28:32 -0500
    
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Refactor source coordination to split interfaces between SourceCoordinator and SourceCoordinationStore (#2444)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 5 Apr 2023 09:24:33 -0500
    
    
    Refactor source coordination to split interfaces between SourceCoordinator and
    SourceCoordinationStore
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Logging improvements to the csv codec, role arn, and span messages (#2448)__

    [roshan-dongre](mailto:roshan-dongre@users.noreply.github.com) - Wed, 5 Apr 2023 08:26:47 -0500
    
    
    * Logging improvements to the csv codec, role arn, and span messages
     Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;
    
    * improving the log messaging of the otel trace raw processor
     Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;
    
    ---------
     Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;

* __implementing s3 dlq writer (#2419)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 4 Apr 2023 09:32:18 -0500
    
    
    * implementing s3 dlq writer
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Addresses S3 Select YAML code review suggestions (#2439)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Mon, 3 Apr 2023 11:35:32 -0500
    
    
    * Rebased. Fixed test cases and addressed review comments (#2399)
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;
     Support for Snappy PR# 2420 (#2421)
     Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     MAINT: replace grok debugger (#2425)
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;
     Wire in source_coordination config and SourceCoordinator interface to Source
    plugins (#2395)
     Wire in source_coordination config and SourceCoordinator inteface to Source
    plugins
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
     Add EventFactory and AcknowledgementSetManager instantiations and make them
    available  (#2426)
     Add EventFactory and AcknowledgementSetManager instantiations and make them
    available
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Updated SQS logging (#2417)
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Address code review comment of changing yaml parameters to expression,
    input_serialization
    PR#1971
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
    Signed-off-by: Ashok Telukuntla
    &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
    Signed-off-by: Ashok Telukuntla
    &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;
     Fix test broken by rename
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Fix spacing
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    ---------
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    Co-authored-by:
    kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Updated SQS logging (#2417)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sat, 1 Apr 2023 22:20:08 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add EventFactory and AcknowledgementSetManager instantiations and make them available  (#2426)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Fri, 31 Mar 2023 15:04:33 -0500
    
    
    Add EventFactory and AcknowledgementSetManager instantiations and make them
    available
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Wire in source_coordination config and SourceCoordinator interface to Source plugins (#2395)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 31 Mar 2023 12:45:43 -0500
    
    
    Wire in source_coordination config and SourceCoordinator inteface to Source
    plugins
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __MAINT: replace grok debugger (#2425)__

    [Qi Chen](mailto:qchea@amazon.com) - Thu, 30 Mar 2023 23:24:39 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Support for Snappy PR# 2420 (#2421)__

    [Ashok Telukuntla](mailto:55903152+ashoktelukuntla@users.noreply.github.com) - Thu, 30 Mar 2023 16:13:54 -0500
    
    
    Snappy Support for PR# 2420
     Signed-off-by: Ashok Telukuntla &lt;ashoktla@amazon.com&gt;

* __Rebased. Fixed test cases and addressed review comments (#2399)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 30 Mar 2023 15:05:35 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __S3 select dev (#2353)__

    [Uday Chintala](mailto:udaych20@gmail.com) - Thu, 30 Mar 2023 09:44:20 -0500
    
    
    * Support S3 Select when loading objects from S3 via the S3 source #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Support S3 Select when loading objects from S3 via the S3 source #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Incorporated review comments for Issue #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Indentation issue fix.
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * EventMetadataModifier used in S3 Select, Review Comment Changes
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Modified S3 Select Readme.md file
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Incorporated S3 Select review changes #1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Removed as this is related to s3 select integration tests
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    * Incorporated review comments for Issue#1971
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;
    
    ---------
     Signed-off-by: Uday Kumar Chintala &lt;udaych20@gmail.com&gt;

* __updating opensearch sink constructor to support loading dlq plugins (#2415)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 29 Mar 2023 14:57:14 -0500
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __adding dlqObject and dlqWriter interface to (#2392)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 29 Mar 2023 14:56:42 -0500
    
    
    ---------
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Add acknowledgementSet and acknowledgementSetManager - End-to-End Ack Support (#2394)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Wed, 29 Mar 2023 12:31:50 -0500
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __ENH: opensearch sink AOSS support (#2385)__

    [Qi Chen](mailto:qchea@amazon.com) - Tue, 28 Mar 2023 09:14:18 -0500
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Updated the Developer Guide to point users to the Data Prepper documentation on opensearch.org. (#2367)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 28 Mar 2023 09:04:13 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/otel-metrics-source (#2336)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Mar 2023 16:35:35 -0500
    
    
    Bumps commons-io:commons-io from 2.10.0 to 2.11.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump net.bytebuddy:byte-buddy in /data-prepper-plugins/opensearch (#2407)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Mar 2023 13:49:01 -0500
    
    
    Bumps [net.bytebuddy:byte-buddy](https://github.com/raphw/byte-buddy) from
    1.12.22 to 1.14.2.
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.12.22...byte-buddy-1.14.2)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump commons-io:commons-io in /data-prepper-plugins/otel-trace-source (#2335)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Mar 2023 13:45:58 -0500
    
    
    Bumps commons-io:commons-io from 2.10.0 to 2.11.0.
    
    ---
    updated-dependencies:
    - dependency-name: commons-io:commons-io
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
    Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-expression (#2334)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 27 Mar 2023 13:45:02 -0500
    
    
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

* __Use Gradle version cataglos for Slf4j, Spring, Hamcrest, and Awaitility. Finished some missing JUnit dependencies. Removed some uses of Hamcrest and Awaitility which were not needed since these are part of the root project. (#2382)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Mar 2023 12:30:28 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixing java doc warnings (#2396)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 23 Mar 2023 12:49:41 -0500
    
    efs/heads/s3-metadata-config
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __improving logging in OSDP (#2391)__

    [roshan-dongre](mailto:roshan-dongre@users.noreply.github.com) - Tue, 21 Mar 2023 15:31:24 -0500
    
    
    Signed-off-by: Roshan Dongre &lt;roshdngr@amazon.com&gt;

* __Add Event Factory and generic event builder infrastructure (#2378)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Mon, 20 Mar 2023 19:13:31 -0500
    
    
    Add Event Factory and generic event builder infrastructure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __FIX: isolated service node (#2384)__

    [Qi Chen](mailto:qchea@amazon.com) - Mon, 20 Mar 2023 14:22:52 -0500
    
    
    * FIX: service-map isolated service
     Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Remove the old peer-forwarder build.gradle file. It is not part of the build and just needs to be deleted. (#2386)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 20 Mar 2023 14:20:49 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the Java serialization allowlist to have specific classes for JsonNode. Added new tests to verify that this new allowlist does not interfere with some expected Event patterns. (#2376)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 17 Mar 2023 15:18:11 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __OpenSearch Sink should make the number of retries configurable - Issue #2291 (#2339)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Thu, 16 Mar 2023 16:54:20 -0500
    
    
    OpenSearch Sink should make the number of retries configurable - Issue #2291
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;

* __Added change log for 2.1.1 (#2377)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 16 Mar 2023 11:27:35 -0500
    
    efs/heads/snakeyaml-2.0
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added release notes for 2.1.1 (#2375)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 16 Mar 2023 11:27:19 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Use Netty version supplied by dependencies (#2031)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Mar 2023 16:21:11 -0500
    
    efs/heads/2.1.1-release-notes
    * Removed old constraints on Netty which were resulting in pulling in older
    version of Netty. Our dependencies (Armeria and AWS SDK Java client) are
    pulling in newer versions so these old configurations are not necessary
    anymore.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix: Fixed IllegalArgumentException in PluginMetrics  (#2369)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 9 Mar 2023 12:54:31 -0600
    
    
    * Fix: Fixed IllegalArgumentException in PluginMetrics caused by pipeline name
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __FIX: traceState not required in Link (#2363)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 3 Mar 2023 16:55:37 -0600
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;

* __Added 2.1 change log (#2360)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 2 Mar 2023 15:20:55 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add release notes for 2.1.0 (#2354)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Thu, 2 Mar 2023 13:52:16 -0600
    
    
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


