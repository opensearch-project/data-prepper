
* __Prepare release 2.15.0 (#6728)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 6 Apr 2026 07:55:34 -0700
    
    EAD -&gt; refs/heads/2.15, tag: refs/tags/2.15.0, refs/remotes/upstream/2.15
    Signed-off-by: github-actions[bot]
    &lt;41898282+github-actions[bot]@users.noreply.github.com&gt; Co-authored-by:
    dlvenable &lt;293424+dlvenable@users.noreply.github.com&gt;

* __chore: Upgrade Jackson to 2.21.0 (#6709)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Thu, 2 Apr 2026 16:03:07 -0700
    
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Upgrade aws-cdk-lib to 2.247.0 (CVE-2026-33750, CVE-2026-33532). Resolves #6689. (#6715)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 2 Apr 2026 13:04:25 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __fix: expand necessary OpenSearch permissions for data prepper (#6649)__

    [JongminChung](mailto:chungjm0711@gmail.com) - Thu, 2 Apr 2026 11:54:06 -0700
    
    
    Signed-off-by: Jongmin Chung &lt;chungjm0711@gmail.com&gt;

* __fix a typo in s3_enrich metrics (#6714)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Thu, 2 Apr 2026 11:38:05 -0700
    
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __fix: error syntax in log-standard-template  (#6647)__

    [JongminChung](mailto:chungjm0711@gmail.com) - Thu, 2 Apr 2026 10:10:12 -0700
    
    
    fix: error syntax in template (logs-otel-v1-index-standard-template)
    
    Signed-off-by: Jongmin Chung &lt;chungjm0711@gmail.com&gt;

* __Prometheus Remote Write v1 Source  (#6627)__

    [Srikanth Padakanti](mailto:srikanth29.9@gmail.com) - Thu, 2 Apr 2026 10:09:48 -0700
    
    
    Add Prometheus Remote Write v1 source plugin
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Fix NPE in sanitizeMetricName when unit or aggregationTemporality is … (#6687)__

    [Srikanth Padakanti](mailto:srikanth29.9@gmail.com) - Thu, 2 Apr 2026 10:09:15 -0700
    
    
    Fix NPE in sanitizeMetricName when unit or aggregationTemporality is null
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Use EventFactory instead of JacksonEvent.builder() in iceberg-source (#6641)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Wed, 1 Apr 2026 06:40:49 -0700
    
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Add custom Jackson deserializer to handle empty plugin configs and reject empty strings (#6598)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Tue, 31 Mar 2026 12:23:36 -0700
    
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Minor updates to the OpenSearch versions. Update to latest patches and focus on odd versions. (#6670)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 31 Mar 2026 09:17:04 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add insecure flag to Prometheus sink to require HTTPS by default (#6688)__

    [Shenoy Pratik](mailto:pshenoy36@gmail.com) - Tue, 31 Mar 2026 07:15:24 -0700
    
    
    Signed-off-by: ps48 &lt;pshenoy36@gmail.com&gt;

* __Bump minimatch and aws-cdk-lib in /testing/aws-testing-cdk (#6599)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Mar 2026 06:54:41 -0700
    
    
    Bumps [minimatch](https://github.com/isaacs/minimatch) to 3.1.5 and updates
    ancestor dependency
    [aws-cdk-lib](https://github.com/aws/aws-cdk/tree/HEAD/packages/aws-cdk-lib).
    These dependencies need to be updated together.
    
     Updates `minimatch` from 3.1.2 to 3.1.5
    - [Changelog](https://github.com/isaacs/minimatch/blob/main/changelog.md)
    - [Commits](https://github.com/isaacs/minimatch/compare/v3.1.2...v3.1.5)
    
    Updates `aws-cdk-lib` from 2.177.0 to 2.241.0
    - [Release notes](https://github.com/aws/aws-cdk/releases)
    - [Changelog](https://github.com/aws/aws-cdk/blob/main/CHANGELOG.v2.alpha.md)
    -
    [Commits](https://github.com/aws/aws-cdk/commits/v2.241.0/packages/aws-cdk-lib)
    
    --- updated-dependencies:
    - dependency-name: minimatch
     dependency-version: 3.1.5
     dependency-type: indirect
    - dependency-name: aws-cdk-lib
     dependency-version: 2.241.0
     dependency-type: direct:production
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump requests in /examples/trace-analytics-sample-app/sample-app (#6677)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Mar 2026 06:54:02 -0700
    
    
    Bumps [requests](https://github.com/psf/requests) from 2.32.4 to 2.33.0.
    - [Release notes](https://github.com/psf/requests/releases)
    - [Changelog](https://github.com/psf/requests/blob/main/HISTORY.md)
    - [Commits](https://github.com/psf/requests/compare/v2.32.4...v2.33.0)
    
    --- updated-dependencies:
    - dependency-name: requests
     dependency-version: 2.33.0
     dependency-type: direct:production
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump flatted from 3.3.3 to 3.4.2 in /release/staging-resources-cdk (#6660)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Mar 2026 06:53:37 -0700
    
    
    Bumps [flatted](https://github.com/WebReflection/flatted) from 3.3.3 to 3.4.2.
    - [Commits](https://github.com/WebReflection/flatted/compare/v3.3.3...v3.4.2)
    
    --- updated-dependencies:
    - dependency-name: flatted
     dependency-version: 3.4.2
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump flatted from 3.2.9 to 3.4.2 in /testing/aws-testing-cdk (#6658)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Mar 2026 06:53:11 -0700
    
    
    Bumps [flatted](https://github.com/WebReflection/flatted) from 3.2.9 to 3.4.2.
    - [Commits](https://github.com/WebReflection/flatted/compare/v3.2.9...v3.4.2)
    
    --- updated-dependencies:
    - dependency-name: flatted
     dependency-version: 3.4.2
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump brace-expansion in /release/staging-resources-cdk (#6681)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Mar 2026 06:52:42 -0700
    
    
    Bumps  and [brace-expansion](https://github.com/juliangruber/brace-expansion).
    These dependencies needed to be updated together.
    
    Updates `brace-expansion` from 1.1.12 to 1.1.13
    - [Release notes](https://github.com/juliangruber/brace-expansion/releases)
    -
    [Commits](https://github.com/juliangruber/brace-expansion/compare/v1.1.12...v1.1.13)
    
    Updates `brace-expansion` from 2.0.2 to 2.0.3
    - [Release notes](https://github.com/juliangruber/brace-expansion/releases)
    -
    [Commits](https://github.com/juliangruber/brace-expansion/compare/v1.1.12...v1.1.13)
    
    --- updated-dependencies:
    - dependency-name: brace-expansion
     dependency-version: 1.1.13
     dependency-type: indirect
    - dependency-name: brace-expansion
     dependency-version: 2.0.3
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump brace-expansion from 1.1.12 to 1.1.13 in /testing/aws-testing-cdk (#6679)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Mar 2026 06:52:15 -0700
    
    
    Bumps [brace-expansion](https://github.com/juliangruber/brace-expansion) from
    1.1.12 to 1.1.13.
    - [Release notes](https://github.com/juliangruber/brace-expansion/releases)
    -
    [Commits](https://github.com/juliangruber/brace-expansion/compare/v1.1.12...v1.1.13)
    
    --- updated-dependencies:
    - dependency-name: brace-expansion
     dependency-version: 1.1.13
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump picomatch from 2.3.1 to 2.3.2 in /testing/aws-testing-cdk (#6676)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 30 Mar 2026 06:51:43 -0700
    
    
    Bumps [picomatch](https://github.com/micromatch/picomatch) from 2.3.1 to 2.3.2.
    - [Release notes](https://github.com/micromatch/picomatch/releases)
    - [Changelog](https://github.com/micromatch/picomatch/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/micromatch/picomatch/compare/2.3.1...2.3.2)
    
    --- updated-dependencies:
    - dependency-name: picomatch
     dependency-version: 2.3.2
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump picomatch from 2.3.1 to 2.3.2 in /release/staging-resources-cdk (#6675)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 27 Mar 2026 09:38:13 -0700
    
    
    Bumps [picomatch](https://github.com/micromatch/picomatch) from 2.3.1 to 2.3.2.
    - [Release notes](https://github.com/micromatch/picomatch/releases)
    - [Changelog](https://github.com/micromatch/picomatch/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/micromatch/picomatch/compare/2.3.1...2.3.2)
    
    --- updated-dependencies:
    - dependency-name: picomatch
     dependency-version: 2.3.2
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Fix dlqPipeline functionality broken by PR 6349 (#6678)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Fri, 27 Mar 2026 09:37:43 -0700
    
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Fixes the typeof operator along with data types in expressions. (#6673)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 25 Mar 2026 12:31:01 -0700
    
    
    Fixes the typeof operator along with data types in expressions.
    
    The typeof operator was not working because FunctionName came before DataTypes
    it and it has a very broad matching rule. This means that ANTLR was resolving
    these as functions instead of the DataTypes that typeof needs.
    
    The fix here is to move FunctionName to the bottom. Additionally, I renamed
    FunctionName to Identifier to make this more generic for other future
    identifiers.
    
    Also there were no tests for the typeof operator. I have added some of these
    tests, first to verify the failure and fix, and second to ensure future changes
    do not break them.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __add S3 Enrich processor to merge ml batch job output with source inputs (#5992)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Tue, 24 Mar 2026 17:32:13 -0500
    
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Adding support for pipeline DLQ to SQS sink (#6593)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Tue, 24 Mar 2026 15:22:14 -0500
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Updated the plugin names for the OTLP sources for consistency. (#6530)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 24 Mar 2026 12:02:23 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates to the CDK stack to support the S3 sink integration tests. (#6662)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 24 Mar 2026 09:23:42 -0700
    
    
    Exports the resources and grants permissions so that the integration tests on
    GitHub can pull in the exports. Also grants read permissions to the S3 bucket
    since the tests need to read to verify the results.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add generateUuid() function (#6653)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Mon, 23 Mar 2026 14:28:02 -0700
    
    
    Add generateUuid() function for UUID creation
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Remove Experimental annotation from sqs sink (#6661)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 23 Mar 2026 10:18:59 -0700
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fixes a bug with expressions when functions are combined with and/or operations. (#6669)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Mar 2026 10:12:53 -0700
    
    
    The function composition change (#6628) converted function from a lexer rule to
    a parser rule, which meant function arguments became full parser
    sub-expressions (including conditionalExpression). The
    ParseTreeEvaluatorListener uses LPAREN/RPAREN tokens on the operatorSymbolStack
    as barriers to prevent operators from being evaluated inside nested scopes -
    this is how parenthesesExpression works correctly. However, the function
    composition code explicitly skipped LPAREN/RPAREN tokens inside functions,
    removing this barrier. When a multi-argument function appeared as the right
    operand of and/or, the AND operator sitting on the stack would fire prematurely
    when the walker exited the inner conditionalExpression of a function argument,
    consuming the function&#39;s arguments as boolean operands, silently pushing false,
    and leaving the function with only one argument instead of two.
    
    The fix simply stops skipping LPAREN/RPAREN inside functions, letting them flow
    through to the normal handling where they act as operator stack barriers.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add configurable AWS credential validation at bootstrap (#6629)__

    [Sumit Bhattacharya](mailto:41795508+BhattacharyaSumit@users.noreply.github.com) - Mon, 23 Mar 2026 09:55:49 -0700
    
    
    Add per-secret skip_validation_on_start flag for credential validation
    
    - Add skip_validation_on_start boolean field to AwsSecretManagerConfiguration
    - Default value is false (safe-by-default - validates credentials at bootstrap)
    - When set to false, secret retrieval is deferred until first access
    - Implement lazy-loading logic in AwsSecretsSupplier for deferred secrets
    - Add comprehensive unit and integration tests
    - Maintain backward compatibility (default behavior unchanged)
    
    This allows users to disable credential validation per-secret when credentials
    are not available at bootstrap time, while maintaining fail-fast behavior by
    default for production safety.
    
    Resolves issue where DataPrepper fails to start with invalid credentials by
    providing per-secret control over bootstrap validation.
    
    Make lazy-loading of secrets atomic and handle updateValue with unloaded
    secrets
    
    Use ConcurrentMap.compute() in loadSecretIfNeeded() to ensure the sentinel
    check and secret retrieval are performed atomically, avoiding race conditions
    with concurrent access.
    
    Add loadSecretIfNeeded() call at the beginning of updateValue() to ensure
    secrets are loaded before update logic runs, preventing the NOT_LOADED_SENTINEL
    from being treated as a plain value store.
    
    Signed-off-by: Sumit Bhattacharya &lt;sumit4739@gmail.com&gt;

* __S3 sink server-side encryption with KMS (#6655)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 20 Mar 2026 14:19:01 -0700
    
    
    S3 sink server-side encryption with KMS
    
    Adds new configuration for encryption options in the S3 sink. Allow configuring
    a custom KMS key for S3 server-side encryption. Support SSE-KMS and DSSE-KMS.
    Supports multi-part and locally buffered options.
    
    Resolves #6528.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix duplicate _seconds suffix in APM latency metric name (#6657)__

    [Vamsi Manohar](mailto:reddyvam@amazon.com) - Thu, 19 Mar 2026 20:21:17 -0500
    
    
    The Prometheus sink appends unit suffixes to metric names (e.g., unit &#34;s&#34; 
    becomes &#34;_seconds&#34;). The APM service map processor was naming the histogram 
    metric &#34;latency_seconds&#34; with unit &#34;s&#34;, resulting in &#34;latency_seconds_seconds&#34; 
    when exported to Prometheus. Rename the metric to &#34;latency&#34; so the final 
    Prometheus metric name is correctly &#34;latency_seconds&#34;.
    
    Signed-off-by: Vamsi Manohar &lt;reddyvam@amazon.com&gt;

* __Add HTTP basic auth and no-auth support for prometheus-sink (#6595)__

    [Shenoy Pratik](mailto:pshenoy36@gmail.com) - Thu, 19 Mar 2026 17:18:30 -0500
    
    
    Signed-off-by: ps48 &lt;pshenoy36@gmail.com&gt;

* __Revert &#34;Otel metrics source http service (#6604)&#34; (#6656)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 19 Mar 2026 14:44:51 -0500
    
    
    This reverts commit bb61dbe2c48cb098ce90e49a986b8d3dd585bcf4.
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix race condition between export partition creation and data file partition completion for ddb source (#6651)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 19 Mar 2026 13:03:45 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add S3 Scan processing condition evaluator to ensure object completeness (#6624)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Fri, 20 Mar 2026 01:57:24 +0800
    
    
    Add S3 Scan processing condition evaluator to ensure S3 object completeness
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Remove default codec and require codec for sqs sink (#6486)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 18 Mar 2026 16:38:02 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Allow group id for standard queues sqs sink (#6527)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 18 Mar 2026 16:37:29 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix invalid document version events still included in bulk requests (#6601) (#6645)__

    [Keyur Patel](mailto:keyurpatel.opensource@gmail.com) - Wed, 18 Mar 2026 13:00:09 -0500
    
    
    Events with invalid document_version values are correctly sent to the DLQ but
    were still being added to the OpenSearch bulk request. Added continue 
    statements after the NumberFormatException and RuntimeException catch blocks in
    doOutput() to skip the rest of the loop iteration for failed events.
    
    Added unit tests to verify events with invalid versions are not added to the
    bulk request.
    
    Signed-off-by: Keyur-S-Patel &lt;keyurpatel.opensource@gmail.com&gt;

* __Add substring expression functions (#6621)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Tue, 17 Mar 2026 16:29:37 -0500
    
    
    The expression language has no way to extract a portion of a string by
    delimiter. Existing string processors mutate fields in-place but cannot produce
    a value for assignment via value_expression.
    
    Add four new expression functions:
    
    - substringAfter(s, d): text after the first occurrence of d
    - substringBefore(s, d): text before the first occurrence of d
    - substringAfterLast(s, d): text after the last occurrence of d
    - substringBeforeLast(s, d): text before the last occurrence of d
    
    Both arguments accept JSON Pointers or string literals. If the delimiter is not
    found, the original string is returned. If the source resolves to null, null is
    returned.
    
    Resolve #6612
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Otel metrics source http service (#6604)__

    [Tomas](mailto:tlongo@sternad.de) - Tue, 17 Mar 2026 09:39:36 +0100
    
    
    * Add HTTP service to otel_metrics_source
    
    Integrates an HTTP (non-gRPC) service into the OTel metrics source plugin, 
    mirroring the existing pattern from otel_trace_source and otel_logs_source. 
    Both gRPC and HTTP services now run on the same Armeria server.
    
    Key changes:
    - Add ArmeriaHttpService for handling HTTP metric export requests
    - Add HttpExceptionHandler for HTTP-specific error handling
    - Support compression, authentication, throttling, and health checks for HTTP
    - Add configurable http_path option
    - Refactor OTelMetricsSource to directly configure the server
    - Remove ConvertConfiguration (inlined into source)
    - Split monolithic test into focused test classes (gRPC, HTTP, RetryInfo)
    - Add E2E tests for HTTP, gRPC, protobuf, and unframed requests
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt; Signed-off-by: Kai Sternad
    &lt;kai@sternad.de&gt;
    
    * Kepp constant handling consistent with other sources
    
    Signed-off-by: Kai Sternad &lt;kai@sternad.de&gt;
    
    * Add guard to httpPath, deduplicate health check
    
      1. createServer() — configureHttpService() is now guarded with if
    (getHttpPath() != null), preventing the NPE
     2. HTTP health check — moved from configureHttpService() into createServer(),
    so it registers when either httpPath or enableUnframedRequests is set (matching
    the OTelLogsSource pattern)
     3. configureHttpService() — removed the duplicate health check registration
    
    Signed-off-by: Kai Sternad &lt;kai@sternad.de&gt;
    
    * Move healthCheck back to configureHttpService
    
    Signed-off-by: Kai Sternad &lt;kai@sternad.de&gt;
    
    * Incorporate review findings
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt;
    
    * Deduplicate output format, decompose createServer
    
    Signed-off-by: Kai Sternad &lt;kai@sternad.de&gt;
    
    ---------
    
    Signed-off-by: Tomas Longo &lt;tlongo@sternad.de&gt; Signed-off-by: Kai Sternad
    &lt;kai@sternad.de&gt; Co-authored-by: Kai Sternad &lt;kai@sternad.de&gt;

* __Fix possible missing file count in data file loader (#6639)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 16 Mar 2026 17:11:47 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Fix requestsTooLarge metric reporting when decompression buffer overflows on armeria in otel logs source (#6633)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 16 Mar 2026 08:37:35 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add minute range support to Dimensional TimeSlice Source Crawler framework (#6368)__

    [Raju Enugula](mailto:enugraju@amazon.com) - Fri, 13 Mar 2026 15:39:39 -0500
    
    
    * Add minute range support to Dimensional TimeSlice crawler framework
    
    Signed-off-by: enugraju &lt;enugraju@amazon.com&gt;
    
    * Changed return type of getLookBackMinutes to Instant
    
    Signed-off-by: enugraju &lt;enugraju@amazon.com&gt;
    
    * Review comment fixes
    
    Signed-off-by: enugraju &lt;enugraju@amazon.com&gt;
    
    * Created a generic method to centralize the logic for wrapping
    adjustedStartTime
    
    Signed-off-by: enugraju &lt;enugraju@amazon.com&gt;
    
    ---------
    
    Signed-off-by: enugraju &lt;enugraju@amazon.com&gt;

* __fix: add BackoffCredentialsProvider to mitigate STS throttling across all plugins (#6637)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 13 Mar 2026 15:25:59 -0500
    
    
    Wrap StsAssumeRoleCredentialsProvider with BackoffCredentialsProvider in 
    CredentialsProviderFactory. When credential resolution fails (e.g. role deleted
    or trust policy misconfigured), the wrapper caches the failure and applies
    exponential backoff (10s to 10min) before retrying STS, preventing excessive
    AssumeRole calls that cause STS throttling.
    
    This protects all plugins that use CredentialsProviderFactory including S3,
    OpenSearch, Lambda, SQS, and most AWS-integrated sources and sinks.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix where stream and leader scheduler could die from unexpected error for mongodb source (#6638)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 13 Mar 2026 14:25:35 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Adds the current release as a GitHub label on the README. It links to the releases tab in GitHub. (#6625)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 Mar 2026 11:28:00 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes the cache for the KMS encryption plugin. (#6636)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 Mar 2026 09:18:44 -0700
    
    
    The cache was using byte[] as the key. As an array it doesn&#39;t have
    equals/hashCode so the keys would never be found. To cache it correctly I use
    SdkBytes which implements both.
    
    I also added three metrics for the KMS plugin: 1. A gauge on decrypted keys in
    the cache; 2. KMS requests succeeded; 3. KMS requests failed.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __fix: mitigate STS assume role throttling in Kafka buffer (#6634)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Fri, 13 Mar 2026 11:00:46 -0500
    
    
    Prevent excessive STS AssumeRole calls when customers delete their IAM role or
    misconfigure the trust policy. Previously, one pipeline could generate 12,000
    STS calls in 4 minutes due to unbounded retries of non-retryable
    AccessDeniedException errors.
    
    Changes:
    - KafkaSecurityConfigurer: Fail fast on STS 403 (AccessDenied) in
    getBootStrapServersForMsk() instead of retrying 360 times
    - KafkaSecurityConfigurer: Replace fixed 10s retry sleep with exponential
    backoff (10s to 10min max) for retryable STS and Kafka errors
    - KafkaCustomConsumer: Replace fixed 10s retry with exponential backoff using
    Kafka&#39;s ExponentialBackoff (10s to 10min max) for AuthenticationException
    errors
    - KafkaCustomConsumer: Use Duration constants for backoff readability
    - KafkaCustomConsumer: Reset backoff counter on successful poll to handle
    transient errors gracefully Add exponential backoff to outer run() exception
    handler
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * chore: remove unused imports from KafkaSecurityConfigurerTest
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * refactor: address review comments - use Kafka ExponentialBackoff, Duration
    constants, remove silent shutdown
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Provides a mechanism to get the size of the JSON representation of an event. (#6635)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 13 Mar 2026 08:46:31 -0700
    
    
    Provides a mechanism to get the size of the JSON representation of an event.
    
    This adds a new toJsonString() function to expressions. Updates length()
    function to accept a direct string as input, so that it can be composed with
    toJsonString().
    
    Includes a fix for add_entries to validate expressions, but not evaluate them
    in the constructor. The approach was brittle and failed for the new
    toJsonString function.
    
    Resolves #6278.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add support for invoking acknowledgmentSet callback on expiry (#6596)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 12 Mar 2026 15:05:52 -0700
    
    
    Add support for invoking acknowledgmentSet callback on expiry. Fixed expiry
    logic in DefaultAcknowledgementSet.
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Split the GitHub Action for license header checks into two workflows. One for checking the PR and the other for posting PR comments. (#6632)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 12 Mar 2026 12:13:06 -0700
    
    
    The current approach is resulting in permissions failures when trying to post
    comments. This approach should give the necessary permissions.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support function composition in expressions. (#6628)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 12 Mar 2026 11:02:31 -0700
    
    
    Support function composition in expressions.
    
    The grammar changed function from a lexer rule to a parser rule. The previous
    approach yielded on token that needed to be parsed manually. As a parser rule
    each argument is a full sub-expression. This includes conditionalExpression,
    arithmeticExpression, stringExpression, jsonPointer, or literal. This gives
    ANTLR structural visibility into function calls, enabling composable functions
    that were impossible when the whole call was a single unparsed string. The
    grammar change meant that the ParseTreeEvaluatorListener now parses functions
    instead of ParseTreeCoercionService.
    
    I also consolidated SET_DELIMITER and COMMA and DIVIDE and FORWARDSLASH. Having
    these as different tokens caused problems parsing grammars.
    
    Resolves #6322.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add Iceberg CDC source plugin (#6554)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Wed, 11 Mar 2026 09:40:53 -0700
    
    
    Add Iceberg CDC source plugin (#6552)
    
    * Merge UPDATE pairs into single INDEX for CDC events
    
    When a CoW UPDATE produces a DELETE + INSERT pair with the same document_id
    after carryover removal, emit only the INSERT as INDEX. Since OpenSearch INDEX
    is an upsert, the DELETE is unnecessary.
    
    This also eliminates a potential issue where multiple ProcessWorker threads
    consuming from the buffer in parallel could reorder DELETE and INDEX operations
    for the same document, causing data loss.
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Fix flaky Router_ThreeRoutesDefaultIT by polling all assertions (#6620)__

    [Kai Sternad](mailto:kai@sternad.de) - Wed, 11 Mar 2026 09:28:54 +0100
    
    
    Move all assertions inside the await() block so they are polled until they
    pass, rather than checking sinks are non-empty and then immediately asserting
    exact counts outside the polling loop. Also increase the timeout from 2s to 10s
    to accommodate slow CI runners.
    
    Signed-off-by: Kai Sternad &lt;kai@sternad.de&gt;

* __feat: add Claude Code attribute mapping profile for GenAI normalization (#6623)__

    [Kyle Hounslow](mailto:7102778+kylehounslow@users.noreply.github.com) - Tue, 10 Mar 2026 11:45:09 -0700
    
    
    * feat: add Claude Code attribute mapping profile for GenAI normalization
    
    Adds claude_code profile to genai-attribute-mappings.yaml:
    - model → gen_ai.request.model
    - input_tokens → gen_ai.usage.input_tokens
    - output_tokens → gen_ai.usage.output_tokens
    
    Tested with real Claude Code v2.1.71 traces
    (CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1).
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    * docs: add GenAI enrichment docs to otel_traces processor and output_format
    README
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;

* __Refactors how expression functions handle string literals and EventKeys. Now the ParseTreeCoercionService will provide either a String without quotes or an EventKey. This is important for function composition to work because the result of one function will be a string without the quotes. It also removes a lot of duplicated logic for checking the literal type. (#6626)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 10 Mar 2026 11:10:53 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Increase the Jenkins job timeout to 2 hours from 1. The 2.14.1 job is timing out. (#6618)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 5 Mar 2026 18:10:44 -0800
    
    efs/heads/function-composition-1
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Release notes for 2.14.1. (#6616)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 5 Mar 2026 13:59:49 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support validating library compatibility on data-prepper-api. (#6607)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 5 Mar 2026 12:34:12 -0800
    
    
    Resolves #6605.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix AMP IntegrationTests (#6608)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 5 Mar 2026 09:56:36 -0800
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Updates RELEASING.md to remove the section to update DataPrepperVersion. This value is now dynamically determined so there is no manual step for releasing. (#6559)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 4 Mar 2026 08:01:28 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __GitHub Action to verify the Gradle buildSrc. (#6597)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 3 Mar 2026 09:02:57 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Security procedures for creating or editing push-based sources (#6538)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 3 Mar 2026 07:30:15 -0800
    
    
    Creates security procedures for creating new push-based sources.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add change log for 2.14.0 (#6564)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Sat, 28 Feb 2026 13:59:44 -0600
    
    
    * Add release notes for 2.14.0
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Update changelog with #6548
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __test(otel_logs_source): invert default httpPath in test config fixture (#6575)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 27 Feb 2026 14:42:19 -0600
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __fix(otel_logs_source): fix NPE and regressions introduced by HTTP service support (#6572)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 27 Feb 2026 10:57:25 -0600
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Release notes for Data Prepper 2.14.0. (#6571)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 27 Feb 2026 06:21:11 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __feat: Add GenAI agent trace enrichment to otel_traces processor (#6548)__

    [Kyle Hounslow](mailto:7102778+kylehounslow@users.noreply.github.com) - Tue, 24 Feb 2026 16:24:49 -0800
    
    
    * feat: add GenAI agent trace enrichment to otel_traces processor
    
    Always-on enrichment in otel_traces processor:
    - Normalizes vendor attributes (OpenInference, OpenLLMetry) to gen_ai.* semconv
    - Propagates select gen_ai attributes from child spans to root
    - Aggregates token counts across children to root
    - Strips conflicting flattened sub-keys
    
    No configuration required. No-op for non-GenAI traces.
    
    RFC: https://github.com/opensearch-project/data-prepper/issues/6542
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    * fix: use OTelProtoOpensearchCodec storage key format in GenAI enrichment
    
    OTelProtoOpensearchCodec converts span attribute keys from dot-notation to a
    prefixed @ format before storing them in the JacksonSpan attributes map (e.g.
    gen_ai.system -&gt; span.attributes.gen_ai@system). The enrichment code was using
    dot-notation for lookups and writes, so it silently found nothing in production
    even though unit tests passed (tests bypass the codec).
    
    Fix: add toStorageKey()/toLogicalKey() helpers that convert between the two
    formats. All attribute reads and writes in enrichRootSpan, normalizeAttributes,
    and stripFlattenedSubkeys now use the storage format.
    
    Test fix: add convertToStorageFormat() helper that renames attribute keys to
    simulate the codec, and storageFormatRecords() that applies it before passing
    spans to the processor. JSON fixtures stay in dot-notation. All GenAI tests now
    exercise the real code path.
    
    E2E validated: LangGraph, Strands, CrewAI root spans now have gen_ai.* 
    attributes propagated correctly.
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    * refactor: address PR review comments on GenAiAttributeMappings and tests
    
    - Make MappingTarget fields private with getKey()/isWrapSlice() getters
    - Make LOOKUP_TABLE/OPERATION_NAME_VALUES private with static getters
    - Add GenAiAttributeMappingsTest with direct coverage of getters and mappings
    - Assert result/attrs non-empty in testFlattenedSubkeysStripped to prevent
     silent pass when collections are empty
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    * test: add integration test verifying GenAI enrichment runs in
    OTelTraceRawProcessor.doExecute
    
    Adds testGenAiEnrichmentRunsDuringDoExecute to OTelTraceRawProcessorTest. 
    Passes a span with OpenLLMetry vendor attributes through doExecute and asserts
    the normalized gen_ai.* attribute appears on the output span, verifying the
    enrichment call is wired into the processor pipeline.
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    * refactor: load GenAI attribute mappings from YAML resource file
    
    Moves hardcoded attribute mappings from GenAiAttributeMappings.java into 
    genai-attribute-mappings.yaml in the jar resources. Loaded at class init via
    Jackson YAML. This separates data from code and makes it easier to add new
    instrumentation library mappings without modifying Java.
    
    Covers OpenInference (15 mappings) and OpenLLMetry (20 mappings) profiles plus
    operation_name_values. Adds testMappingsFileExists to verify the resource file
    is present and readable.
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    * rename: wrapSlice -&gt; wrapAsArray for clarity in GenAI attribute mappings
    
    Renamed the flag that wraps a scalar string value into a single-element JSON
    array from wrapSlice to wrapAsArray (Java) and wrap_as_array (YAML). The new
    name makes the behavior immediately clear without needing context.
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    * revert: remove storage-key format from GenAI enrichment
    
    Reverts the behavioral changes from 5ac188ad which converted attribute lookups
    and writes to use span.attributes.* prefix with @ separators
    (e.g. span.attributes.gen_ai@system). The enrichment code now uses plain
    dot-notation keys (e.g. gen_ai.system) matching the format in the JacksonSpan
    attributes map at processing time.
    
    Removed: toStorageKey(), toLogicalKey(), STORAGE_PREFIX, and the 
    convertToStorageFormat()/storageFormatRecords() test helpers.
    
    Preserves accessor refactors (getKey(), isWrapAsArray(), getLookupTable()) and
    test assertions from subsequent commits.
    
    Unit tests: 35/35 pass (full otel-trace-raw-processor module) E2E: Strands +
    LangGraph agents → local DP → OpenSearch verified
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    * fix: add license header to genai-attribute-mappings.yaml
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kyle Hounslow &lt;kylhouns@amazon.com&gt;

* __otel_apm_service_map: Added support for deriving remote service and remote operation (#6539)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Mon, 23 Feb 2026 20:52:51 -0800
    
    
    * otel_apm_service_map: Added support for deriving remote service and remote
    operation
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed license header check failures
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;

* __Increasing ack timeout to 4 hours for kds source (#6547)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Mon, 23 Feb 2026 12:46:55 -0600
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Bump the next version of Data Prepper to 2.15. (#6558)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 23 Feb 2026 09:29:47 -0800
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


