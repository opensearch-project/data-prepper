
* __fix(dlq): release event handle when DlqObject has Event and is sent to S3 DLQ (#7244) (#7260)__

    [opensearch-ci](mailto:83309141+opensearch-ci-bot@users.noreply.github.com) - Thu, 5 Nov 2026 13:45:33 -0800
    
    EAD -&gt; refs/heads/2.17, tag: refs/tags/2.17.0, refs/remotes/origin/2.17
    When both a failure pipeline and S3 DLQ are configured, the DlqObject carries
    the full Event but may be released directly after S3 DLQ write instead of being
    sent to the failure pipeline. Previously, releaseEventHandle() silently dropped
    the release when event was not null, causing acknowledgement timeouts and kafka
    reprocessing.
    
    Release via event.getEventHandle().release() when event is present.
    
    
    (cherry picked from commit 03b33817bc94c718d17a6830197ff959156f4d01)
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt; Signed-off-by: opensearch-ci-bot
    &lt;opensearch-infra@amazon.com&gt; Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bypass className resolution in Glue Schema Registry JSON deserialization (#7256) (#7259)__

    [opensearch-ci](mailto:83309141+opensearch-ci-bot@users.noreply.github.com) - Thu, 5 Nov 2026 13:35:08 -0800
    
    
    Wrap GlueSchemaRegistryKafkaDeserializer in
    SafeGlueSchemaRegistryKafkaDeserializer that bypasses Class.forName() for JSON
    schemas by parsing raw payload directly as a Map. For non-JSON schemas (AVRO,
    Protobuf), the delegate deserializer is used.
    
    
    
    (cherry picked from commit 66b9a481e6fd92d02064904ff533b2203d8c0840)
    
    Signed-off-by: Harsha Siva Sai Sannapaneni &lt;sivasais@amazon.com&gt; Signed-off-by:
    opensearch-ci-bot &lt;opensearch-infra@amazon.com&gt; Co-authored-by: Harsha Siva Sai
    Sannapaneni &lt;harshasivasai85@gmail.com&gt; Co-authored-by: Harsha Siva Sai
    Sannapaneni &lt;sivasais@amazon.com&gt;

* __fix: Pass PostgreSQL database name as a driver property (#7255) (#7257)__

    [opensearch-ci](mailto:83309141+opensearch-ci-bot@users.noreply.github.com) - Thu, 5 Nov 2026 12:55:22 -0800
    
    
    (cherry picked from commit 83a70828ea5753fcd070ad2034b81ea22a1f7d45)
    
    
    (cherry picked from commit 2c6af61c00b735a01c324fec579377fa341663c1)
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt; Signed-off-by:
    opensearch-ci-bot &lt;opensearch-infra@amazon.com&gt; Co-authored-by: Divyansh
    Bokadia &lt;dbokadia@amazon.com&gt;

* __Adds the release notes for Data Prepper 2.17.0. (#7253) (#7258)__

    [opensearch-ci](mailto:83309141+opensearch-ci-bot@users.noreply.github.com) - Thu, 5 Nov 2026 12:23:43 -0800
    
    
    (cherry picked from commit 17ecb42d77c8dbd711179afd0848225bb2e2c6c3)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Signed-off-by: opensearch-ci-bot
    &lt;opensearch-infra@amazon.com&gt; Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Prepare release 2.17.0 (#7245)__

    [opensearch-ci](mailto:83309141+opensearch-ci-bot@users.noreply.github.com) - Wed, 4 Nov 2026 06:45:58 -0800
    
    
    Signed-off-by: github-actions[bot]
    &lt;41898282+github-actions[bot]@users.noreply.github.com&gt; Co-authored-by:
    dlvenable &lt;293424+dlvenable@users.noreply.github.com&gt;

* __Support a scan delay for empty S3 folder partitions on the RDS source (#7238)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Mon, 2 Nov 2026 14:37:27 -0600
    
    
    When a folder partition returns no objects, the S3 scan worker gives it up 
    immediately, so a quiet source re-lists its folders back to back at S3 latency.
    A source with a trickle of changes therefore makes tens of ListObjectsV2 calls
    per second per worker even though almost every listing is empty.
    
    Add folder_partitions.empty_folder_rescan_delay to the S3 source. When set, an
    empty folder partition keeps its lease for the delay instead of being given up,
    so no worker lists it again until the delay has passed. The partition progress
    state is preserved, so idle folder deletion still works. If the lease cannot be
    extended, the partition is given up as before.
    
    Expose it on the RDS source as empty_scan_delay and pass it through the RDS 
    template. The default is zero, which keeps the current behavior.
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Increase the maximum JSON string length in the OpenSearch source and sink to 64 MB (#7222)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Mon, 2 Nov 2026 10:42:55 -0800
    
    
    The opensearch source deserializes search responses using JacksonJsonpMapper 
    instances created with Jackson&#39;s default StreamReadConstraints, which limit 
    string values to 20,000,000 characters. Documents with a larger string field 
    fail with StreamConstraintsException before an Event is created, and the source
    retries the index indefinitely.
    
    Raise the limit to JsonFactories.MAX_JSON_STRING_LENGTH (64 MB, the limit used 
    by JacksonEvent since #7146) on the OpenSearch (RestClient and AwsSdk2) and 
    Elasticsearch client mappers of the source.
    
    The opensearch sink also re-parses documents with default limits in a few 
    places, so apply the same limit there:
    - BulkOperationFactory, used for update/upsert with document filters
    - FailedBulkOperationConverter, which otherwise writes an empty document to the
    DLQ
    - PullIngestionEnvelopeBuilder
    - PreSerializedJsonpMapper&#39;s JSON provider, used to parse responses
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Update dependency setuptools to v83 (#7009)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Mon, 2 Nov 2026 09:40:43 -0800
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Update dependency werkzeug to v3.1.9 (#7176)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Mon, 2 Nov 2026 09:40:20 -0800
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __fix(cloudwatch-logs): stop acknowledging undelivered logs (#7212)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Mon, 2 Nov 2026 09:38:20 -0800
    
    
    * fix(cloudwatch-logs): stop acknowledging undelivered logs
    
    With acknowledgements enabled and no DLQ, the sink queued batches on an
    unbounded executor and returned immediately. It then retried failed batches
    forever and released every failure as delivered, so data was lost silently and
    memory grew during an outage.
    
    doOutput now hands batches to a fixed pool of `workers` threads and waits until
    every batch is settled. Each batch is retried up to max_retries, then sent to
    the DLQ or negatively acknowledged so the source can redrive it. The change
    also adds metrics for events released without delivery, sent to the DLQ, or
    negatively acknowledged.
    
    It also removes CloudWatchLogsLimits and Buffer methods that nothing calls any
    more, and trims unit tests that only repeated what another test already proves.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    * Retry until delivered when no DLQ or acks exist
    
    End-to-end acknowledgements are off by default, and a DLQ is optional. In that
    configuration a negative acknowledgement has nowhere to go, so a batch that ran
    out of retries was dropped after about 2.3 minutes of outage at the default
    max_retries.
    
    Such a batch is now retried until it is delivered or the sink shuts down.
    doOutput waits on it, so an outage applies backpressure to the source instead
    of dropping events, and memory stays bounded. max_retries still bounds retries
    when a DLQ is configured or every event has an acknowledgement set.
    
    Permanent errors (InvalidParameterException, HTTP 413) end the request after
    one attempt in both modes, so a batch that no retry can fix does not stall the
    pipeline. The backoff exponent is capped so unbounded retries cannot overflow
    the delay.
    
    Trade-off: while CloudWatch Logs is unavailable, the pipeline worker is 
    blocked, so other sinks in the same pipeline stall with it.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    * Fail fast on missing log groups and fix review issues
    
    - Treat ResourceNotFoundException as permanent when neither
     create_log_group nor create_log_stream is set, so one missing
     destination no longer stalls the pipeline.
    - Add a permanentFailure reason so InvalidParameterException, HTTP 413
     and missing resources are no longer counted as retriesExhausted.
    - Retry log group and stream creation on the next
     ResourceNotFoundException if the previous creation failed.
    - Run the undeliverable, unexpected and refused terminal steps even if
     takeReady throws, so oversized events keep their undeliverable
     handling.
    - Log once when max_retries does not apply because there is no DLQ or
     acknowledgement set.
    - Remove an internal product reference from the README.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    * Mark hot-path logs noisy and close retry gaps
    
    - Mark per-event, per-batch and per-attempt logs NOISY.
    - Stop catching StackOverflowError, as no other sink does.
    - Fail fast on a missing log group when only create_log_stream
     is set.
    - Test that send() retries until delivered only without a DLQ or
     acknowledgement set.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    ---------
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Support Amazon OpenSearch Serverless NextGen time series indexes (#7225)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Fri, 30 Oct 2026 16:05:09 -0700
    
    
    When aws.serverless is true, the opensearch sink detects NextGen time series
    indexes by reading the index.timestamp_field index setting. The result is
    cached per index. A failed lookup is not cached and is retried after the next
    bulk flush, since the flush may create the index.
    
    For create and index actions on a time series index, the sink adds the 
    timestamp field to the document it sends without modifying the event: it keeps
    the field if present, otherwise moves @timestamp to it, otherwise sets it from
    the event time.
    
    When a bulk item is rejected with &#34;document is missing required timestamp field
    [X]&#34;, the sink adds X to that document and retries it in the same flush, and
    records X for the index. This covers indexes that do not exist yet or whose
    settings cannot be read. All other errors keep the existing retry and DLQ
    behavior.
    
    Resolves #7221
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __fix(rds-source): decrement numOfWorkers once per data file when acknowledgments are enabled (#7209)__

    [photoruction-gb](mailto:80666489+photoruction-gb@users.noreply.github.com) - Fri, 30 Oct 2026 13:54:43 -0500
    
    
    With acknowledgments enabled, DataFileScheduler decremented numOfWorkers twice
    per data file: once in runLoader.whenComplete and again in completeDataLoader,
    which the acknowledgment callback calls. The counter went negative, so the
    scheduler kept acquiring data file partitions while the only loader thread was
    busy. Those partitions waited in the executor queue without lease renewal,
    expired, and failed on the final save with ConditionalCheckFailed (Expected
    version: 1), causing the files to be reprocessed.
    
    Move the decrement out of completeDataLoader into the no-acknowledgment 
    whenComplete path, so each data file releases its slot once, when its loader
    finishes. Type runLoader as CompletableFuture&lt;Void&gt;.
    
    Add DataFileSchedulerTest cases with acknowledgments enabled: the worker count
    returns to 0 after an acknowledged file, and no further partition is acquired
    while the loader is busy.
    
    Signed-off-by: Guillaume Bodi &lt;guillaume.bodi@photoruction.com&gt;

* __Add GenAI enrichment settings to otel_traces: on/off switch, root propagation and coverage cache size (#7210)__

    [Shenoy Pratik](mailto:pshenoy36@gmail.com) - Fri, 30 Oct 2026 06:57:20 -0700
    
    
    * Add genai_root_propagation and genai_coverage_cache_max_size to otel_traces
    
    Root span enrichment copied agent data and token totals onto every trace root,
    including the HTTP or RPC span an agent runs behind. Now that agent runs are
    marked with derived.gen_ai.is_root, those copies are redundant and misleading.
    The new genai_root_propagation setting chooses which roots get them: ai_root
    (default) copies only onto a GenAI root or a root marked at the source,
    all_roots keeps the previous behavior, none copies nothing.
    
    The GenAI coverage cache size, previously fixed at 500,000 span ids, is now 
    configurable with genai_coverage_cache_max_size.
    
    Rewrites the README GenAI section to document normalization, sub-key stripping,
    the agent root marker, the coverage cache and its sizing, root propagation
    modes, the token counting rule and the genAiCoverageCount metric.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    * Add genai_enrichment_enabled and address review comments
    
    Adds genai_enrichment_enabled (default true) to turn GenAI enrichment off
    entirely. When off, the processor skips normalization, sub-key stripping,
    marking and root enrichment, and creates no coverage cache.
    
    Review fixes:
    * genai_coverage_cache_max_size must be at least 1.
    * New genAiCoverageSpanIdCount gauge reports the cache&#39;s size in span
     ids, the unit of its limit; genAiCoverageCount counts traces.
    * An unknown genai_root_propagation value now fails with the allowed
     values instead of falling back to the default.
    * README: the cache size is a long, documents the new setting, metric
     and an example config.
    
    Adds config, enum and coverage unit tests, and processor-level tests for each
    propagation mode and for enrichment turned off.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    * Keep test dependencies unchanged and make the span id gauge current
    
    Checks the @Min(1) constraint on genai_coverage_cache_max_size through 
    reflection instead of adding a validator test dependency. Applies pending cache
    writes before reading the weighted size, which Caffeine updates asynchronously,
    so genAiCoverageSpanIdCount reports the current count.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    * Rename the GenAI switch to disable_genai_enrichment and validate GenAI
    settings
    
    * genai_enrichment_enabled (default true) becomes disable_genai_enrichment
     (default false).
    * genai_root_propagation and genai_coverage_cache_max_size cannot be set
     when disable_genai_enrichment is true. Both are now unset by default and
     their getters return the defaults, so the check can tell whether the
     user set them.
    * genai_root_propagation values must match exactly; they are no longer
     lowercased.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt; 
    Co-authored-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;

* __Preserve standard span serialization when routing copies (#7198)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Fri, 30 Oct 2026 14:03:48 +0100
    
    
    * Preserve standard span serialization when routing copies
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Copy standard span metadata independently
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    ---------
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Fix duplicate OTel attribute keys dropping entire OTLP request (#7155)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Thu, 29 Oct 2026 21:00:40 +0100
    
    
    Collectors.toMap without a merge function throws IllegalStateException when any
    OTLP attribute list carries repeated attribute keys. The exception propagated
    to the gRPC service layer, which discarded the whole batch.
    
    Fixed by replacing Collectors.toMap with a mutable-reduction 
    collect(HashMap::new, (m, i) -&gt; m.put(...), Map::putAll) across all
    attribute-converting methods in OTelProtoOpensearchCodec and 
    OTelProtoStandardCodec, covering trace spans, resources, links, events, log
    records, metric data points, and exemplars. Last value wins for duplicates,
    matching the receiver behaviour described in the OTLP spec. The repeated
    pattern is extracted into a private collectAttributes helper in each codec.
    
    Resolves #7151
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Fix duplicate document metric classification (#7207)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Thu, 29 Oct 2026 20:36:18 +0100
    
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __Normalize DocumentDB source s3_prefix so a trailing slash does not change the staging layout (#7215)__

    [Dinu John](mailto:dinujohn@amazon.com) - Tue, 27 Oct 2026 15:46:29 -0500
    
    
    * Normalize DocumentDB source s3_prefix so a trailing slash does not change the
    staging layout
    
    A trailing slash in s3_prefix (for example &#34;prefix/&#34;) was concatenated with the
    source&#39;s own delimiter, producing keys under &#34;prefix//...&#34;. The extra empty
    path segment shifts the folder depth that the S3 read stage partitions on, so
    all staged objects were read as a single folder: slowly, and in key order
    rather than time order.
    
    Strip leading and trailing delimiters and collapse repeated delimiters so
    &#34;prefix&#34;, &#34;prefix/&#34; and &#34;/prefix/&#34; produce the same layout.
    
    Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    
    * Use a generic prefix in DocumentDBServiceTest
    
    Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    
    * Add full license header to DocumentDBServiceTest
    
    Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;

* __Fix giveUpPartition not executing in continuous scan mode (#7214)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 27 Oct 2026 12:39:52 -0500
    
    
    The giveUpPartition call in the continuous scan mode path of 
    ScanObjectWorker.processFolderPartition passed 0 as maxRetries. In
    LeaseBasedSourceCoordinator.giveUpPartition, the retry loop condition is
    &#39;attempt &lt;= maxRetries&#39;, so with 0 the loop body never executes and the
    partition is never actually released.
    
    This caused folder partitions to remain stuck in ASSIGNED status until the
    10-minute ownership lease expired, adding significant latency to continuous
    scan mode object detection-to-processing.
    
    Change maxRetries from 0 to MAX_RETRIES (5) to match the non-continuous scan
    mode path and the error handler.
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix DocumentDB source stream never starting after a long export (#7213)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 27 Oct 2026 11:42:32 -0500
    
    
    The change stream waits for the STREAM global state, which is created once the
    export finishes. On large, long-running exports it could be skipped for good,
    so the pipeline stopped at &#34;Initial load not complete&#34;:
    
    - ExportScheduler: the write that marks the total partition count as final
     is retried when it loses a version race with an export worker. The export
     partition is already completed at that point and is never acquired again,
     so a dropped write left the flag false forever. If every partition is
     already loaded when the flag is written, the scheduler starts the stream.
    - ExportWorker: a data query partition is completed before it is counted, so
     the late acknowledgment of an owner whose lease expired is not counted a
     second time. The stream starts once loaded &gt;= total instead of only on
     exact equality, which an earlier double count could step over.
    
    Signed-off-by: Dinu John &lt;dinujohn@amazon.com&gt;

* __Mark the outermost GenAI span of traces with a non-GenAI root (#7192)__

    [Shenoy Pratik](mailto:pshenoy36@gmail.com) - Wed, 21 Oct 2026 19:14:30 -0700
    
    
    * Mark the outermost GenAI span of traces with a non-GenAI root
    
    An agent served behind an HTTP handler has an HTTP root span, and the GenAI
    spans (invoke_agent, chat, execute_tool) are its descendants. Consumers that
    list agent traces by their root, such as OpenSearch Dashboards Agent Traces,
    miss these traces (#7191).
    
    When a trace&#39;s root is not a GenAI span, otel_traces now sets 
    derived.gen_ai.is_root = true on each outermost GenAI span (a span with 
    gen_ai.operation.name and no GenAI ancestor) and aggregates its descendants&#39;
    tokens onto it. Spans flushed without their root are still marked. A marker
    already set by an SDK or by hand is kept, and the processor then marks nothing
    else in the trace.
    
    Root propagation of gen_ai.agent.name, gen_ai.request.model and the other keys
    now takes values from the outermost GenAI spans first, instead of whichever
    child came first in the batch. The README no longer says gen_ai.operation.name
    is propagated.
    
    Resolves #7191
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    * Count each span&#39;s token usage once; cover agents calling agents
    
    Root enrichment summed token usage over every child, and the agent root marker
    did the same over its descendants. Agent spans such as invoke_agent often
    report the total of their own LLM calls, so those calls were counted twice; on
    a real Strands trace the root showed about twice the actual usage. Both now
    count only spans that have no ancestor reporting usage.
    
    Adds tests for an agent that calls another agent over HTTP in the same trace:
    only the calling agent is marked, and the called agent is marked only when its
    spans are flushed without the caller&#39;s. The README documents both.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    * Mark GenAI roots too; don&#39;t mark spans that arrive after their ancestors
    
    - The agent root marker now also goes on a trace root that is a GenAI
     span, so every agent trace carries one marker, wherever the agent
     sits.
    - Spans that arrive after their trace&#39;s root was processed are enriched
     on their own, without their ancestors, so a late chat span or the
     spans of a called service looked outermost and were marked. The
     processor now remembers, per trace and for trace_group_cache_ttl,
     which span ids are covered by a GenAI span or a marker (bounded by
     span id count), and looks there when a parent is not in the batch.
    - A span marked at the source also covers the spans below it.
    - README documents the remaining edge cases.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    * Address review: atomic per-trace coverage, one index per trace, safer values
    
    - Coverage now outlives the orphan flush (2 x trace_flush_interval +
     trace_group_cache_ttl), so spans flushed without their root still see
     what earlier batches covered; its size is reported as genAiCoverageCount.
    - The covered read, the marking and the write run atomically per trace
     (Caffeine compute), so pipeline workers handling spans of one trace at
     the same time cannot both mark an outermost span.
    - Each trace is indexed once: attributes are read once per span, traces
     without GenAI data are skipped, and one ancestor walk serves marking,
     coverage and token counting.
    - Start times compare as instants, not text (fractional precision).
    - An agent span reporting only output tokens keeps its value; the root
     check is symmetric too.
    - Token counts and propagated values are read tolerantly: numeric strings
     count, anything else is ignored instead of failing the batch.
    - Propagation uses the coverage too, like marking.
    - Only derived.gen_ai.is_root: true is a source marker; a span set to
     false is never marked. A debug log notes traces skipped for a marker.
    - README: the coverage window, the marker values, and the token rule&#39;s
     assumption that an ancestor&#39;s usage includes its subtree.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt; 
    Co-authored-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;

* __Map Traceloop session ids to gen_ai.conversation.id (#7173)__

    [Shenoy Pratik](mailto:pshenoy36@gmail.com) - Wed, 21 Oct 2026 18:55:09 -0700
    
    
    * Map Traceloop session ids to gen_ai.conversation.id
    
    Map traceloop.association.properties.{session_id,conversation_id,thread_id} to
    gen_ai.conversation.id in the openllmetry profile, so OpenLLMetry apps that set
    a session through association properties get the OTel GenAI attribute.
    
    The id is normalized on the span that carries it and is not copied to the root
    span: it is the key that groups traces into sessions, and the OTel GenAI 
    semantic conventions define no root-span or propagation rule for it (they ask 
    instrumentations to set it where it is readily available, including 
    execute_tool spans since semantic-conventions-genai#518).
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    * Pick the first-listed GenAI mapping source; drop guessed Traceloop keys
    
    When several source attributes on one span mapped to the same gen_ai.* target,
    normalizeAttributes checked a copy of the attributes taken before the loop, so
    every later source overwrote the earlier one and the winner depended on
    attribute order. It now picks one source per target first, by position in the
    mappings file, then writes.
    
    Drops traceloop.association.properties.conversation_id and thread_id: Traceloop
    does not set them as association properties. Its set_conversation_id() and
    LangGraph thread_id handling write gen_ai.conversation.id directly. session_id
    stays: it is Traceloop&#39;s standard AssociationProperty.SESSION_ID.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    * Test GenAI mapping priority: file order, duplicates, skipped entries
    
    Covers how buildLookupTable assigns priorities (file order across profiles, a
    duplicate source keeping its first target and priority, malformed entries
    taking no priority), and that the shipped OpenInference session.id outranks the
    Traceloop session_id mapping.
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt; 
    Co-authored-by: Shenoy Pratik Gurudatt &lt;4348487+ps48@users.noreply.github.com&gt;

* __fix(cloudwatch-logs): drop entity when a resolved value is blank or too long (#7203)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Wed, 21 Oct 2026 18:33:24 -0500
    
    
    In dynamic-entity mode, a templated entity value that resolved to empty or over
    512 characters made PutLogEvents reject the whole batch with a 400, which the
    sink retried indefinitely. Such events are now sent without an entity and
    counted in cloudWatchLogsEntityDropped.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __fix(source-crawler): renew lease during ack wait (#7202)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Wed, 21 Oct 2026 15:14:14 -0500
    
    
    Worker partitions hold a 10 minute lease that was never renewed, while their
    acknowledgement set waits up to 2 hours. A sink stalled for more than 10
    minutes let the lease expire, so the partition was re-acquired and its events
    re-emitted every 10 minutes, leaking an acknowledgement set each time.
    
    Renew the lease every 2 minutes from the acknowledgement set&#39;s progress check,
    as s3-source does, through a new renewPartitionOwnership on 
    EnhancedSourceCoordinator. A blocked worker keeps its partition and a dead one
    releases it after one lease. Writes for a partition are serialized, and after a
    failed renewal the worker stops writing it and lets the lease lapse, so a stale
    write cannot overwrite a new owner.
    
    The ack callback also counted a success before completePartition ran, so a
    failed completion was counted as a success and its exception lost. Count a
    success only after completion, and count failures in a new 
    acknowledgementCallbackFailures metric.
    
    Resolves #7200
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __fix(kafka): pause and call poll() immediately on writeAll failure (#7196)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 19 Oct 2026 13:43:14 -0500
    
    
    When the Kafka consumer buffer writeAll call fails (buffer full or circuit 
    breaker), the consumer now pauses immediately on the first failure and calls
    doPoll() on every retry iteration. This keeps the member in the group by
    driving the network I/O that reads JoinGroup/SyncGroup responses and allows the
    heartbeat thread to function during rebalances.
    
    Previously, the consumer would retry writeAll for maxRetriesOnException 
    iterations (calculated as max_poll_interval / 2000) with a 1s sleep between
    each retry before pausing. During a rebalance, the heartbeat thread disables
    itself after JoinGroup and only re-enables when poll() processes the JoinGroup
    response. Without poll() being called, the coordinator&#39;s JoinGroup response
    sits unread in the socket, and after session.timeout.ms (60s) expires with no
    heartbeat or SyncGroup, the coordinator removes the member. The main thread
    then gets UNKNOWN_MEMBER_ID on its next poll, triggering another rebalance.
    
    With this fix, poll() is called on every retry, so JoinGroup/SyncGroup 
    responses are processed promptly and the heartbeat thread stays active. 
    Rebalances complete in seconds even when the buffer is full.
    
    Also adds doPoll() in the pauseConsumePredicate path so that external pause
    conditions (like circuit breaker) also maintain group membership.
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Make in-memory buffer and batch size configurable for kafka buffer, lower default values (#7194)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Sun, 18 Oct 2026 11:59:27 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add trace-analytics-plain-raw-v2 index type (#7188)__

    [Suresh N S](mailto:41610499+nssuresh2007@users.noreply.github.com) - Fri, 16 Oct 2026 09:14:38 -0700
    
    
    * Add trace-analytics-plain-raw-v2 index type
    
    Add an opensearch sink index_type, trace-analytics-plain-raw-v2, that writes
    the same span documents as trace-analytics-plain-raw to a new otel-v2-apm-span
    alias, following the otel-v2-apm-service-map pattern. It has its own template
    (V1 and index-template formats) and its own ISM policy,
    otel-v2-apm-span-policy, whose ISM template matches otel-v2-apm-span-*.
    
    The template is the plain span template plus:
    - keyword mappings for string fields under the nested events.attributes
     and links.attributes, for clusters that do not support text fields
     inside nested fields;
    - index.append_only.enabled: false, so that spans can be written with
     the document ID ${traceId}/${spanId} on clusters where new indices
     are append-only by default. This setting requires OpenSearch 3.0.
    
    Signed-off-by: Suresh N S &lt;nssuresh@amazon.com&gt;
    
    * Compose the v2 span template from component templates
    
    Build the trace-analytics-plain-raw-v2 index template from component templates
    instead of a full copy of the plain span template, so the span mappings are
    defined in one file only.
    
    The index template now only sets index.append_only.enabled: false and composes,
    in order:
    - otel-v2-apm-span-plain: the template block and version of the
     existing plain span template (otel-v1-apm-span-index-standard-
     template.json), built at startup;
    - otel-v2-apm-span-nested-keyword: keyword mappings for strings under
     events.attributes and links.attributes.
    
    The sink creates or updates these component templates before the index
    template, each only when its version is higher than the one on the cluster,
    using the same check as for index templates. Component templates configured
    through component_templates are still appended after them.
    
    Because a V1 template cannot compose component templates, this index type
    always uses template_type index-template and is rejected with 
    distribution_version es6. The legacy V1-format copy of the v2 template is
    removed.
    
    Signed-off-by: Suresh N S &lt;nssuresh@amazon.com&gt;
    
    * Add full license header to component template wrapper
    
    The license header check requires all five header lines from CONTRIBUTING.md;
    the new wrapper and its test only had the first two.
    
    Signed-off-by: Suresh N S &lt;nssuresh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Suresh N S &lt;nssuresh@amazon.com&gt;

* __fix(kafka): apply sts_header_overrides to MSK IAM SASL auth (#7187)__

    [hozkaya2000](mailto:63765239+hozkaya2000@users.noreply.github.com) - Tue, 13 Oct 2026 10:09:31 -0700
    
    
    Route role-based AWS_MSK_IAM SASL (data-plane) authentication through a
    dedicated callback handler that sources credentials exclusively from Data
    Prepper&#39;s header-aware STS provider, so aws.sts_header_overrides
    (x-amz-source-arn / x-amz-source-account) are applied on the data-plane
    sts:AssumeRole used for broker auth.
    
    The handler implements AuthenticateCallbackHandler directly (never extends
    IAMClientCallbackHandler), so the vendored library never assembles its own
    header-less MSKCredentialProvider. The ROLE path is now unconditional:
    credentials always flow through our provider regardless of whether overrides
    are configured.
    
    Resolves #6923
    
    Signed-off-by: Halit Ozkaya &lt;hozkaya@amazon.com&gt;

* __Upgrade OpenTelemetry proto Java bindings and add event_name support for log records (#7148)__

    [Karsten Schnitter](mailto:k.schnitter@sap.com) - Tue, 13 Oct 2026 15:47:23 +0200
    
    
    * Upgrade OpenTelemetry proto Java bindings to 1.11.0-alpha
    
    Updates io.opentelemetry.proto:opentelemetry-proto from 1.3.2-alpha to 
    1.11.0-alpha, bringing support for all OTLP protocol changes through proto
    v1.11.0 including the stabilised event_name field in LogRecord, EntityRef in
    Resource definitions, and the new ProcessContext message.
    
    Resolves #1269
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Add support for event_name field in OTel log records
    
    Maps the top-level LogRecord.event_name field (introduced in OTel proto v1.5.0)
    to a new eventName property on OpenTelemetryLog. Both the standard and
    OpenSearch codecs now decode the field from incoming requests, and the standard
    codec encoder writes it back when re-serialising logs to protobuf.
    
    Resolves #6505
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Generate OTel proto Java classes from source to keep protobuf-java at 3.25.5
    
    opentelemetry-proto:1.11.0-alpha requires protobuf-java:4.35.1, which conflicts
    with the project-wide pin of protobuf-java:3.25.5.
    
    Instead of consuming the pre-built Maven JAR, generate the Java classes and
    gRPC service stubs directly from the .proto files bundled in the 
    opentelemetry-proto:1.11.0-alpha:sources@jar artifact using protoc:3.25.5 and
    protoc-gen-grpc-java:1.70.0. This keeps protobuf-java firmly at 3.25.5 across
    all modules.
    
    Changes:
    - otel-proto-common: add com.google.protobuf Gradle plugin v0.9.4,
     switch to java-library, generate proto and gRPC classes from sources
     JAR, expose protobuf-util/grpc-stub/grpc-protobuf as api dependencies
    - Remove direct opentelemetry-proto dependency from all other modules
     (otel-trace-source, otel-logs-source, otel-metrics-source, otlp-source,
     otel-trace-raw-processor, otel-metrics-raw-processor, http-common,
     otlp-sink, kafka-plugins, aggregate-processor, service-map-stateful,
     all e2e-test projects)
    - service-map-stateful: replace direct proto artifact with project dep
     on otel-proto-common, removing the now-unnecessary protobuf-java exclude
    - Remove opentelemetry version catalog entry from settings.gradle
    - Exclude generated proto sources from Spotless and Checkstyle
    - Fix implicit Spotless/antlr task dependency in data-prepper-expression
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix e2e test compilation: add otel-proto-common dependency
    
    The e2e trace and log tests import io.opentelemetry.proto.* classes directly.
    After switching otel-proto-common to generate those classes from source, the
    e2e projects need an explicit dependency on otel-proto-common to get them on
    the integration test compile classpath.
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    * Fix Gradle 8.8 implicit task dependency in
    data-prepper-logstash-configuration
    
    Gradle 8.8 detects that spotlessJava reads from the ANTLR output directory 
    produced by generateGrammarSource without a declared dependency, causing a
    build failure when the two tasks run in parallel.
    
    Add an explicit dependsOn to satisfy Gradle&#39;s validation and restrict the
    Spotless target to src/ so the generated files are not formatted.
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;
    
    ---------
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt;

* __fix(opensearch-sink): S3 DLQ takes precedence over failure pipeline (#7189)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 12 Oct 2026 19:27:20 -0500
    
    
    Previously, when both a per-sink S3 DLQ and a global failure pipeline
    (dlq_pipeline) were configured, the failure pipeline always took precedence.
    This meant the S3 DLQ configured on the OpenSearch sink was never used.
    
    This caused problems when the failure pipeline writes to the same OpenSearch
    cluster that is experiencing issues — the failure pipeline would also fail,
    blocking acknowledgements indefinitely (with acks enabled) or causing negative
    acks upstream.
    
    New priority order for handling failed documents: 1. S3 DLQ file writer
    (dlqFileWriter) — if configured 2. S3 DLQ writer (dlqWriter) — if configured on
    the sink 3. Failure pipeline (dlq_pipeline) — fallback if no sink-level DLQ 4.
    Log warning and release(false) — nothing configured
    
    If S3 DLQ write fails, fall back to the failure pipeline. If no failure 
    pipeline exists either, release with false.
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __fix(core): Skip circuit breaker for DLQ pipeline buffer (#7185)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Thu, 8 Oct 2026 13:58:36 -0500
    
    
    The DLQ (failure) pipeline uses a HeadlessPipelineSource which writes events to
    its buffer via sendEvents(). When a circuit breaker is active, the buffer is
    wrapped with CircuitBreakingBuffer which throws TimeoutException when the
    breaker is open.
    
    This causes DLQ writes to block indefinitely (with acks enabled) or release
    with false (without acks), preventing acknowledgements from completing
    upstream. Since the DLQ pipeline exists specifically to drain failed events,
    blocking it under memory pressure defeats its purpose and can cascade into
    negative acknowledgements on the source pipeline&#39;s Kafka buffer.
    
    Skip applying CircuitBreakingBuffer to HeadlessPipelineSource pipelines, 
    matching the existing skip for PipelineConnector sources.
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __fix(prometheus-sink): Retry on unprocessed requests (GOAWAY, connection reset) (#7182)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 5 Oct 2026 23:11:35 -0500
    
    
    Add retry handling for UnprocessedRequestException in the Prometheus sink&#39;s 
    Armeria WebClient. Previously, only HTTP status codes 429, 502, 503, and 504 
    triggered retries. Connection-level failures like HTTP/2 GOAWAY frames, 
    connection resets, and connection refused errors were not retried, causing the
    request to fail immediately and trigger negative acknowledgements upstream in
    the pipeline.
    
    HTTP/2 GOAWAY is a normal connection lifecycle event sent by load balancers and
    server proxies during connection rotation. The request was never processed by
    the server, so it is safe to retry on a new connection.
    
    Changes:
    - Add .onUnprocessed() to the Armeria RetryRuleWithContent chain, which
     matches UnprocessedRequestException (covers GoAwayReceivedException,
     connection reset, connection refused)
    - Extract shared Backoff instance to avoid duplication
    - Add unit tests for GoAwayReceivedException handling
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Support continuous scan mode on RDS source (#7181)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 5 Oct 2026 16:10:31 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Support disabling the ttl cleanup on documentdb source with s3 source folder partitioning (#7172)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 5 Oct 2026 12:18:55 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __[aws-lambda] Add retryable_status_codes option to retry transient HTTP status codes (#7165)__

    [Luis Norman](mailto:lanorman14@gmail.com) - Fri, 2 Oct 2026 13:06:53 -0700
    
    
    Signed-off-by: LuisNorman &lt;lunorman@amazon.com&gt; Co-authored-by: LuisNorman
    &lt;lunorman@amazon.com&gt;

* __Make DimensionalTimeSliceCrawler partition creation wait configurable (#7179)__

    [lksfrnz](mailto:lksfrnz@amazon.com) - Thu, 1 Oct 2026 20:58:33 -0500
    
    
    Adds CrawlerSourceConfig.getPartitionCreationWait() (default 5 minutes). 
    DimensionalTimeSliceCrawler now receives the source config through an
    @Inject constructor and uses that wait instead of the hard-coded 5 minutes, so
    a source changes the wait by overriding the getter. The wait must be between 0
    and 60 minutes and is validated when the crawler is created. The existing
    (CrawlerClient, PluginMetrics) constructor keeps the 5-minute default.
    
    Signed-off-by: Lukas Franz &lt;lksfrnz@amazon.com&gt;

* __Emit typed external-dependency nodes and RED metrics in APM service map processor (#7147)__

    [Adam Tackett](mailto:105462877+TackAdam@users.noreply.github.com) - Thu, 1 Oct 2026 11:40:22 -0700
    
    
    * feat: synthesize typed external-dependency nodes and RED metrics in APM
    service map processor
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * feat: emit per-type dependency identity attributes on synthesized nodes
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * feat: suppress unresolved/raw-IP dependency peers; add tests and docs
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * fix(apm): address review — exclude dep attributes from node identity, gate by
    span kind, inherit env, harden URL parsing, hoist providers
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * fix(apm): address bot re-review - keep AWS::*/ARN dependency names, strict
    IP-literal filter, safer URL path parsing, exact span-kind match
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * chore: retrigger CI (unrelated flaky failures)
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * fix(apm): gate dependency nodes behind opt-in flag, cap cardinality, name DBs
    by instance, fix double counts
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * fix(apm): add .keyword sub-field to dependencyAttributes template (v2),
    document host-less DB nodes and query paths, lock in DB name fallback
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * fix(apm): admit capped dependencies by call count, keep overflow out of the
    operation budget, make the overflow metrics per-call counters, log derivation
    failures
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * chore(apm): use Javadoc for new fields; set service map template version to 1
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * feat(apm): enable dependency_nodes by default
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    * fix(apm): sticky dependency admission across windows, per-type overflow
    names, spanKind label on messaging series; document rollout, naming spec and
    data contract
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Adam Tackett &lt;tackadam@amazon.com&gt; Co-authored-by: Adam Tackett
    &lt;tackadam@amazon.com&gt;

* __Remove stale tableId mapping in RDS source when a table id is reused (#7162)__

    [gg-mskm](mailto:masakide0514gg@gmail.com) - Wed, 30 Sep 2026 15:38:22 -0500
    
    
    The MySQL RDS source keeps an in-memory `tableId -&gt; TableMetadata` map that is 
    built from binlog TABLE_MAP events. `handleTableMapEvent` returned early for
    any table that is not of interest without touching the map, so an entry
    recorded for a table of interest survived indefinitely.
    
    The binlog `table_id` is assigned by the server from its table definition cache 
    and is not stable: it can be reassigned to a different table across a server 
    restart, and also when a table is evicted from and reloaded into the cache.
    When a table id previously bound to a table of interest was reassigned to a
    table that is not of interest, `isValidTableId` still accepted the row events
    for that table because the stale entry was present and pointed at a table of
    interest. Those rows were then decoded using the wrong column definitions,
    which silently wrote corrupted documents to the sink and raised type-conversion
    errors for the rows whose types did not line up.
    
    Remove the mapping held for a table id whenever a TABLE_MAP event shows the id 
    now refers to a table that is not of interest, so subsequent row events for
    that id are skipped as intended.
    
    Resolves #7125
    
    Signed-off-by: Masaki Mori &lt;masakigg@amazon.co.jp&gt; Co-authored-by: Masaki Mori
    &lt;masakigg@amazon.co.jp&gt;

* __Consolidates the code for configuring 64 MB when deserializing from JSON. (#7167)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 29 Sep 2026 12:24:42 -0700
    
    
    Adds a new JsonFactories class that creates a default JsonFactory. Use this in
    code where we updated to 64 MB already and in the ndjson and json codecs.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add component_templates option to the opensearch sink (#7161)__

    [Vinay Krishna Pudyodu](mailto:vinkrish.neo@gmail.com) - Mon, 28 Sep 2026 17:37:24 -0700
    
    
    Adds the configured component template names to the composed_of list of the 
    index template the sink creates, including the built-in index_type templates. 
    This lets users layer index settings (e.g. index.codec, or a parquet-backed 
    data format) onto built-in templates such as trace-analytics-plain-raw without 
    copying them. Requires template_type index-template.
    
    Resolves #7142
    
    Signed-off-by: Vinay Krishna Pudyodu &lt;vinkrish.neo@gmail.com&gt;

* __Now that we support 64 MB strings in Events we also need to support this larger value on the event_json codec. (#7166)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 28 Sep 2026 17:04:39 -0700
    
    
    Updates the EventJsonInputCodec to support a 64 MB maximum string length.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __feat(cloudwatch-logs): Send dynamic per-request headers (#7157)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Wed, 23 Sep 2026 15:27:08 -0500
    
    
    The sink could only attach headers once at client build, so every PutLogEvents
    request carried the same values. Templated values in header_overrides are now
    resolved per event and attached to their own request, letting a single sink
    label each request with headers derived from the events it carries.
    
    Header values join the existing entity key attributes in the grouping key, so
    events that differ only by a header are sent as separate requests while events
    that share a key are batched into one. Literal header values keep the original
    client-level behavior.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __fix(cloudwatch-logs): Count UTF-8 bytes (#7154)__

    [James Liu](mailto:71547730+Jiaaming@users.noreply.github.com) - Tue, 22 Sep 2026 15:31:38 -0500
    
    
    Measure event size using the exact UTF-8 bytes buffered for delivery. This
    prevents request and event size checks from undercounting non-ASCII log data.
    
    Signed-off-by: James Liu &lt;jamesliu.jiaming@gmail.com&gt;

* __test(mongodb): fix flaky ExportSchedulerTest await anchors (#7111)__

    [kiran536](mailto:veeravelly.kirankumar@gmail.com) - Sat, 19 Sep 2026 05:40:42 -0700
    
    
    Three tests in ExportSchedulerTest awaited an intermediate coordinator 
    interaction and then called future.cancel(true), which can interrupt 
    ExportScheduler.run() before it makes the remaining calls the assertions 
    expect. Observed in CI as:
    
      TooFewActualInvocations: coordinator.getPartition(...)
     Wanted 3 times:  ExportSchedulerTest.java:173
     But was 2 times: ExportScheduler.run(ExportScheduler.java:79)
                      ExportScheduler.run(ExportScheduler.java:83)
    
    Line 172, the getPartition inside markTotalPartitionsAsComplete, never ran
    because cancel landed after createPartition but before it.
    
    Anchor each await on the last coordinator interaction of the scheduler&#39;s 
    iteration, saveProgressStateForPartition(globalState, ...), which every other
    assertion in these tests happens at or before.
    
    Contributes to #3481.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt; Co-authored-by:
    Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;

* __Fix flaky EndToEndRawSpanTest in peer forwarder e2e tests (#6981)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Sat, 19 Sep 2026 05:38:43 -0700
    
    
    * Fix flaky EndToEndRawSpanTest in peer forwarder e2e tests
    
    Raise trace_flush_interval from 5 to 30 in the raw-span e2e pipeline configs so
    a peer-forwarded root span is not garbage-collected away from its buffered
    children, and report all spans with missing fields via soft assertions instead
    of throwing on the first one.
    
    Ref #6721
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Use a dedicated pipeline config for the peer forwarder e2e test
    
    rawSpanEndToEndTest runs two nodes without peer forwarding and relies on the 5
    second trace_flush_interval to release buffered child spans for
    otel_trace_group backfill, so raising the shared value broke it. Keep
    raw-span-e2e-pipeline.yml at 5 and give rawSpanPeerForwarderEndToEndTest its
    own config with trace_flush_interval 30.
    
    Ref #6721
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Add license header to new e2e pipeline config
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    ---------
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Add Splunk HEC source plugin (RFC #6823)   (#7105)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Thu, 17 Sep 2026 16:51:52 -0500
    
    
    * Add a Splunk HTTP Event Collector (HEC) source plugin
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Add a getDefaultSsl override hook to the HTTP server configuration
    
    Sources which should be secure by default can override getDefaultSsl to enable
    ssl when the pipeline author does not set it, mirroring the existing
    getDefaultPort and getDefaultPath hooks. The interface method defaults to false
    so that every existing HTTP source is unchanged.
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Use BackgroundThreadFactory for the Splunk HEC ack cleanup thread
    
    Replaces the hand-rolled ThreadFactory with the shared BackgroundThreadFactory
    for consistency with the rest of the project.
    
    Also switches the expired ack cleanup from compute to computeIfPresent. The two
    are equivalent here, but compute required a null-state guard that no test could
    reach, which held the module below its required JaCoCo coverage minimum of 1.0.
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Build Splunk HEC events with EventFactory and share the ingest path
    
    Extracts every event construction concern into HecEventBuilder, which builds
    events through the injected EventFactory instead of JacksonEvent.builder.
    Metadata resolution, flattening, field merging and timestamp parsing now live
    in a plain class that HecEventBuilderTest covers directly with 32 tests,
    without standing up an HTTP port.
    
    Collapses processEventWithAck, processEventWithoutAck and processRawRequest
    into a single handleIngestRequest entry point plus a shared writeRecords.
    Authentication, data channel validation, request metrics and the buffer write
    now run on one path that no endpoint can bypass, and /ack authenticates before
    reporting whether acknowledgements are enabled. This takes SplunkHecService
    from 745 to 389 lines.
    
    Two inconsistencies surface from sharing the path: /raw now sets the same
    sourcetype, source and host metadata attributes that /event sets, and /raw now
    applies the per-token default fields it previously ignored. A failed
    acknowledgement also removes the pending ack rather than leaving it pending
    until it expires.
    
    Configuration changes:
    
    * ssl now defaults to true, so the certificate and key are required
     unless ssl is explicitly disabled.
    * Configuring the shared authentication plugin is rejected, since the
     HEC protocol authenticates with the tokens option and two concurrent
     authentication mechanisms would be ambiguous.
    * Renames ack_expiry to acknowledgement_expiry.
    * Removes use_forwarded_headers, which nothing read.
    * Documents why default_sourcetype is a free-form string rather than an
     enum: Splunk sourcetypes are defined per deployment with no fixed set.
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Update the Splunk HEC source README for the configuration changes
    
    Documents the acknowledgement_expiry rename, the removal of 
    use_forwarded_headers, the secure-by-default ssl behavior, why the 
    authentication plugin is not supported alongside HEC tokens, why 
    default_sourcetype is a free-form string, and that both endpoints now attach
    the same routing metadata.
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    ---------
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Increase the maximum string length in an Event to 64 MB. (#7146)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 16 Sep 2026 12:10:07 -0700
    
    
    Resolves #7145.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Parse arrays in parse_json and parse_ion. (#7141)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 15 Sep 2026 18:35:25 -0700
    
    
    * Parse arrays in parse_json and parse_ion.
    
    The parse_json and parse_ion processors currently do not support parsing arrays
    that are not enclosed in an object. This change has them parse the arrays if
    the `destination` is set. It is not possible to parse into the root, so the
    parse will fail if no destination field is set when an array is detected.
    Additionally, this implementation also allows parsing of scalar values.
    
    Resolves #5705
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    * Fix the pointer path bug found by the PR auto-review.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    
    ---------
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add fields parameter to Jira /rest/api/3/search/jql request (#7144)__

    [Harsha Siva Sai Sannapaneni](mailto:harshasivasai85@gmail.com) - Tue, 15 Sep 2026 14:28:18 -0700
    
    
    The migration from /rest/api/3/search to /rest/api/3/search/jql (#7106) did not
    account for a behavior difference: the old endpoint returned all navigable
    fields by default, whereas the new endpoint returns only issue IDs. Without the
    fields parameter, IssueBean.fields is null, causing a NullPointerException in
    JiraItemInfo.getKeyAttributes() on every crawl cycle.
    
    Fix: add fields=project,created,updated to the search/jql request. These are
    the only fields consumed from the search response by JiraItemInfo.
    
    Resolves #7143
    
    Signed-off-by: Harsha Siva Sai Sannapaneni &lt;sivasais@amazon.com&gt; 
    Co-authored-by: Harsha Siva Sai Sannapaneni &lt;sivasais@amazon.com&gt;

* __Fix NPE when a cascading-action column is updated to NULL in the rds source (#7137)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 11 Sep 2026 08:37:29 -0700
    
    
    Currently, the rds source throws a NullPointerException and drops the entire
    change event when an UPDATE sets a column to NULL on a table that is a parent
    in a foreign key relationship with a cascading action (CASCADE, SET_NULL, or
    SET_DEFAULT).
    
    This change builds the changed-columns map with a HashMap instead of
    Collectors.toMap(), which rejects null values. It also adds unit tests covering
    an UPDATE that sets a cascading column to NULL.
    
    Resolves #7129
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Creates a new configuration for acknowledgements and makes the shutdown timeout configurable. (#7127)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 8 Sep 2026 12:11:03 -0700
    
    
    This adds a new acknowledgements section in data-prepper-config.yaml and
    includes a shutdown_timeout which is a configurable Duration. It retains the
    same default value of five seconds as is currently in use.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __feat(key-value): add destination_conflict_strategy for per-field merge in key-value processor (#7101)__

    [lfpantin](mailto:lmayaud@amazon.com) - Tue, 1 Sep 2026 13:52:33 -0700
    
    
    feat(key-value): add destination_conflict_strategy to key-value processor
    
    The key-value setting `overwrite_if_destination_exists` was all-or-nothing when
    a destination was set: it replaced the whole destination map or skipped the
    write entirely.
    
    This commit adds `destination_conflict_strategy` with skip, overwrite, 
    merge_preserve_existing_keys, and merge_overwrite_existing_keys. The merge 
    strategies resolve conflicts per-field on an existing destination map.
    
    overwrite_if_destination_exists is deprecated but still honored (false -&gt; skip,
    true -&gt; overwrite); setting both options is rejected. The default stays
    overwrite, so existing pipelines are unaffected. Write logic is extracted into
    a reusable EventDestinationWriter, with unit, processor, and integration tests
    plus a README update.
    
    Resolves #7091
    
    Signed-off-by: Luis Pantin &lt;lmayaud@amazon.com&gt;

* __feat(osgi): add opt-in Apache Felix plugin framework (#6917)__

    [kiran536](mailto:veeravelly.kirankumar@gmail.com) - Tue, 1 Sep 2026 08:31:16 -0700
    
    
    * feat(osgi): add opt-in Apache Felix plugin framework
    
    Add an opt-in plugin-loading mode backed by an embedded Apache Felix OSGi
    framework, enabled with -Ddata-prepper.plugin.framework=osgi. When the property
    is unset, Data Prepper uses the existing classpath loader. In that legacy mode
    plugin discovery is functionally identical to before: the same ServiceLoader
    providers are found in the same order, the OSGi framework never starts, no OSGi
    code path executes, and only a couple of inert Spring beans are constructed.
    
    Plugins are prepared for OSGi at build time rather than adapted at runtime. A
    new Gradle plugin module data-prepper-gradle-plugins/osgi-plugin
    (plugin id org.opensearch.dataprepper.plugin) bakes an OSGi manifest into the
    plugin JAR at build time using bnd (biz.aQute.bnd). It is consumed through
    includeBuild in pluginManagement and is not published to any repository yet, so
    for now only projects in this repository can apply it; in-repo plugins opt in
    by applying the plugin id. It does not apply maven-publish, since no
    publication or repository is configured for it. There is no runtime adaptation
    and no BundleAdapter. At startup in OSGi mode StaticBundleLoader installs the 
    pre-built bundle JARs directly, resolves and starts them with fail-fast 
    behavior, a startup summary log, and Micrometer metrics. A JAR that lacks an
    OSGi manifest (no Bundle-SymbolicName) fails fast with an actionable message
    pointing the author to the Gradle plugin.
    
    The plugin-framework-osgi module embeds the Felix lifecycle
    (FelixPluginManager), bootstraps it in OSGi mode through a Spring-managed 
    runner (OsgiFrameworkRunner), and exposes a PluginProvider backed by the OSGi
    service registry (OsgiPluginRegistry). Baked plugins declare their
    @DataPrepperPlugin packages through the DataPrepper-Plugin-Classes manifest
    header, which the Gradle plugin generates from 
    META-INF/data-prepper.plugins.properties, and LegacyPluginBundleActivator 
    registers those classes as OSGi services.
    
    Data Prepper API packages are exported to bundles with semver version 
    attributes. The exported package set is generated at build time by a Gradle
    task that writes META-INF/osgi-shared-packages.properties listing every
    org.opensearch.dataprepper.* package except 
    org.opensearch.dataprepper.plugins.*, so there is no hand-maintained list. The
    version comes from the existing data-prepper-api DataPrepperVersion, formatted
    into the OSGi three-part string by a small package-private toOsgiVersion
    helper; the separate DataPrepperApiVersion class was removed. This is
    consistent with the data-prepper-api backward-compatibility contract from
    #6607, so a plugin built on 2.15 resolves on a 2.16 host. The shared set also
    includes jakarta.validation, jakarta.validation.constraints, and
    com.fasterxml.jackson.annotation
    (annotations only: jackson-databind and jackson-core are intentionally not
    exported, preserving isolation), plus org.slf4j and the framework package
    org.opensearch.dataprepper.plugin.osgi so bundles can load 
    LegacyPluginBundleActivator from the system bundle.
    
    Felix framework storage resolves under 
    System.getProperty(&#34;data-prepper.dir&#34;)/data/osgi/felix-cache, matching the 
    MaxMindConfig idiom, with a java.io.tmpdir fallback when the property is unset
    such as in tests. It does not use the system temp directory by default. The
    directory name is fixed rather than process-unique so that 
    FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT actually reclaims it on the next start; a
    per-process name would orphan one cache directory per restart and grow without
    bound on a long-lived host or a container with a persistent data volume. A
    fixed name is safe because a single framework instance owns the directory for
    the life of the process, and a cache left by an unclean shutdown is cleaned on
    the next first init. Caches orphaned by the earlier process-suffixed naming are
    purged at startup. Only the tmpdir test fallback keeps a PID suffix, where
    concurrent test JVMs would otherwise share one directory.
    
    The OSGi service registry is the single source of truth for plugin discovery.
    LegacyPluginBundleActivator loads each bundle&#39;s annotated classes once at
    bundle start and publishes one service per plugin class, carrying every name
    the plugin answers to — its name, deprecated name, and alternate names — as a
    multi-valued service property. OsgiPluginRegistry reads those properties,
    selected with an LDAP filter on dataprepper.plugin.name, and resolves the named
    classes through the publishing bundle rather than re-scanning bundles for
    annotations, so no plugin class is loaded twice and no unrelated service is
    force-activated through getService(). Fragment bundles are attached at resolve
    time and cannot be started, so they are excluded from the started-bundle list
    and from the bundlesLoaded counter rather than inflating them; the startup 
    summary still lists them, reported by their actual state. BundleHealthCheck
    reports a bundle whose classloader cannot be verified as not isolated, agreeing
    with its own null-wiring path, instead of swallowing the error and passing.
    
    OSGi metrics are recorded against the application&#39;s CompositeMeterRegistry, 
    injected into OsgiFrameworkRunner, rather than Metrics.globalRegistry, which
    Data Prepper does not bind to the registries it exports. The dependency is an
    Optional because this bean is found by package scan and several contexts scan
    the OSGi package without the metrics configuration; when no registry bean
    exists, metrics go to a detached registry rather than to the global one.
    
    The Gradle plugin emits bnd Export-Package directives per clause, since a bnd
    directive applies only to the clause it is attached to; attaching
    -noimport once to a joined package list would leave every package but the last
    importing its own exports. Each declared package is trimmed so a list written
    with spaces after the commas does not produce clauses with leading whitespace.
    bnd is pinned to 6.4.0: 7.0.0 is compiled to Java 17 bytecode, and because the
    bnd plugin is declared in settings.gradle every build evaluates it, so a Java
    11 Gradle JVM cannot load BndBuilderPlugin at all and Java 11 jobs such as
    verify-api-compatibility fail before any task runs.
    
    BundleResolutionErrorTranslator turns Felix resolution failures into readable
    diagnostics. BundleHealthCheck verifies framework, bundle, and classloader
    isolation. BundleClassLoaderScope manages the thread context classloader at the
    plugin invocation boundary so ServiceLoader and SPI lookups resolve against the
    bundle classloader. PluginHealthProbe is a seam for future functional health
    checks.
    
    The OsgiFrameworkRunner bean is always instantiated by Spring but returns early
    in @PostConstruct unless data-prepper.plugin.framework=osgi. The Felix
    framework JAR ships on the runtime classpath as a small (&lt;1MB) library with
    zero transitive dependencies and is not exercised in legacy mode.
    
    Scope is limited to classloader isolation and deploy-time validation. Bundles
    install once at startup and stop at shutdown, the same lifecycle as classpath
    plugins. There is no production hot-reload; PluginHotLoader is test-scoped
    only.
    
    Data Prepper-owned system properties use the data-prepper. namespace: 
    data-prepper.plugin.framework and data-prepper.plugin.bundles.dir. The genuine
    OSGi and Felix properties (org.osgi.*, felix.*) are unchanged.
    
    Testing: an integration test OsgiIT in a new integrationTest source set under
    plugin-framework-osgi builds a DefaultPluginFactory like 
    DefaultPluginFactoryIT, loads a test plugin through it, and proves the load
    happened through OSGi rather than the classpath. It asserts that 
    FrameworkUtil.getBundle is non-null, the plugin classloader is a 
    BundleReference, the classloader is not the application classloader, and the
    application classloader throws ClassNotFoundException for the plugin class
    because the plugin is supplied only as a bundle JAR and never on the
    application classpath. Unit tests drive OsgiPluginRegistry entirely from mocked
    service references, covering resolution of every published name, the
    plain-string property shape, and services that are incomplete or whose classes
    cannot be loaded. StaticBundleLoader is tested with a real fragment attached to
    a real host to assert the fragment is absent from the started list and does not
    increment the bundlesLoaded counter. Each behavior above is covered by a test
    that fails if that behavior is reverted: metrics are asserted to land in the
    injected registry and to be absent from Metrics.globalRegistry;
    OsgiPluginRegistry is asserted never to call getService();
    LegacyPluginBundleActivator is asserted to publish all of a plugin&#39;s names as
    one multi-valued String[] property; BundleHealthCheck is asserted to report a
    bundle as not isolated both when its wiring is null and when reading its
    classloader throws; PluginProviderLoader is asserted to return registered
    providers ahead of classpath providers; DefaultPluginFactory is asserted to
    resolve a provider registered after its construction and to name both
    empty-provider cases in its error message; and toPackageGlobs is asserted to
    trim a package list written with spaces after the commas. The Gradle plugin has
    unit and functional
    (GradleRunner) tests, including a two-package project that proves the 
    per-clause Export-Package fix: with the directive on only the last clause, bnd
    emits a self-import for the first package, and the test asserts that import is
    absent.
    
    Wiring changes: settings.gradle adds the plugin-framework-osgi module, the new
    data-prepper-gradle-plugins/osgi-plugin (consumed via includeBuild in
    pluginManagement), the data-prepper-test/osgi-test-plugin subproject, a Felix
    version-catalog entry, and the bnd plugin. build-resources.gradle adds
    plugin-framework-osgi to coreProjects. data-prepper-core declares a runtimeOnly
    dependency on the new module. PluginProviderLoader is made public with a
    registerProvider() hook and caches the framework-mode flag at construction.
    DefaultPluginFactory re-reads the providers on every lookup rather than
    snapshotting them in its constructor, which is what makes the OSGi provider
    visible: the OSGi framework registers it from a @PostConstruct and there is no
    Spring ordering edge forcing that to run before the first plugin lookup. Both
    ends document that a snapshot must not be reintroduced without adding such an 
    edge, and the empty-provider error message now distinguishes a missing 
    PluginProvider service file from an OSGi framework that has not finished 
    starting. Tests verify these changes, and legacy behavior is unchanged.
    
    In OSGi mode getPluginProviders() returns the OSGi-registered providers ahead
    of the classpath providers and DefaultPluginFactory takes the first match, so a
    bundle shadows a classpath plugin of the same name. This is intended and is now
    documented in the module README.
    
    The example and test plugin is data-prepper-test/osgi-test-plugin, not a 
    released data-prepper-plugins module. It demonstrates a minimal
    @DataPrepperPlugin built as an OSGi bundle through the new Gradle plugin.
    
    Resolves #6760
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    * fix(osgi): address review follow-ups and minimize new dependency surface
    
    Follow-up to the OSGi plugin framework commit, covering the remaining reviewer
    feedback and the High-severity findings the PR Code Analyzer reported. Kept as
    a separate commit so the reviewed history is not rewritten.
    
    Reviewer follow-ups:
    
    SPI resolution now covers plugin construction. PluginCreator switches the 
    thread context classloader to pluginClass.getClassLoader() around 
    constructor.newInstance and restores it in a finally block, and only when it
    differs from the current TCCL. That makes it a strict no-op for the classpath
    framework, where plugin classes come from the application classloader, while
    under OSGi it is the plugin&#39;s own bundle classloader. It lives in
    data-prepper-plugin-framework, needs no OSGi types, and covers extensions too
    because ExtensionLoader shares the PluginCreator bean. Runtime invocation is
    deliberately not wrapped: that would put a TCCL swap on the per-batch hot path.
    The README now lists exactly which three boundaries are covered and states
    plainly what is not.
    
    Provider registration order is now guaranteed rather than merely early. 
    OsgiFrameworkRunner implements the new PluginProviderRegistrar interface and
    DefaultPluginFactory takes it as Optional, so Spring must finish the runner&#39;s
    @PostConstruct — which starts Felix and registers the OSGi provider — before
    the factory can be constructed, and no plugin can be looked up before the
    factory exists. Optional because the only implementation lives in a module core
    depends on at runtime only; in legacy mode the argument is simply empty. The
    per-lookup provider re-read stays as defense in depth rather than as the
    ordering guarantee.
    
    The bundles-directory scan no longer rejects build by-products. 
    StaticBundleLoader skips -sources.jar and -javadoc.jar, which Gradle and Maven
    publish next to a real artifact and which are never bundles. Every other JAR
    without an OSGi manifest is still rejected, so a plugin that forgot to apply
    the Gradle plugin fails loudly at startup instead of going missing at pipeline
    start.
    
    Fixed a stale-bundle defect that was masking test results. stageOsgiBundles was
    a Gradle Copy, and Copy never deletes, so a bundle JAR built under an earlier
    project version stayed in the staging directory beside the current one. OsgiIT
    points Felix at that whole directory, OSGi installs both versions of the same
    Bundle-SymbolicName, both register the same plugin name, and which one wins a
    lookup is undefined. The task is now a Sync, and OsgiIT asserts that exactly
    one bundle with the test plugin&#39;s symbolic name is installed so the same class
    of problem cannot return silently.
    
    Dependency surface:
    
    Removed the third-party bnd build plugin declaration from settings.gradle. bnd
    is now reached only through the included osgi-plugin build&#39;s own runtime
    classpath, so this repository declares no third-party build plugin; only the
    first-party includeBuild remains.
    
    Dropped the bnd plugin and its generated manifest from plugin-framework-osgi 
    itself, along with the validateBundle check that asserted the header. That 
    manifest was dead weight: data-prepper-core declares runtimeOnly
    project(&#39;:plugin-framework-osgi&#39;), so this module is the OSGi host on the
    application classpath and is never installed as a bundle. Bundles reach
    org.opensearch.dataprepper.plugin.osgi through FRAMEWORK_SYSTEMPACKAGES_EXTRA,
    not through a bundle manifest.
    
    Removed three dependencies after verifying each was unnecessary. 
    org.osgi:osgi.core is redundant because the OSGi core API, including the 
    FrameworkUtil and BundleReference the integration tests use, is contained in 
    the Felix framework JAR. logback-classic and micrometer-test were unused. 
    Duplicated javax.inject and javax.annotation-api declarations in the 
    integrationTest block were inherited already.
    
    That leaves exactly two new third-party coordinates, both pinned exactly rather
    than by range: Apache Felix (org.apache.felix.framework 7.0.5, Apache-2.0),
    which is the framework itself and is inert unless
    -Ddata-prepper.plugin.framework=osgi is set, and bnd
    (biz.aQute.bnd.gradle 6.4.0, Apache-2.0 OR EPL-2.0), which is build-time only
    and confined to the included build. bnd stays on the latest 6.x because 7.x is
    compiled to Java 17 bytecode and this repository builds on a Java 11 Gradle
    JVM. Every other dependency of the new module is already used at the same
    version elsewhere in Data Prepper. Provenance and license for both are recorded
    in plugin-framework-osgi/README.md.
    
    Hardened the stale bundle-cache purge in FelixPluginManager. It matched any 
    directory whose name merely began with the cache name, so an unrelated 
    directory such as felix-cache-backup was eligible for deletion, and it used a
    symlink-following directory check, so a link planted under the OSGi directory
    could redirect the walk outside it. A candidate is now removed only when it is
    a direct child of the OSGi directory, its name matches felix-cache-&lt;digits&gt;
    exactly, and it is a real directory rather than a symbolic link.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    * test(mongodb): fix pre-existing flaky ExportSchedulerTest await anchors
    
    Three tests in ExportSchedulerTest awaited an intermediate coordinator 
    interaction and then called future.cancel(true), which can interrupt 
    ExportScheduler.run() before it makes the remaining calls the assertions 
    expect. This surfaced on build (21) as:
    
      TooFewActualInvocations: coordinator.getPartition(...)
     Wanted 3 times:  ExportSchedulerTest.java:173
     But was 2 times: ExportScheduler.run(ExportScheduler.java:79)
                      ExportScheduler.run(ExportScheduler.java:83)
    
    Line 172, the getPartition inside markTotalPartitionsAsComplete, never ran
    because cancel landed after createPartition but before it.
    
    Anchor each await on the last coordinator interaction of the scheduler&#39;s 
    iteration, saveProgressStateForPartition(globalState, ...), which every other
    assertion in these tests happens at or before.
    
    The race predates this branch: the times(3) getPartition assertion was added in
    #4553, and data-prepper-plugins/mongodb is otherwise untouched by this PR.
    Tracked under #3481.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    * test: fix flaky ProcessorValidationIT @SingleThread assertion
    
    ProcessorValidationIT.test_events_processed_when_SingleThread_processor 
    intermittently failed in CI with &#34;Expected: &lt;1&gt; but: was &lt;0&gt;&#34; at 
    verifySingleThreadUsage. The multi-processor-pipeline/SingleBatch case submits
    a single batch to a four-worker pipeline, so a worker can legitimately finish
    without ever reading a batch, leaving its
    @SingleThread processor instance unused. Asserting that every instance was used
    by exactly one thread therefore asserted something the pipeline does not
    guarantee.
    
    Assert the actual @SingleThread contract instead: no instance is used by more
    than one thread, no thread is shared across instances, and at least one
    instance was used. This still catches an instance being shared between workers,
    which the previous assertion could not distinguish from an idle worker.
    
    Also make SingleThreadEventsTrackingTestProcessor.PROCESSORS a 
    CopyOnWriteArrayList. Instances are constructed on the process worker threads
    and the list is read from the test thread, so a plain ArrayList was unsafely
    published, unlike its sibling ConcurrentHashMap.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    * fix(osgi): export third-party packages with versions and name unsatisfied
    imports
    
    Three defects made the OSGi path unusable for any real plugin. They only affect
    the opt-in `-Ddata-prepper.plugin.framework=osgi` path; the default plugin
    framework is untouched.
    
    1. `third-party.packages` was generated without a `version` attribute. An OSGi
      export with no version defaults to `0.0.0`, which satisfies no version
      range, and bnd computes ranges for every third-party import it finds in a
      plugin JAR. `mutate-string-processors`, for example, imports
      `com.fasterxml.jackson.annotation;version=&#34;[2.21,3)&#34;`, so it could never
      resolve. Each package is now exported with the version resolved for this
      build rather than a hardcoded one, so the two cannot drift apart.
    
    2. `org.slf4j.spi` was never exported. SLF4J 2.x bytecode references the
      provider SPI, so bnd adds `org.slf4j.spi` to the imports of virtually any
      plugin that logs, and exporting `org.slf4j` alone is not enough.
    
    3. `StaticBundleLoader` reported resolution failures before starting any
      bundle, so the message could only ever be &#34;Bundle state is INSTALLED (not
      RESOLVED)&#34; and never named the unsatisfied requirement.
      `FrameworkWiring.resolveBundles` returns only a boolean, so the detail is
      now recovered from the `BundleException` that `Bundle.start()` throws and
      passed to the existing `BundleResolutionErrorTranslator`. Fragments are
      skipped because they cannot be started. A bundle importing a package the
      host does not export now reports:
    
         Plugin &#39;...&#39; could not be resolved: it requires package
        com.fasterxml.jackson.databind;version=[2.21.0,3.0.0) but the host/other
        bundles do not provide a compatible version.
    
    Verified by bundling a real plugin (`mutate-string-processors`) with its JAR 
    held out of `lib/`: it resolves, goes ACTIVE and processes an event, which was 
    impossible before this change. `DataPrepperOsgiPackages.SLF4J_PACKAGE` still 
    pins `org.slf4j` to a hardcoded version; that is left for a follow-up since the
    pinned version satisfies the range plugins ask for.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt; Co-authored-by:
    Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;

* __feat(os-s3-dlq): add codec option for JSON or NDJSON output (#7108)__

    [Teja Reddy](mailto:tejareddy.bvsk@wbd.com) - Thu, 27 Aug 2026 07:04:38 -0700
    
    
    feat(os-s3-dlq): add codec option for JSON or NDJSON output
    
    Signed-off-by: Teja Reddy &lt;tejareddy.bvsk@wbd.com&gt;

* __Add JDBC-based source coordination store plugin (#6757)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Tue, 25 Aug 2026 12:06:17 -0700
    
    
    Add JDBC-based source coordination store plugin
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __feat(prometheus-sink): add instance_label (#7084)__

    [Teja Reddy](mailto:bvsktejareddy@gmail.com) - Tue, 25 Aug 2026 10:48:09 -0500
    
    
    Time series labels are drawn only from metric attributes, so every Data Prepper
    instance running the same pipeline emits series with identical labels.
    Prometheus, AMP, Cortex, and Mimir deduplicate on series plus timestamp, so
    they keep one instance&#39;s sample and drop the rest without reporting an error.
    An N-instance deployment reports roughly 1/N of the true value and nothing
    surfaces the loss.
    
    instance_label names a label whose value the sink resolves at startup from this
    instance&#39;s IPv4 address, so each instance writes a distinct series and
    consumers recover the true value by aggregating the label away. The value is
    resolved rather than configured, so one configuration shared by every instance
    is enough. Managed deployments need that: OpenSearch Ingestion runs every OCU
    of a pipeline from a single definition, leaving nowhere to write a per-instance
    value.
    
    The address is not taken from the hostname. Hostname resolution depends on the
    hostname being present in the name service, and HostContext falls back to the
    shared constant &#34;unknown&#34; when it is not, which would give every instance the
    same value and reintroduce the collision.
    
    The sink instead asks the operating system which address it would use to reach
    the configured url, by connecting a datagram socket to the endpoint and reading
    the socket&#39;s local address. Connecting a datagram socket performs a routing
    lookup and sends nothing, so this opens no connection and costs one syscall at
    startup. On an instance holding more than one address this yields the address
    the endpoint observes rather than an arbitrary choice among them. Where the
    lookup cannot answer, such as an endpoint which does not resolve at startup,
    the sink falls back to the IPv4 addresses on the local interfaces, sorted so
    the choice is stable across restarts. NetworkInterface is already how peer
    forwarding decides whether an address belongs to this instance, and rds-source
    already derives a per-instance server id from the local address, so the address 
    is an established identity for an instance. An instance which cannot resolve an
    address at all fails at startup instead.
    
    The label name must be a valid Prometheus label name, must not begin with &#34;__&#34;,
    and must not be one of the names the sink adds to individual series (&#34;le&#34;,
    &#34;ge&#34;, &#34;quantile&#34;), which remote write would reject as a repeated label name.
    
    Signed-off-by: Teja Reddy &lt;tejareddy.bvsk@wbd.com&gt;

* __Add support for rate limiting at a group level (#7103)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Mon, 24 Aug 2026 12:40:05 -0700
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __feat(cloudwatch-logs): support dynamic per-entity grouping in sink (#7034)__

    [Kirtan Kakadiya](mailto:35823164+KirtanKakadiya@users.noreply.github.com) - Mon, 24 Aug 2026 08:24:15 -0700
    
    
    PutLogEvents accepts exactly one entity per request. When any entity 
    key_attributes or attributes value contains a ${...} reference, the sink now
    partitions buffered events by their resolved key attributes and sends one
    request per distinct entity, so multi-source streams get correct per-resource
    entity attribution. Concurrently buffered entities are bounded by
    entity.max_cardinality; idle groups are released, and overflow falls back to a
    shared group with no entity. Static-entity mode is preserved and unchanged when
    no value is templated.
    
    Signed-off-by: Kirtan Kakadiya &lt;kirtanhk@amazon.com&gt; Co-authored-by: Kirtan
    Kakadiya &lt;kirtanhk@amazon.com&gt;

* __Migrate Jira search from removed /rest/api/3/search to /rest/api/3/se… (#7106)__

    [Harsha Siva Sai Sannapaneni](mailto:harshasivasai85@gmail.com) - Fri, 21 Aug 2026 16:57:55 -0700
    
    
    Migrate Jira search from removed /rest/api/3/search to /rest/api/3/search/jql
    
    Closes #7104
    
    Signed-off-by: Harsha Siva Sai Sannapaneni &lt;sivasais@amazon.com&gt;

* __Add Kafka source observability metrics (#7102)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Thu, 20 Aug 2026 17:06:33 -0500
    
    
    Add metrics to the Kafka source, published through the existing Micrometer
    registry, so operators can diagnose scaling and reliability issues that were
    previously visible only in logs, or not at all:
    
    - Scaling: per-partition lag, assigned partitions, active readers, and
     configured workers, so a bottleneck can be seen as partition-bound vs
     worker-bound.
    - Rebalance activity: rebalance and partitions-revoked counts.
    - Processing latency: poll to buffer handoff.
    - Federated auth (azure_federated): token-refresh success and failure
     counts (failures tagged by a bounded errorType), and headroom in
     seconds to the next proactive token refresh.
    
    The failure-handling counters from the issue (commit failures, buffer write
    failures, offset-reset failures) are intentionally left out here; they are
    covered by #7088.
    
    Because the azure_federated callback handler is created reflectively by 
    kafka-clients, its metrics object is handed off through a singleton
    (KafkaSourceAuthMetricsProvider), mirroring AwsCredentialsSupplierProvider, and
    is registered only for azure_federated sources.
    
    Also make the azure_federated auth failure logging generic and actionable: log
    the underlying exception with its stack trace instead of asserting a single
    presumed cause, and drop the SENSITIVE marker from messages whose arguments are
    not credentials.
    
    Resolves #7074
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Bump 1password/load-secrets-action to v5.0.1 (data-prepper) (#7107)__

    [Peter Zhu](mailto:zhujiaxi@amazon.com) - Thu, 20 Aug 2026 15:23:10 -0400
    
    
    Signed-off-by: Peter Zhu &lt;zhujiaxi@amazon.com&gt;

* __Fix CloudWatch sink DLQ to use default AWS credentials (#7076)__

    [Reshmi Vijayan](mailto:reshmi.vijayan@ibm.com) - Tue, 18 Aug 2026 15:45:58 -0700
    
    
    Fix CloudWatch sink DLQ to use default AWS credentials
    
    Signed-off-by: revijay &lt;reshmi.vijayan@ibm.com&gt;

* __Fix JSON Pointer escape handling in JacksonEvent (#7079)__

    [Reshmi Vijayan](mailto:reshmi.vijayan@ibm.com) - Tue, 18 Aug 2026 09:26:51 -0700
    
    
    Fix JSON Pointer escape handling in JacksonEvent
    
    Signed-off-by: revijay &lt;reshmi.vijayan@ibm.com&gt;

* __Stop the acknowledgement set monitor thread when stopping the Data Prepper container. (#7100)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 18 Aug 2026 07:01:38 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates AWS SDK to 2.53. (#7097)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 14 Aug 2026 10:16:52 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support custom index names in otel_trace_group processor (#6936) (#6946)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Thu, 13 Aug 2026 12:34:52 -0500
    
    
    * Support custom index names in otel_trace_group processor (#6936)
    
    Adds an optional `indices` list to the otel_trace_group processor config. The
    processor previously hardcoded the search target to the TRACE_ANALYTICS_RAW
    alias (&#34;otel-v1-apm-span&#34;), so users whose raw spans were stored under custom
    index names or patterns could not use the processor. The new field accepts
    concrete index names, aliases, and patterns such as `otel-v1-apm-span-*`.
    
    Backward compatibility is preserved: when `indices` is omitted the processor
    uses the existing TRACE_ANALYTICS_RAW alias, so deployed pipelines.yaml files
    continue to work without changes. The field is marked @NotEmpty so explicit
    misconfiguration fails fast at startup with a clear validation error rather
    than silently broadcasting the search to all indices.
    
    Resolves #6936
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Use full Apache-2.0 license header on otel_trace_group config files
    
    OTelTraceGroupProcessorConfig.java and OTelTraceGroupProcessorConfigTest.java 
    had no license header on main; this PR added the short 3-line form. Use the 
    full 9-line header per CONTRIBUTING.md.
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    ---------
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __fix(rss): harden feed poller error handling and config validation (#7085)__

    [Tony Piazza](mailto:tony.piazza@aiven.io) - Thu, 13 Aug 2026 11:24:31 -0500
    
    
    * fix(rss): harden feed poller error handling, validate backoff/tracker config,
    and cover auth + validation in tests
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    * fix(rss): address review feedback on error handling, logging, and validation
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    ---------
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;

* __Update BouncyCastle to 1.85. (#7087)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 12 Aug 2026 10:39:48 -0700
    
    
    The 1.85.2 patch exists only for some dependencies. To keep these in sync I&#39;m
    updating only to 1.85.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Feature/aggregate sum action (#7080)__

    [harshwadhawe](mailto:harsh.wadhawe@tamu.edu) - Tue, 11 Aug 2026 19:40:27 -0500
    
    
    

* __feat(rss): rewrite rss source to fix serious bugs and add support for multiple feeds (#7036)__

    [Tony Piazza](mailto:tony@piazzaconsulting.com) - Tue, 11 Aug 2026 11:59:56 -0500
    
    
    * feat(rss): rewrite rss source to fix serious bugs and add support for
    multiple feeds
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    * test(rss): replace jsr310 dep with DataPrepperDurationDeserializer
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    * address review: fix dedup edge cases, backoff scheduling, license headers,
    docs
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    * address review: enforce min polling_frequency, stagger feed startup, fix
    license header
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    * Rename pub_date to publication_date, guid to item_id, and validate feed names
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    * address review: name pool threads, unify item_id with dedup key, add
    configurable request_timeout
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    * chore: trigger CI
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;
    
    ---------
    
    Signed-off-by: Tony Piazza &lt;tony.piazza@aiven.io&gt;

* __fix(rds-source): Use null-safe comparison for CDC update event column values (#7082)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 11 Aug 2026 11:26:36 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add auto-detection of pre-compression JSON headers in decompression engines (#7025)__

    [yavmanis](mailto:yavmanis@amazon.com) - Fri, 7 Aug 2026 10:40:35 -0700
    
    
    Add auto-detection of pre-compression headers in decompression engines.
    Supports JSON-only detection for Snappy.
    
    Signed-off-by: Manisha Yadav &lt;yavmanis@amazon.com&gt;

* __Add Kafka source consumer config properties (#7075)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Thu, 6 Aug 2026 05:49:31 -0700
    
    
    Expose metadata_max_age and acknowledgments_expiry_reset on the source 
    consumer, and fix SourceTopicConfig validation constraints.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Add configurable connections_max_idle for topics (#7017)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Wed, 5 Aug 2026 09:37:56 -0700
    
    
    * Add configurable connections_max_idle for topics
    
    Expose Kafka&#39;s connections.max.idle.ms as a per-topic connections_max_idle 
    option on the Kafka source. This lets operators recycle idle connections before
    brokers that close them early.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    * Apply connections_max_idle to the Kafka buffer
    
    Expose getConnectionsMaxIdle() on the shared TopicConsumerConfig interface 
    instead of a dedicated marker interface, so the Kafka buffer consumer picks up
    connections_max_idle in addition to the source. The factory reads it through a
    plain interface call, dropping the instanceof downcast. Also constrain the
    value to a minimum of one second via @DurationMin. Default remains 540000ms, so
    existing pipelines are unchanged.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    ---------
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __fix(rds-source): Use schema-qualified table name for join metadata enrichment in PostgreSQL (#7044)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 4 Aug 2026 00:15:01 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __fix(dynamodb-source): Decrement numOfWorkers before giveUpPartition to prevent scheduler stall (#7070)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 3 Aug 2026 14:41:00 -0500
    
    
    When a DataFileLoader completes with an exception and acknowledgments are
    enabled, the whenComplete callback previously called giveUpPartition before
    decrementing numOfWorkers. If giveUpPartition threw an unchecked exception, the
    decrement was skipped and numOfWorkers stayed at MAX_JOB_COUNT permanently,
    preventing the DataFileScheduler from acquiring any new partitions.
    
    This fix moves the decrement before giveUpPartition and wraps the 
    giveUpPartition call in a try/catch. The counter is always decremented 
    regardless of whether partition release succeeds, ensuring the scheduler can
    continue acquiring work.
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add gRPC keepalive max_connection_age settings on OTel sources (#6745) (#6947)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Mon, 3 Aug 2026 07:59:29 -0700
    
    
    Expose `max_connection_age` and `connection_drain_duration` on every Data
    Prepper source that exposes a gRPC server (otlp, otel-logs, otel-trace,
    otel-metrics). Both settings are optional `Duration` fields on the existing
    per-source configuration. When set, they are applied to the Armeria
    `ServerBuilder` via `maxConnectionAge(Duration)` and
    `connectionDrainDuration(Duration)`. When unset they preserve current behavior
    (no upper bound on connection age).
    
    This addresses the horizontal-scaling problem where long-lived gRPC channels
    never re-resolve DNS to discover new replicas. Configuring
    `max_connection_age` forces clients to reconnect periodically and pick up new
    endpoints.
    
    Implementation details:
    - New `Duration` fields with `@DurationMin` / `@DurationMax` validation
     on `OTLPSourceConfig`, `OTelLogsSourceConfig`, `OTelTraceSourceConfig`,
     `OTelMetricsSourceConfig`.
    - `ServerConfiguration` in `http-common` carries the two new fields so
     `otlp-source` and `otel-metrics-source` pick them up through the
     shared `CreateServer.createGRPCServer(...)` path. `otel-logs-source`
     and `otel-trace-source` apply them directly to their inline
     `ServerBuilder`.
    - `hibernate-validator` is promoted to the version catalog
     (`libs.hibernate.validator`) so all four source modules share a
     single pinned version, replacing the inline pin previously in
     `otlp-source`.
    
    Resolves #6745
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Match OpenSearch shard routing in pull-ingestion IndexRouter (#7013)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Mon, 3 Aug 2026 07:54:32 -0700
    
    
    Match OpenSearch shard routing in pull-ingestion IndexRouter
    
    The pull-ingestion IndexRouter computed the Murmur3 hash over UTF-8 bytes, but
    OpenSearch&#39;s Murmur3HashFunction.hash(String) hashes UTF-16 little-endian 
    bytes, so documents could be routed to a different partition than the shard 
    OpenSearch places them on. The shard calculation also used floorMod(hash,
    numberOfShards) instead of OpenSearch&#39;s floorMod(hash, routingNumShards) /
    routingFactor, diverging whenever number_of_routing_shards differs from
    number_of_shards.
    
    Hash the UTF-16 little-endian bytes, source number_of_routing_shards from the 
    index settings (falling back to number_of_shards), and scale by routingFactor 
    so partition assignment matches OpenSearch&#39;s OperationRouting.generateShardId. 
    Adds a cross-check against OpenSearch&#39;s authoritative Murmur3HashFunction known 
    values and known-answer shard tests.
    
    Resolves #6993
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __feat(flatten): Add configurable flatten_separator parameter (#7042)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 29 Jul 2026 14:59:15 -0500
    
    
    Add flatten_separator parameter to the flatten processor to allow configuring
    the character used to join nested key names. Previously hardcoded to &#34;.&#34;, now
    supports any single character (e.g., &#34;_&#34;, &#34;-&#34;).
    
    Default remains &#34;.&#34; for backward compatibility.
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix azure_federated Kafka auth after token refresh (#7040)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Wed, 29 Jul 2026 09:40:07 -0700
    
    
    Fix azure_federated Kafka auth after token refresh
    
    The Kafka source stopped consuming about one hour after start and did not
    recover.
    
    getToken() cached one token object and returned that same instance. Kafka
    identifies OAUTHBEARER tokens on the SASL Subject by object identity: a
    re-login adds the new token and removes the old one. With the same instance,
    the add does nothing and the remove empties the Subject, so the consumer can no
    longer authenticate.
    
    Return a new token instance from each getToken() call, but keep the cached
    value and absolute expiry so the token exchange does not repeat and the refresh
    schedule does not change.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Extend length() expression function to support Map, List, and Array types (#7031)__

    [bhavsshr](mailto:bhavsshr@amazon.com) - Wed, 29 Jul 2026 08:45:33 -0700
    
    
    Previously, length() only supported String values (returning character count). 
    This enhancement adds support for:
    - Map: returns the number of keys (e.g., length(/metadata) returns 2 for
    {&#34;a&#34;:1,&#34;b&#34;:2})
    - List: returns the number of elements (e.g., length(/tags) returns 3 for
    [&#34;x&#34;,&#34;y&#34;,&#34;z&#34;])
    - Array: returns the number of elements (raw Java arrays)
    
    This enables users to write conditions based on collection sizes in
    expressions, e.g., &#39;length(/body/details) &gt; 0&#39; to check if a map/list is
    non-empty, or
    &#39;length(/records) &gt; 100&#39; to route based on batch size.
    
    The change is fully backward-compatible - existing String behavior is
    unchanged.
    
    Testing:
    - Added tests using @Nested classes for grouping (String, Map, List, Array,
    Error cases)
    - Used mocked Event for Map, List, Array, and Error tests to guarantee branch
    coverage
    - All code paths covered for 100% JaCoCo instruction coverage
    - getFunctionName(), all type branches, and all error paths tested
    
    Signed-off-by: bhavsshr &lt;bhavsshr@amazon.com&gt;

* __Add integration test verifying processors work with empty, null, and bare key configurations (#7012)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Tue, 28 Jul 2026 13:52:37 -0700
    
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Reduce global permissions (#7035)__

    [Peter Zhu](mailto:zhujiaxi@amazon.com) - Mon, 27 Jul 2026 17:57:14 -0400
    
    
    Signed-off-by: Peter Zhu &lt;zhujiaxi@amazon.com&gt;

* __Add code diff analyzer/reviewer to dataprepper repository (#7033)__

    [Peter Zhu](mailto:zhujiaxi@amazon.com) - Mon, 27 Jul 2026 17:37:00 -0400
    
    
    Signed-off-by: Peter Zhu &lt;zhujiaxi@amazon.com&gt;

* __Detect zstd compressed objects in automatic compression mode (#6909)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Mon, 27 Jul 2026 12:42:06 -0700
    
    
    CompressionOption.fromFileName() backs the &#34;automatic&#34; compression option for
    the S3 source (both SQS-notification and scan paths) and the S3 enrich 
    processor. It only recognized &#34;.gz&#34; (GZIP) and &#34;.snappy&#34; (SNAPPY) and silently
    fell back to NONE for everything else, so zstd-compressed objects were passed
    to the codec without decompression and failed to parse — even though ZSTD is a
    valid CompressionOption with a working ZstdDecompressionEngine already wired
    in.
    
    Map the &#34;.zst&#34; (canonical, per the zstd CLI and RFC 8478) and &#34;.zstd&#34;
    (informal alternate spelling) extensions to CompressionOption.ZSTD so automatic
    detection decompresses these objects correctly. Behavior is unchanged unless a
    user explicitly configures compression: automatic.
    
    Add unit tests covering both extensions.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Add configurable shard_discovery_interval to DynamoDB stream source (#7001)__

    [Archit Kaushik](mailto:asarchit65@gmail.com) - Mon, 27 Jul 2026 11:26:21 -0700
    
    
    * Add configurable shard_discovery_interval to DynamoDB stream source
    
    The DynamoDB stream source&#39;s LeaderScheduler ran shard discovery on a hardcoded
    1-minute interval. Because DynamoDB Streams shards roll over continuously,
    newly opened child shards were not read until the next discovery pass, putting
    a ~1-minute floor on shard-rotation latency and surfacing as high
    EndToEndLatency while PipelineLatency stayed low.
    
    Add an optional shard_discovery_interval on the stream config, defaulting to 1
    minute so behavior is unchanged unless set. A minimum of 1 second is enforced
    to avoid excessive DescribeStream/ListShards calls.
    
    Resolves #7000
    
    Signed-off-by: Archit Kaushik &lt;kausarch@amazon.com&gt;
    
    * refactor: use @DurationMin annotation and drop pipeline-parser test dep
    
    Address review feedback:
    - Replace custom @AssertTrue validation with @DurationMin(seconds = 1)
     from hibernate-validator, matching the pattern used by aws-lambda,
     cloudwatch-logs, and geoip-processor plugins.
    - Remove testImplementation on :data-prepper-pipeline-parser; use
     jackson-datatype-jsr310 JavaTimeModule for Duration deserialization
     in tests (ISO-8601 format), matching the aws-lambda test convention.
    - Validate via jakarta Validator in tests instead of calling a method.
    
    Signed-off-by: Archit Kaushik &lt;architkshk@gmail.com&gt; Signed-off-by: Archit
    Kaushik &lt;kausarch@amazon.com&gt;
    
    * refactor: simplify LeaderScheduler construction and fix license header
    
    Address dlvenable&#39;s review feedback:
    - Extract lease interval into a local variable and always use the 5-arg
     LeaderScheduler constructor, removing the two-constructor branch.
    - Make DEFAULT_SHARD_DISCOVERY_INTERVAL public so DynamoDBService can
     reference it for the null-streamConfig fallback.
    - Add the full license header to StreamConfigTest.java per CONTRIBUTING.md.
    
    Signed-off-by: Archit Kaushik &lt;architkshk@gmail.com&gt; Signed-off-by: Archit
    Kaushik &lt;kausarch@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Archit Kaushik &lt;kausarch@amazon.com&gt; Signed-off-by: Archit
    Kaushik &lt;architkshk@gmail.com&gt; Co-authored-by: Archit Kaushik
    &lt;kausarch@amazon.com&gt;

* __Add azure_federated authentication to Kafka source (#7011)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Thu, 23 Jul 2026 12:24:11 -0700
    
    
    * Add azure_federated authentication to Kafka source
    
    Enable the Kafka source to consume from Azure Event Hubs by federating an AWS
    identity to Azure Active Directory for OAuth, so no Azure client secret is
    stored in the pipeline. Credentials use Data Prepper&#39;s standard AWS
    configuration, so one pipeline works across AWS credential setups.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    * Close STS client and share JAAS constants in azure_federated Kafka auth
    
    - AzureFederatedTokenProvider now exposes close() (lock-guarded) and
     AzureFederatedCallbackHandler.close() delegates to it, releasing the
     lazily-created StsClient on shutdown instead of leaking it.
    - Promote the OPT_* JAAS option keys on AzureFederatedCallbackHandler to
     public and reference them from KafkaSecurityConfigurer so the writer and
     reader share one definition and can&#39;t drift.
    - Replace the repeated &#34;SASL_SSL&#34; literals in KafkaSecurityConfigurer with
     a SASL_SSL_PROTOCOL constant.
    - Add unit tests for close() delegation and StsClient cleanup.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    * Simplify azure_federated Kafka auth configuration
    
    Drop the redundant azure_ prefix from config keys since they already sit under
    azure_federated. Remove tenant_id, which duplicated the tenant already present
    in token_endpoint. Resolve the AWS region from the data-prepper-config.yaml
    default when the pipeline config omits it, rather than requiring aws.region.
    Remove the orphaned integration-test gradle properties whose test is not in the
    repo.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;
    
    ---------
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __fix(s3-source): Log full stack trace in S3ObjectWorker buffer write error (#7015)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 21 Jul 2026 14:43:34 -0500
    
    
    The existing error log only prints e.getMessage() which is null for exceptions
    like NullPointerException, making the error impossible to diagnose. Log the
    full exception with stack trace instead.
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix flaky file-source unit tests with an injectable Clock and controllable executor (#6984) (#6995)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Tue, 21 Jul 2026 09:16:04 -0700
    
    
    FileReader.readLoop and its backpressure/batch logic branched on 
    System.currentTimeMillis(), so the max-read-time and drain-timeout tests 
    depended on wall-clock timing. On a slow or loaded runner the loop could break
    before the first channel.read, leaving Mockito stubs unused and failing with
    UnnecessaryStubbingException.
    
    Inject a java.time.Clock into FileReader (public constructor defaults to 
    Clock.systemDefaultZone(), package-private constructor accepts a Clock), 
    matching the existing pattern in ServiceMapStatefulProcessor, 
    OTelApmServiceMapProcessor, and PipelineShutdown. Tests now drive time with a
    mocked Clock: the read stub advances the fake clock past the deadline after
    delivering data once, so exactly one read happens deterministically. Removes
    all Thread.sleep and wall-clock assumptions from these tests.
    
    FileReaderPoolTest bookkeeping tests submitted readers to a real thread pool
    where the reader completed on a background thread and mutated the 
    active/pending counts before the assertions ran. Add 
    createPoolWithNonRunningExecutor so queueing, promotion, and close logic run
    synchronously. Tests that verify execution keep the real executor and await via
    verify(..., timeout(...)).
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __feat(split-event): Add split_when conditional parameter (#7014)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Mon, 20 Jul 2026 16:59:24 -0500
    
    
    Add split_when parameter to the split_event processor, consistent with the
    conditional expression pattern used by other processors (e.g., delete_when,
    add_when, parse_when). When split_when evaluates to false, the event passes
    through unchanged.
    
    Closes #6992
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Log data stream config notices once per stream instead of per document (#6979)__

    [nsa-softpay](mailto:nsa@softpay.io) - Mon, 20 Jul 2026 09:16:07 -0700
    
    
    The opensearch sink logs configuration notices for data streams
    (first-write-wins document_id, unsupported routing, and action coercion to
    `create`) inside determineAction, which runs once per event. On high-volume
    pipelines this emits one WARN per ingested document, dominating log output.
    
    These notices describe configured, expected behavior rather than a per-record
    fault, so dedupe them by (data stream, notice type) and log each at most once
    per data stream. The returned action is unchanged.
    
    Resolves #6972
    
    Signed-off-by: nsa-softpay &lt;nsa@softpay.io&gt;

* __Return HTTP 400 with informative message for client-side protobuf validation errors (#6989)__

    [Indraja Babu](mailto:156961807+indrajababu@users.noreply.github.com) - Sat, 18 Jul 2026 10:41:43 -0700
    
    
    When a client sends a protobuf request containing invalid UTF-8 in string
    fields
    (e.g., InstrumentationScope name/version), the pipeline currently returns HTTP
    500 with a generic error. This change returns HTTP 400 with a client-actionable
    message explaining the issue.
    
    Changes:
    - Override apply() in GrpcRequestExceptionHandler to intercept
     InvalidProtocolBufferException before Armeria&#39;s default short-circuit
    - Detect via stable cause-chain type check (not brittle message string)
    - Apply same fix to HTTP path handlers in otel-logs-source and
    otel-trace-source
    - Add wire-level integration test proving HTTP 400 for invalid UTF-8 protobuf
    - Add test for unsupported grpc-encoding header rejection
    
    Signed-off-by: Indraja Babu &lt;indbabu@amazon.com&gt;

* __fix(sqs): Change acknowledgments default to true to prevent data loss (#6999)__

    [Indraja Babu](mailto:156961807+indrajababu@users.noreply.github.com) - Wed, 15 Jul 2026 09:34:27 -0500
    
    
    Change the SQS source plugin default for acknowledgments from false to true.
    With the previous default, messages were deleted from SQS after being written
    to the internal buffer but before downstream delivery was confirmed. If the
    pipeline failed after buffering (sink unavailable, worker crash, processor
    error), messages were permanently lost with no recovery path.
    
    With acknowledgments enabled by default, messages are only deleted from SQS
    after the entire pipeline confirms successful end-to-end delivery.
    
    Changes:
    - Change default from false to true in SqsSourceConfig
    - Add WARNING log when acknowledgments is explicitly disabled
    - Add javadoc explaining data loss risk and visibility timeout
     implications
    - Add sqsMessagesDeletedWithoutAcknowledgment metric counter
    - Add integration tests verifying behavior with both modes under
     processing failure
    
    This is a BREAKING CHANGE. Users who explicitly require the previous 
    best-effort behavior should set acknowledgments: false in their pipeline
    configuration. Users should ensure their SQS visibility timeout exceeds the
    expected end-to-end processing time to avoid duplicate delivery.
    
    Fixes #6998
    
    Signed-off-by: Indraja Babu &lt;indbabu@amazon.com&gt;

* __Add metadata for document routing to OpenSearch source (#6994)__

    [fifthist](mailto:Overseer.O5.X@gmail.com) - Tue, 14 Jul 2026 07:34:07 -0700
    
    
    Exposes the document routing value from search hits via the 
    opensearch-document_routing event metadata attribute, mirroring the existing
    document id, index, and version metadata. Routing is only added when present so
    documents without custom routing are unaffected.
    
    Signed-off-by: Sergey Kojoian &lt;skojoian.am@gmail.com&gt; Co-authored-by: Claude
    Opus 4.8 &lt;noreply@anthropic.com&gt;

* __Accept empty string as null in PluginModel deserializer and add Coerc… (#6915)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Mon, 13 Jul 2026 15:49:27 -0700
    
    
    Accept empty string as null in PluginModel deserializer and add CoercionConfig
    for settings conversion
    
    Remove global CoercionConfig, keep targeted empty-string handling
    
    Remove the SERIALIZER_OBJECT_MAPPER CoercionConfig that globally coerced empty
    strings to null across all fields. The targeted VALUE_STRING check in the
    deserializer already handles bare YAML keys (e.g. stdout:) by treating empty
    string as null only at the plugin name level.
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Fix thread safety in dissect processor by eliminating mutable shared state (#6858)__

    [Shikhar Dhawale](mailto:51256166+shik-25@users.noreply.github.com) - Wed, 8 Jul 2026 09:39:52 -0700
    
    
    * Fix thread safety in dissect processor by eliminating mutable shared state
    
    Signed-off-by: Shikhar Dhawale &lt;shikhar_dhawale@apple.com&gt;
    
    * Address review comments: make Field.key final, package-protect set Next,
    pre-size HashMaps, pre-sort AppendField lists, add ERROR logs, expand test
    coverage
    
    Signed-off-by: Shikhar Dhawale &lt;shikhar_dhawale@apple.com&gt;
    
    * Restore inline comments in DissectProcessorTest
    
    Signed-off-by: Shikhar Dhawale &lt;shikhar_dhawale@apple.com&gt;
    
    * Remove zip64 benchmarking artifact from build.gradle
    
    Signed-off-by: Shikhar Dhawale &lt;shikhar_dhawale@apple.com&gt;
    
    * Add debug logging when indirect field references are dropped in dissect
    processor
    
    Signed-off-by: Shikhar Dhawale &lt;shikhar_dhawale@apple.com&gt;
    
    * Address review comments: make template maps final, fix HashMap initial
    capacity in dissectText
    
    Signed-off-by: Shikhar Dhawale &lt;shikhar_dhawale@apple.com&gt;
    
    ---------
    
    Signed-off-by: Shikhar Dhawale &lt;shikhar_dhawale@apple.com&gt; Co-authored-by:
    Shikhar Dhawale &lt;shikhar_dhawale@apple.com&gt;

* __Add debug logging for every document id success (#6987)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 7 Jul 2026 18:05:51 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add /ready endpoint to DataPrepperServer that returns 200 only after all pipeline sources have started. This prevents ALB from routing traffic to targets before the data ingestion port is listening, eliminating 502 errors during scale-up events. (#6977)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Tue, 7 Jul 2026 11:25:19 -0500
    
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Update the release process documentation for latest changes. (#6986)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Jul 2026 12:28:35 -0700
    
    
    Update the release process documentation for latest changes.
    
    We have made a number of significant changes to the release process. Some of
    these come from changes in the OpenSearch project and some are from additional
    automation we have added. This includes updates for how we coordinate with
    other parts of the OpenSearch project, steps that require the OpenSearch
    project to create branches and tags, and updates based on automation. I&#39;ve also
    added additional information about releasing blog posts and updating the
    community.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add Data Prepper 2.16.0 changelog. (#6973)__

    [opensearch-ci](mailto:83309141+opensearch-ci-bot@users.noreply.github.com) - Mon, 6 Jul 2026 07:43:15 -0700
    
    
    Signed-off-by: github-actions[bot]
    &lt;41898282+github-actions[bot]@users.noreply.github.com&gt; Co-authored-by:
    dlvenable &lt;293424+dlvenable@users.noreply.github.com&gt;

* __Publish a pre-release GitHub Release and use that for the Jenkins job. (#6974)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 2 Jul 2026 11:55:59 -0700
    
    
    Publish a pre-release GitHub Release and use that for the Jenkins job.
    
    Per changes in the OpenSearch project, we can no longer modify repos using
    bots. So the new process is to publish a pre-release Release. Then the Jenkins
    job will use this pre-release. It&#39;s final step is to create a GitHub issue
    instead of publishing the Release.
    
    Use ncipollo/release-action instead of softprops/action-gh-release in the
    release action to keep consistency with other OpenSearch projects.
    
    Provide failure notifications for failing Jenkins jobs.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the Claude command to generate release notes with instructions to look at the release blog. (#6952)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 1 Jul 2026 10:57:08 -0700
    
    
    Using the release blog is a good way to be sure that the release notes have all
    the features we expect. It also is a good indication of what we want to
    highlight so we can have these promoted better in the release notes.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds release notes for Data Prepper 2.16.0. (#6951)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 1 Jul 2026 09:05:38 -0700
    
    
    Adds release notes for Data Prepper 2.16.0.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update fast-uri and brace-expansion to fix CVEs CVE-2026-6322,CVE-2026-6321 (#6969)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 1 Jul 2026 07:31:18 -0700
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Adds a new Claude command for generating the Thank You text and notes this command in the documentation for it. (#6950)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 1 Jul 2026 07:27:41 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the next version to 2.17. (#6949)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 30 Jun 2026 15:53:14 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Onboard new backport-pr re-usable github workflow (data-prepper) (#6939)__

    [Peter Zhu](mailto:zhujiaxi@amazon.com) - Tue, 30 Jun 2026 15:39:50 -0700
    
    
    - Replace old backport workflow with reusable workflow from opensearch-build
    - Remove obsolete backport-related workflows
    
    Signed-off-by: Peter Zhu &lt;zhujiaxi@amazon.com&gt;

* __Add classified error counters to CloudWatch Logs sink (#6943)__

    [kiran536](mailto:veeravelly.kirankumar@gmail.com) - Tue, 30 Jun 2026 12:57:53 -0700
    
    
    * Add classified error counters to CloudWatch Logs sink
    
    Introduces three per-class error counters (cloudWatchLogsAccessDenied, 
    cloudWatchLogsResourceNotFound, cloudWatchLogsThrottled) so operators can alarm
    on specific failure modes rather than relying on the single opaque
    cloudWatchLogsRequestsFailed total.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;kiran536@gmail.com&gt; Signed-off-by: Kiran
    Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    * Add forkEvery to integrationTest task for JVM isolation
    
    Prevents Mockito state from CloudWatchLogsIT (which requires real AWS 
    credentials) from leaking into CloudWatchLogsMetricsIT when both test classes
    run in the same Gradle task.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;kiran536@gmail.com&gt; Signed-off-by: Kiran
    Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    * Address review: robust AccessDenied matching and surface creation failures
    
    - Use contains(&#34;AccessDenied&#34;) instead of equals(&#34;AccessDeniedException&#34;)
     to handle both AWS error code variants (AccessDenied, AccessDeniedException)
    - Classify and count access-denied exceptions in createResources() so that
     permission problems during log group/stream creation are no longer invisible
    - Add isAccessDenied() and classifyAndCountResourceCreationFailure() helpers
    - Add unit tests for AccessDenied (no suffix), createLogStream access denied,
     createLogGroup access denied, and negative misclassification assertion
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    * Gate resourceNotFound counter on user-owned provisioning
    
    Only increment cloudWatchLogsResourceNotFound when createLogGroup and 
    createLogStream are both disabled. When either flag is on, the user delegated
    resource creation to the sink — a subsequent RNF is internal control flow, not
    an actionable user signal.
    
    Adds tests for both partial-flag permutations (createLogStream-only, 
    createLogGroup-only) verifying the counter is suppressed.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    * Document new classified error counters in README
    
    Add cloudWatchLogsThrottled, cloudWatchLogsAccessDenied, and 
    cloudWatchLogsResourceNotFound to the Metrics section with descriptions of when
    each counter is incremented and its semantic meaning.
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;veeravkk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kiran Kumar Veeravelly &lt;kiran536@gmail.com&gt; Signed-off-by: Kiran
    Kumar Veeravelly &lt;veeravkk@amazon.com&gt; Co-authored-by: Kiran Kumar Veeravelly
    &lt;veeravkk@amazon.com&gt;


