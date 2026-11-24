/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import org.opensearch.dataprepper.plugins.otel.common.EnvironmentSource;

import java.time.Duration;

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_traces</code> processor completes trace-group-related fields in all incoming " +
        "span records by state caching the root span information for each <code>traceId</code>.")
public class OtelTraceRawProcessorConfig {
    static final long DEFAULT_TG_FLUSH_INTERVAL_SEC = 180L;
    static final long DEFAULT_GENAI_ROOT_ENRICHMENT_CACHE_MAX_SIZE = 500_000L;
    static final Duration DEFAULT_GENAI_ROOT_HOLD = Duration.ZERO;
    static final long DEFAULT_GENAI_ROOT_HOLD_MAX_SIZE = 10_000L;
    static final long MAX_TRACE_ID_CACHE_SIZE = 1_000_000L;
    static final long DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE = 500_000L;

    @JsonProperty(value = "trace_flush_interval", defaultValue = "180")
    @JsonPropertyDescription("Represents the time interval in seconds to flush all the descendant spans without any " +
            "root span. Default is <code>180</code>.")
    private long traceFlushInterval = DEFAULT_TG_FLUSH_INTERVAL_SEC;

    @JsonProperty(value = "trace_group_cache_ttl", defaultValue = "PT180S")
    @JsonPropertyDescription("Represents the time-to-live to cache a trace group details. " +
            "The value may be an ISO 8601 notation such as <code>PT1M30S</code> or a duration and unit such as <code>45s</code>. " +
            "Defaults to the value of <code>trace_flush_interval</code> (180 seconds unless configured), so that child spans " +
            "arriving while orphaned spans are still held can be completed with trace group information.")
    private Duration traceGroupCacheTimeToLive;

    @JsonProperty(value = "trace_group_cache_max_size", defaultValue = "1000000")
    @JsonPropertyDescription("Represents the maximum size of the cache to store the trace group details from root spans. " +
            "Default is <code>1000000</code>.")
    private long traceGroupCacheMaxSize = MAX_TRACE_ID_CACHE_SIZE;

    @JsonProperty(value = "disable_genai_enrichment", defaultValue = "false")
    @JsonPropertyDescription("Turns off GenAI agent trace enrichment: normalizing vendor attributes to <code>gen_ai.*</code>, " +
            "stripping flattened sub-keys that conflict with their parent, marking agent root spans and copying agent data " +
            "onto trace roots. Traces without GenAI data are left unchanged either way. Default is <code>false</code>.")
    private boolean disableGenAiEnrichment = false;

    @JsonProperty(value = "genai_coverage_cache_max_size", defaultValue = "500000")
    @JsonPropertyDescription("Represents the maximum number of span ids to keep in the cache of GenAI spans already seen " +
            "per trace, used so that GenAI spans arriving after their ancestors are not marked as a trace's AI root. " +
            "Default is <code>500000</code>.")
    @Min(1)
    private Long genAiCoverageCacheMaxSize;

    @JsonProperty(value = "genai_root_propagation", defaultValue = "ai_root")
    @JsonPropertyDescription("Which trace root spans get <code>gen_ai.system</code>, <code>gen_ai.provider.name</code>, " +
            "<code>gen_ai.agent.name</code>, <code>gen_ai.request.model</code> and token totals copied from their children. " +
            "<code>ai_root</code> copies them only onto a root that is its trace's AI root (a GenAI span, or a span marked " +
            "<code>derived.gen_ai.is_root: true</code> at the source). <code>all_roots</code> also copies them onto " +
            "non-GenAI roots, such as the HTTP span an agent runs behind. <code>none</code> copies nothing. " +
            "Default is <code>ai_root</code>.")
    private GenAiRootPropagation genAiRootPropagation;

    @JsonProperty(value = "environment_source", defaultValue = "platform_first")
    @JsonPropertyDescription("Precedence for <code>attributes.derived.environment</code> on SERVER spans. " +
            "<code>platform_first</code> (default) uses the AWS platform (e.g. <code>eks:default</code>) before " +
            "<code>deployment.environment.name</code> / <code>deployment.environment</code>; <code>deployment_first</code> " +
            "uses the deployment environment first. Set it to the same value as on <code>otel_apm_service_map</code>, " +
            "so spans and their service agree.")
    private EnvironmentSource environmentSource = EnvironmentSource.PLATFORM_FIRST;

    public EnvironmentSource getEnvironmentSource() {
        return environmentSource != null ? environmentSource : EnvironmentSource.PLATFORM_FIRST;
    }

    @JsonProperty(value = "genai_root_marking", defaultValue = "outermost")
    @JsonPropertyDescription("Which spans are marked <code>derived.gen_ai.is_root: true</code> as agent roots. " +
            "<code>outermost</code> marks the outermost GenAI span of each trace. <code>agent_spans</code> marks every " +
            "<code>invoke_agent</code> and <code>invoke_workflow</code> span, whatever the batch or arrival order, " +
            "including nested agents. <code>none</code> marks nothing and keeps only markers set at the source. " +
            "A marker set at the source is never changed. Default is <code>outermost</code>.")
    private GenAiRootMarking genAiRootMarking;

    @JsonProperty(value = "genai_root_enrichment_cache_max_size", defaultValue = "500000")
    @JsonPropertyDescription("Represents the maximum number of span ids to keep in the cache of what trace roots are " +
            "enriched from: per trace, the gen_ai.* values and token usage of children processed in other batches than their " +
            "root, and the parent links needed to count each agent's token usage once. Default is <code>500000</code>.")
    @Min(1)
    private Long genAiRootEnrichmentCacheMaxSize;

    @JsonProperty(value = "genai_root_hold", defaultValue = "PT0S")
    @JsonPropertyDescription("How long to hold a GenAI root span that arrives before the child spans that carry its " +
            "model or token usage, so it can be filled from them. A held root is written when its hold expires, with " +
            "what its children reported by then. Without a hold, such a root is written as it arrives, before its " +
            "children. Use for root-first setups such as streaming responses, for example <code>30s</code>. Must not be " +
            "longer than <code>trace_flush_interval</code>. The value may be an ISO 8601 notation such as " +
            "<code>PT30S</code> or a duration and unit such as <code>30s</code>. Default is <code>0</code>, no hold.")
    private Duration genAiRootHold;

    @JsonProperty(value = "genai_root_hold_max_size", defaultValue = "10000")
    @JsonPropertyDescription("Represents the maximum number of root spans held at once by <code>genai_root_hold</code>. " +
            "When holding one more root would exceed it, the root held longest is written early. " +
            "Default is <code>10000</code>.")
    @Min(1)
    private Long genAiRootHoldMaxSize;

    public long getTraceFlushIntervalSeconds() {
        return traceFlushInterval;
    }

    public Duration getTraceGroupCacheTimeToLive() {
        return traceGroupCacheTimeToLive != null ? traceGroupCacheTimeToLive : Duration.ofSeconds(traceFlushInterval);
    }

    public long getTraceGroupCacheMaxSize() {
        return traceGroupCacheMaxSize;
    }

    public long getGenAiRootEnrichmentCacheMaxSize() {
        return genAiRootEnrichmentCacheMaxSize != null ? genAiRootEnrichmentCacheMaxSize : DEFAULT_GENAI_ROOT_ENRICHMENT_CACHE_MAX_SIZE;
    }

    public Duration getGenAiRootHold() {
        return genAiRootHold != null ? genAiRootHold : DEFAULT_GENAI_ROOT_HOLD;
    }

    public long getGenAiRootHoldMaxSize() {
        return genAiRootHoldMaxSize != null ? genAiRootHoldMaxSize : DEFAULT_GENAI_ROOT_HOLD_MAX_SIZE;
    }

    @AssertTrue(message = "genai_root_enrichment_cache_max_size, genai_root_hold and genai_root_hold_max_size cannot be set " +
            "when disable_genai_enrichment is true")
    boolean isGenAiRootEnrichmentSettingsValid() {
        return !disableGenAiEnrichment
                || (genAiRootEnrichmentCacheMaxSize == null && genAiRootHold == null && genAiRootHoldMaxSize == null);
    }

    @AssertTrue(message = "genai_root_hold must be 0 or more and not longer than trace_flush_interval")
    boolean isGenAiRootHoldValid() {
        return genAiRootHold == null
                || (!genAiRootHold.isNegative() && genAiRootHold.compareTo(Duration.ofSeconds(traceFlushInterval)) <= 0);
    }

    public boolean isGenAiEnrichmentDisabled() {
        return disableGenAiEnrichment;
    }

    public long getGenAiCoverageCacheMaxSize() {
        return genAiCoverageCacheMaxSize != null ? genAiCoverageCacheMaxSize : DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE;
    }

    public GenAiRootPropagation getGenAiRootPropagation() {
        return genAiRootPropagation != null ? genAiRootPropagation : GenAiRootPropagation.AI_ROOT;
    }

    public GenAiRootMarking getGenAiRootMarking() {
        return genAiRootMarking != null ? genAiRootMarking : GenAiRootMarking.OUTERMOST;
    }

    @AssertTrue(message = "genai_root_propagation, genai_root_marking and genai_coverage_cache_max_size cannot be set " +
            "when disable_genai_enrichment is true")
    boolean isGenAiSettingsValid() {
        return !disableGenAiEnrichment
                || (genAiRootPropagation == null && genAiRootMarking == null && genAiCoverageCacheMaxSize == null);
    }
}
