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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class GenAiRootEnrichmentTest {

    private static final String TRACE_ID = "ROOT_ENRICHMENT_TRACE";

    private GenAiRootEnrichmentState state;

    @BeforeEach
    void setUp() {
        state = GenAiRootEnrichmentState.create(1_000, Duration.ofMinutes(1));
    }

    @Test
    void enrichRoots_whenTheChildrenWereRecordedInAnEarlierBatch_enrichesARootAloneInItsBatch() {
        enrich(chatSpan("chat", "agent", "gpt-4o", 150L, 85L));

        final Span root = span("agent", "", Map.of("gen_ai.operation.name", "invoke_agent"));
        enrich(root);

        assertThat(root.getAttributes().get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(150L));
        assertThat(tokens(root, "gen_ai.usage.output_tokens"), equalTo(85L));
    }

    @Test
    void enrichRoots_withChildrenInTheSameBatch_prefersTheOutermostAgentWhateverTheBatchOrder() {
        final Span root = span("http", "", Map.of("http.method", "POST"));
        final List<Span> spans = new ArrayList<>(List.of(
                root,
                span("agent", "http", "2026-01-01T00:00:01Z", Map.of(
                        "gen_ai.operation.name", "invoke_agent", "gen_ai.agent.name", "Travel Planner")),
                span("inner", "agent", "2026-01-01T00:00:03Z", Map.of(
                        "gen_ai.operation.name", "invoke_agent", "gen_ai.agent.name", "Weather Agent"))));
        Collections.reverse(spans);

        GenAiRootEnrichment.enrichRoots(spans, state, GenAiRootPropagation.ALL_ROOTS);

        assertThat(root.getAttributes().get("gen_ai.agent.name"), equalTo("Travel Planner"));
    }

    @Test
    void enrichRoots_acrossBatches_keepsTheFirstValueRecordedForEachAttribute() {
        enrich(chatSpan("chat1", "agent", "gpt-4o", 1L, 1L));
        enrich(chatSpan("chat2", "agent", "claude", 1L, 1L));

        final Span root = span("agent", "", Map.of("gen_ai.operation.name", "invoke_agent"));
        enrich(root);

        assertThat(root.getAttributes().get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(2L));
    }

    @Test
    void enrichRoots_whenAnAgentAndItsCallReportUsageInDifferentBatches_countsTheAgentOnlyInEitherOrder() {
        final GenAiRootEnrichmentState callFirstState = GenAiRootEnrichmentState.create(1_000, Duration.ofMinutes(1));
        final Span agentFirstRoot = span("root", "", Map.of("gen_ai.operation.name", "invoke_agent"));
        final Span callFirstRoot = span("root", "", Map.of("gen_ai.operation.name", "invoke_agent"));

        enrich(state, agentSpanWithTokens());
        enrich(state, chatSpan("chat", "agent", "gpt-4o", 10L, 4L));
        enrich(state, agentFirstRoot);
        enrich(callFirstState, chatSpan("chat", "agent", "gpt-4o", 10L, 4L));
        enrich(callFirstState, agentSpanWithTokens());
        enrich(callFirstState, callFirstRoot);

        for (final Span root : List.of(agentFirstRoot, callFirstRoot)) {
            assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(15L));
            assertThat(tokens(root, "gen_ai.usage.output_tokens"), equalTo(7L));
        }
    }

    @Test
    void enrichRoots_withAiRootPropagation_leavesANonGenAiRootUnchanged() {
        enrich(chatSpan("chat", "agent", "gpt-4o", 150L, 85L));

        final Span root = span("http", "", Map.of("http.method", "POST"));
        enrich(root);

        assertThat(root.getAttributes(), equalTo(Map.of("http.method", "POST")));
    }

    @Test
    void enrichRoots_withAllRootsPropagation_enrichesANonGenAiRootFromEarlierBatches() {
        GenAiRootEnrichment.enrichRoots(List.of(chatSpan("chat", "agent", "gpt-4o", 150L, 85L)), state,
                GenAiRootPropagation.ALL_ROOTS);

        final Span root = span("http", "", Map.of("http.method", "POST"));
        GenAiRootEnrichment.enrichRoots(List.of(root), state, GenAiRootPropagation.ALL_ROOTS);

        assertThat(root.getAttributes().get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(150L));
    }

    @Test
    void enrichRoots_withNoPropagation_changesNothingAndRecordsNothing() {
        final Span chat = chatSpan("chat", "agent", "gpt-4o", 150L, 85L);
        final Span root = span("agent", "", Map.of("gen_ai.operation.name", "invoke_agent"));

        GenAiRootEnrichment.enrichRoots(List.of(chat, root), state, GenAiRootPropagation.NONE);

        assertThat(root.getAttributes(), equalTo(Map.of("gen_ai.operation.name", "invoke_agent")));
        assertThat(state.estimatedSize(), equalTo(0L));
    }

    @Test
    void enrichRoots_normalizesVendorAttributesBeforeRecordingThem() {
        enrich(span("llm", "agent", Map.of(
                "openinference.span.kind", "LLM", "llm.model_name", "gpt-4o", "llm.token_count.prompt", 12L)));

        final Span root = span("agent", "", Map.of("gen_ai.operation.name", "invoke_agent"));
        enrich(root);

        assertThat(root.getAttributes().get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(12L));
    }

    @Test
    void enrichRoots_keepsTheRootsOwnValues() {
        enrich(chatSpan("chat", "agent", "gpt-4o", 150L, 85L));

        final Span root = span("agent", "", Map.of(
                "gen_ai.operation.name", "invoke_agent", "gen_ai.request.model", "own-model", "gen_ai.usage.input_tokens", 1L));
        enrich(root);

        assertThat(root.getAttributes().get("gen_ai.request.model"), equalTo("own-model"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(1L));
        assertThat(root.getAttributes().get("gen_ai.usage.output_tokens"), nullValue());
    }

    @Test
    void isGenAiOrMarkedSpan_isTrueForAGenAiSpanOrASpanMarkedAtTheSource() {
        assertThat(GenAiRootEnrichment.isGenAiOrMarkedSpan(span("a", "", Map.of("gen_ai.operation.name", "chat"))), is(true));
        assertThat(GenAiRootEnrichment.isGenAiOrMarkedSpan(span("a", "", Map.of("derived.gen_ai.is_root", "true"))), is(true));
        assertThat(GenAiRootEnrichment.isGenAiOrMarkedSpan(span("a", "", Map.of("derived.gen_ai.is_root", false))), is(false));
        assertThat(GenAiRootEnrichment.isGenAiOrMarkedSpan(span("a", "", Map.of("http.method", "POST"))), is(false));
    }

    @Test
    void rootAwaitsEnrichment_isTrueOnlyForAGenAiRootMissingDataTheStateCannotProvide() {
        final Span genAiRoot = span("root", "", Map.of("gen_ai.operation.name", "invoke_agent"));
        final Span httpRoot = span("root", "", Map.of("http.method", "POST"));
        final Span completeRoot = span("root", "", Map.of(
                "gen_ai.operation.name", "invoke_agent", "gen_ai.request.model", "gpt-4o", "gen_ai.usage.output_tokens", 1L));

        assertThat(GenAiRootEnrichment.rootAwaitsEnrichment(genAiRoot, state, GenAiRootPropagation.AI_ROOT), is(true));
        assertThat(GenAiRootEnrichment.rootAwaitsEnrichment(genAiRoot, state, GenAiRootPropagation.ALL_ROOTS), is(true));
        assertThat(GenAiRootEnrichment.rootAwaitsEnrichment(genAiRoot, state, GenAiRootPropagation.NONE), is(false));
        assertThat(GenAiRootEnrichment.rootAwaitsEnrichment(httpRoot, state, GenAiRootPropagation.ALL_ROOTS), is(false));
        assertThat(GenAiRootEnrichment.rootAwaitsEnrichment(completeRoot, state, GenAiRootPropagation.AI_ROOT), is(false));

        enrich(span("agent", "root", Map.of("gen_ai.operation.name", "invoke_agent", "gen_ai.usage.input_tokens", 3L)));
        assertThat(GenAiRootEnrichment.rootAwaitsEnrichment(genAiRoot, state, GenAiRootPropagation.AI_ROOT), is(true));

        enrich(chatSpan("chat", "agent", "gpt-4o", 1L, 1L));
        assertThat(GenAiRootEnrichment.rootAwaitsEnrichment(genAiRoot, state, GenAiRootPropagation.AI_ROOT), is(false));
    }

    private void enrich(final Span span) {
        enrich(state, span);
    }

    private static void enrich(final GenAiRootEnrichmentState state, final Span span) {
        GenAiRootEnrichment.enrichRoots(new ArrayList<>(List.of(span)), state, GenAiRootPropagation.AI_ROOT);
    }

    private static Span agentSpanWithTokens() {
        return span("agent", "root", Map.of(
                "gen_ai.operation.name", "invoke_agent", "gen_ai.usage.input_tokens", 15L, "gen_ai.usage.output_tokens", 7L));
    }

    private static Span chatSpan(final String spanId, final String parentSpanId, final String model,
                          final long inputTokens, final long outputTokens) {
        return span(spanId, parentSpanId, Map.of(
                "gen_ai.operation.name", "chat",
                "gen_ai.request.model", model,
                "gen_ai.usage.input_tokens", inputTokens,
                "gen_ai.usage.output_tokens", outputTokens));
    }

    private static long tokens(final Span span, final String key) {
        return ((Number) span.getAttributes().get(key)).longValue();
    }

    private static Span span(final String spanId, final String parentSpanId, final Map<String, Object> attributes) {
        return span(spanId, parentSpanId, "2026-01-01T00:00:00Z", attributes);
    }

    private static Span span(final String spanId, final String parentSpanId, final String startTime,
                             final Map<String, Object> attributes) {
        return JacksonSpan.builder()
                .withTraceId(TRACE_ID)
                .withSpanId(spanId)
                .withParentSpanId(parentSpanId)
                .withTraceState("")
                .withName(spanId)
                .withKind("INTERNAL")
                .withDurationInNanos(1L)
                .withStartTime(startTime)
                .withEndTime("2026-01-01T00:01:00Z")
                .withTraceGroup(null)
                .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                .withAttributes(new HashMap<>(attributes))
                .build();
    }
}
