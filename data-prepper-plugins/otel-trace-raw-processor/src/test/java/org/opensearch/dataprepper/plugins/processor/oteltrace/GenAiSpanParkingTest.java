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
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltrace.GenAiEnrichmentHelper.DERIVED_GEN_AI_IS_ROOT_KEY;

/**
 * A GenAI span whose ancestor chain reaches a span not seen yet is held back up to genai_root_hold,
 * so it is not marked as a second AI root of its trace before its ancestors arrive.
 */
class GenAiSpanParkingTest {

    private static final String TRACE_ID = "PARKING_TRACE";

    private PluginMetrics pluginMetrics;
    private Duration genAiRootHold;

    @BeforeEach
    void setUp() {
        pluginMetrics = mock(PluginMetrics.class);
        genAiRootHold = OtelTraceRawProcessorConfig.DEFAULT_GENAI_ROOT_HOLD;
    }

    @Test
    void test_when_aGenAiSpansParentNeverArrives_then_itIsReleasedAtTheDeadlineAndMarked() {
        genAiRootHold = Duration.ofMillis(200);
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, httpRoot());
        assertThat(execute(processor, agentSpan("S", "A")), is(empty()));
        assertThat(parkedSpanCount(), equalTo(1.0));

        waitAtLeast(300);
        final List<Span> released = execute(processor);

        assertThat(spanIds(released), contains("S"));
        assertThat(released.get(0).getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
        assertThat(parkedSpanCount(), equalTo(0.0));
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void test_when_genAiRootHoldIsZero_then_aSpanWithAnUnknownParentIsMarkedImmediately() {
        genAiRootHold = Duration.ZERO;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, httpRoot());
        final List<Span> emitted = execute(processor, agentSpan("S", "A"));

        assertThat(spanIds(emitted), contains("S"));
        assertThat(emitted.get(0).getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
        assertThat(parkedSpanCount(), equalTo(0.0));
    }

    @Test
    void test_when_shuttingDown_then_parkedSpansAreFlushedAndCountAgainstReadiness() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, httpRoot());
        execute(processor, agentSpan("S", "A"));
        assertThat(processor.isReadyForShutdown(), is(false));

        processor.prepareForShutdown();
        final List<Span> flushed = execute(processor);

        assertThat(spanIds(flushed), contains("S"));
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void test_when_aSpanIsParked_then_itsDescendantsAreEmittedUnmarkedRightAway() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, httpRoot());
        execute(processor, agentSpan("S", "A"));
        final List<Span> chatBatch = execute(processor, chatSpan("C", "S"));

        assertThat(spanIds(chatBatch), contains("C"));
        assertThat(chatBatch.get(0).getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), nullValue());
        assertThat(parkedSpanCount(), equalTo(1.0));

        final List<Span> agentBatch = execute(processor, agentSpan("A", "H"));

        assertThat(spanIds(agentBatch), contains("A", "S"));
        assertThat(markedSpanIds(agentBatch), contains("A"));
        assertThat(parkedSpanCount(), equalTo(0.0));
    }

    @Test
    void test_when_aNonGenAiSpanArrivesAloneBetweenTwoAgents_then_itsLinkKeepsTheLowerAgentUnmarked() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, httpRoot());
        final List<Span> agentBatch = execute(processor, agentSpan("A", "H"));
        // An HTTP client span between the agents: no GenAI data, so only its parent link is recorded.
        final List<Span> clientBatch = execute(processor, span("N", "A", Map.of("http.method", "GET")));
        final List<Span> lowerAgentBatch = execute(processor, agentSpan("S", "N"));

        assertThat(markedSpanIds(agentBatch), contains("A"));
        assertThat(spanIds(clientBatch), contains("N"));
        assertThat(spanIds(lowerAgentBatch), contains("S"));
        assertThat(markedSpanIds(lowerAgentBatch), is(empty()));
    }

    @Test
    void test_when_aParkedSpanIsReleasedAndStillUnresolved_then_itKeepsItsOriginalDeadline() {
        genAiRootHold = Duration.ofMillis(1_000);
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, httpRoot());
        final long parkedAtMillis = System.currentTimeMillis();
        final List<Span> emitted = new ArrayList<>(execute(processor, agentSpan("S", "A")));

        waitUntil(parkedAtMillis + 600);
        // A span of the same trace releases S for re-evaluation; its parent is still unknown.
        emitted.addAll(execute(processor, span("X", "H", Map.of("http.method", "GET"))));
        waitUntil(parkedAtMillis + 1_100);
        emitted.addAll(execute(processor));

        assertThat(spanIds(emitted).stream().filter("S"::equals).count(), equalTo(1L));
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void isOutermostResolvable_isFalseUntilTheParentIsKnown_andTrueOnceItIsCoveredOrTheRootIsAGenAiRoot() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));
        final Span lowerAgent = agentSpan("S", "A");

        assertThat(GenAiEnrichmentHelper.isOutermostResolvable(lowerAgent, traceState, traceId -> Optional.of("H")), is(false));
        assertThat(GenAiEnrichmentHelper.isOutermostResolvable(lowerAgent, traceState, traceId -> Optional.of("A")), is(true));

        traceState.update(TRACE_ID, state -> state.cover("A"));
        assertThat(GenAiEnrichmentHelper.isOutermostResolvable(lowerAgent, traceState, traceId -> Optional.empty()), is(true));

        final GenAiTraceState genAiRootState = GenAiTraceState.create(100, Duration.ofMinutes(1));
        genAiRootState.update(TRACE_ID, GenAiTraceState.TraceState::recordGenAiRoot);
        assertThat(GenAiEnrichmentHelper.isOutermostResolvable(lowerAgent, genAiRootState, traceId -> Optional.empty()), is(true));
    }

    private OTelTraceRawProcessor createObjectUnderTest() {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.isGenAiEnrichmentDisabled()).thenReturn(false);
        when(config.getGenAiCoverageCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE);
        when(config.getGenAiRootPropagation()).thenReturn(GenAiRootPropagation.AI_ROOT);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        when(config.getGenAiRootHold()).thenReturn(genAiRootHold);
        return new OTelTraceRawProcessor(config, mock(PipelineDescription.class), pluginMetrics);
    }

    private static List<Span> execute(final OTelTraceRawProcessor processor, final Span... spans) {
        final List<Record<Span>> records = new ArrayList<>();
        for (final Span span : spans) {
            records.add(new Record<>(span));
        }
        return processor.doExecute(records).stream().map(Record::getData).collect(Collectors.toList());
    }

    private static Span httpRoot() {
        return JacksonSpan.builder()
                .withTraceId(TRACE_ID)
                .withSpanId("H")
                .withParentSpanId("")
                .withTraceState("")
                .withName("H")
                .withKind("SERVER")
                .withDurationInNanos(1L)
                .withStartTime("2026-01-01T00:00:00Z")
                .withEndTime("2026-01-01T00:01:00Z")
                .withTraceGroup("POST /chat")
                .withTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withStatusCode(0)
                        .withEndTime("2026-01-01T00:01:00Z")
                        .withDurationInNanos(1L)
                        .build())
                .withAttributes(new HashMap<>(Map.of("http.method", "POST")))
                .build();
    }

    private static Span agentSpan(final String spanId, final String parentSpanId) {
        return span(spanId, parentSpanId, Map.of("gen_ai.operation.name", "invoke_agent"));
    }

    private static Span chatSpan(final String spanId, final String parentSpanId) {
        return span(spanId, parentSpanId, Map.of(
                "gen_ai.operation.name", "chat",
                "gen_ai.request.model", "gpt-4o",
                "gen_ai.usage.input_tokens", 150L,
                "gen_ai.usage.output_tokens", 85L));
    }

    private static Span span(final String spanId, final String parentSpanId, final Map<String, Object> attributes) {
        return JacksonSpan.builder()
                .withTraceId(TRACE_ID)
                .withSpanId(spanId)
                .withParentSpanId(parentSpanId)
                .withTraceState("")
                .withName(spanId)
                .withKind("INTERNAL")
                .withDurationInNanos(1L)
                .withStartTime("2026-01-01T00:00:01Z")
                .withEndTime("2026-01-01T00:01:00Z")
                .withTraceGroup(null)
                .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                .withAttributes(new HashMap<>(attributes))
                .build();
    }

    private static List<String> spanIds(final Collection<Span> spans) {
        return spans.stream().map(Span::getSpanId).sorted().collect(Collectors.toList());
    }

    private static List<String> markedSpanIds(final Collection<Span> spans) {
        return spans.stream()
                .filter(span -> Boolean.TRUE.equals(span.getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY)))
                .map(Span::getSpanId)
                .sorted()
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private double parkedSpanCount() {
        final ArgumentCaptor<Object> measured = ArgumentCaptor.forClass(Object.class);
        final ArgumentCaptor<ToDoubleFunction> function = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq(OTelTraceRawProcessor.GENAI_PARKED_SPAN_COUNT_METRIC_NAME), measured.capture(),
                function.capture());
        return function.getValue().applyAsDouble(measured.getValue());
    }

    private static void waitAtLeast(final long delayMillis) {
        waitUntil(System.currentTimeMillis() + delayMillis);
    }

    private static void waitUntil(final long epochMillis) {
        await().atMost(Math.max(0, epochMillis - System.currentTimeMillis()) + 5_000, TimeUnit.MILLISECONDS)
                .until(() -> System.currentTimeMillis() >= epochMillis);
    }
}
