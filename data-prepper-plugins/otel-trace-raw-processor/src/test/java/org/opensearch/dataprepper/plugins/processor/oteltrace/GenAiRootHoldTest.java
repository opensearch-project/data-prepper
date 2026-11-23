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

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltrace.GenAiEnrichmentHelper.DERIVED_GEN_AI_IS_ROOT_KEY;

/**
 * A GenAI root span that is still missing a model or token usage is held up to genai_root_hold,
 * so children processed in later batches can fill it before it is written. It is released when
 * its hold expires, on shutdown, when the cap on held roots releases it, or when a newer root of
 * the same trace replaces it.
 */
class GenAiRootHoldTest {

    private static final String TRACE_ID = "ROOT_HOLD_TRACE";
    private static final Duration LONG_HOLD = Duration.ofSeconds(30);

    private PluginMetrics pluginMetrics;
    private Map<String, Counter> counters;
    private Duration genAiRootHold;
    private long genAiRootHoldMaxSize;
    private GenAiRootPropagation genAiRootPropagation;
    private GenAiRootMarking genAiRootMarking;

    @BeforeEach
    void setUp() {
        pluginMetrics = mock(PluginMetrics.class);
        counters = new HashMap<>();
        when(pluginMetrics.counter(anyString())).thenAnswer(invocation ->
                counters.computeIfAbsent(invocation.getArgument(0), name -> mock(Counter.class)));
        genAiRootHold = LONG_HOLD;
        genAiRootHoldMaxSize = OtelTraceRawProcessorConfig.DEFAULT_GENAI_ROOT_HOLD_MAX_SIZE;
        genAiRootPropagation = GenAiRootPropagation.AI_ROOT;
    }

    @Test
    void test_when_genAiRootHoldIsTheDefault_then_aRootThatArrivesFirstIsWrittenImmediately() {
        genAiRootHold = OtelTraceRawProcessorConfig.DEFAULT_GENAI_ROOT_HOLD;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        final List<Span> rootBatch = execute(processor, agentRoot());

        assertThat(spanIds(rootBatch), contains("root"));
        assertThat(rootBatch.get(0).getAttributes().get("gen_ai.request.model"), nullValue());
        assertThat(gauge(OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(0.0));
    }

    @Test
    void test_when_rootArrivesBeforeItsChildren_then_rootIsHeldUntilItsDeadlineAndWrittenEnriched() {
        genAiRootHold = Duration.ofMillis(200);
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, agentRoot()), is(empty()));
        // A child that carries a model and tokens does not release the root early.
        assertThat(spanIds(execute(processor, chatSpan("chat", "root", "gpt-4o", 150L, 85L))), contains("chat"));
        assertThat(gauge(OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(1.0));

        final List<Span> released = executeUntilEmitted(processor);

        assertThat(spanIds(released), contains("root"));
        final Map<String, Object> root = released.get(0).getAttributes();
        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(150L));
        assertThat(tokens(root, "gen_ai.usage.output_tokens"), equalTo(85L));
        assertThat(gauge(OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(0.0));
        verify(counter(OTelTraceRawProcessor.GENAI_HELD_ROOT_EXPIRED_COUNT_METRIC_NAME)).increment();
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void test_when_noChildArrives_then_heldRootIsWrittenAtTheDeadlineWithoutModelOrTokens() {
        genAiRootHold = Duration.ofMillis(200);
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, agentRoot()), is(empty()));
        final List<Span> released = executeUntilEmitted(processor);

        assertThat(spanIds(released), contains("root"));
        assertThat(released.get(0).getAttributes().get("gen_ai.request.model"), nullValue());
        assertThat(released.get(0).getAttributes().get("gen_ai.usage.input_tokens"), nullValue());
        verify(counter(OTelTraceRawProcessor.GENAI_HELD_ROOT_EXPIRED_COUNT_METRIC_NAME)).increment();
    }

    @Test
    void test_when_childrenAreWaitingInMemory_then_rootIsNotHeldAndIsEnrichedImmediately() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, chatSpan("chat", "root", "gpt-4o", 150L, 85L)), is(empty()));
        final List<Span> rootBatch = execute(processor, agentRoot());

        assertThat(spanIds(rootBatch), containsInAnyOrder("root", "chat"));
        final Map<String, Object> root = byId(rootBatch, "root").getAttributes();
        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(150L));
        assertThat(gauge(OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(0.0));
    }

    @Test
    void test_when_anAgentReportsUsageBeforeItsCall_then_rootGetsTheAgentsTotalOnly() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, agentRoot());
        assertThat(spanIds(execute(processor, agentSpan(null, 15L, 7L))), contains("agent"));
        assertThat(spanIds(execute(processor, chatSpan("chat", "agent", "gpt-4o", 10L, 4L))), contains("chat"));
        final Map<String, Object> root = byId(releaseByShutdown(processor), "root").getAttributes();

        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(15L));
        assertThat(tokens(root, "gen_ai.usage.output_tokens"), equalTo(7L));
    }

    @Test
    void test_when_aCallReportsUsageBeforeItsAgent_then_rootGetsTheAgentsTotalOnly() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, agentRoot());
        assertThat(spanIds(execute(processor, chatSpan("chat", "agent", null, 10L, 4L))), contains("chat"));
        assertThat(spanIds(execute(processor, agentSpan("gpt-4o", 15L, 7L))), contains("agent"));
        final Map<String, Object> root = byId(releaseByShutdown(processor), "root").getAttributes();

        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(15L));
        assertThat(tokens(root, "gen_ai.usage.output_tokens"), equalTo(7L));
    }

    @Test
    void test_when_childrenArriveDuringTheHold_then_theRootStaysHeldAndCountsAgainstReadiness() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, agentRoot());
        final List<Span> toolBatch = execute(processor, span("tool", "root", Map.of("gen_ai.operation.name", "execute_tool")));
        final List<Span> chatBatch = execute(processor, chatSpan("chat", "tool", "gpt-4o", 10L, 4L));

        assertThat(spanIds(toolBatch), contains("tool"));
        assertThat(spanIds(chatBatch), contains("chat"));
        assertThat(gauge(OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(1.0));
        assertThat(processor.isReadyForShutdown(), is(false));
    }

    @Test
    void test_when_rootIsNotAGenAiSpan_then_rootIsNeverHeldInAnyPropagationMode() {
        for (final GenAiRootPropagation propagation : GenAiRootPropagation.values()) {
            genAiRootPropagation = propagation;
            final OTelTraceRawProcessor processor = createObjectUnderTest();

            assertThat(propagation.toString(), spanIds(execute(processor, span("root", "", Map.of("http.method", "POST")))),
                    contains("root"));
        }
    }

    @Test
    void test_when_propagationIsAllRoots_then_aGenAiRootIsHeldAndEnrichedLikeWithAiRoot() {
        genAiRootPropagation = GenAiRootPropagation.ALL_ROOTS;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, agentRoot()), is(empty()));
        execute(processor, chatSpan("chat", "root", "gpt-4o", 150L, 85L));

        assertThat(byId(releaseByShutdown(processor), "root").getAttributes().get("gen_ai.request.model"), equalTo("gpt-4o"));
    }

    @Test
    void test_when_propagationIsNone_then_aGenAiRootIsNeitherHeldNorEnriched() {
        genAiRootPropagation = GenAiRootPropagation.NONE;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        final List<Span> rootBatch = execute(processor, agentRoot());
        execute(processor, chatSpan("chat", "root", "gpt-4o", 150L, 85L));

        assertThat(spanIds(rootBatch), contains("root"));
        assertThat(rootBatch.get(0).getAttributes().get("gen_ai.request.model"), nullValue());
        assertThat(gauge(OTelTraceRawProcessor.GENAI_ROOT_ENRICHMENT_COUNT_METRIC_NAME), equalTo(0.0));
    }

    @Test
    void test_when_rootAlreadyHasModelAndTokens_then_rootIsNotHeld() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        final List<Span> batch = execute(processor, span("root", "", Map.of(
                "gen_ai.operation.name", "invoke_agent", "gen_ai.request.model", "gpt-4o", "gen_ai.usage.input_tokens", 12L)));

        assertThat(spanIds(batch), contains("root"));
    }

    @Test
    void test_when_genAiRootHoldIsZero_then_genAiRootsAreEmittedImmediately() {
        genAiRootHold = Duration.ZERO;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(spanIds(execute(processor, agentRoot())), contains("root"));
    }

    @Test
    void test_when_shuttingDown_then_heldRootsAreFlushedCountedAndCountAgainstReadiness() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, agentRoot());
        assertThat(processor.isReadyForShutdown(), is(false));

        final List<Span> flushed = releaseByShutdown(processor);

        assertThat(spanIds(flushed), contains("root"));
        assertThat(processor.isReadyForShutdown(), is(true));
        verify(counter(OTelTraceRawProcessor.GENAI_HELD_ROOT_SHUTDOWN_COUNT_METRIC_NAME)).increment();
        verify(counter(OTelTraceRawProcessor.GENAI_HELD_ROOT_EXPIRED_COUNT_METRIC_NAME), never()).increment();
    }

    @Test
    void test_when_holdingARootWouldExceedTheCap_then_theRootHeldLongestIsWrittenOnceEnrichedFromTheState() {
        genAiRootHoldMaxSize = 2;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, agentRoot("trace-1")), is(empty()));
        execute(processor, span("trace-1", "chat", "root", Map.of(
                "gen_ai.operation.name", "chat", "gen_ai.request.model", "gpt-4o", "gen_ai.usage.input_tokens", 3L)));
        assertThat(execute(processor, agentRoot("trace-2")), is(empty()));
        final List<Span> thirdRootBatch = execute(processor, agentRoot("trace-3"));

        assertThat(thirdRootBatch.size(), equalTo(1));
        final Span evicted = thirdRootBatch.get(0);
        assertThat(evicted.getTraceId(), equalTo("trace-1"));
        assertThat(evicted.getAttributes().get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(evicted.getAttributes(), "gen_ai.usage.input_tokens"), equalTo(3L));
        assertThat(gauge(OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(2.0));
        verify(counter(OTelTraceRawProcessor.GENAI_HELD_ROOT_EVICTED_COUNT_METRIC_NAME)).increment();

        final List<Span> flushed = releaseByShutdown(processor);
        assertThat(flushed.stream().map(Span::getTraceId).sorted().collect(Collectors.toList()),
                equalTo(List.of("trace-2", "trace-3")));
        verify(counter(OTelTraceRawProcessor.GENAI_HELD_ROOT_SHUTDOWN_COUNT_METRIC_NAME), times(2)).increment();
    }

    @Test
    void test_when_aSecondRootOfTheSameTraceIsHeld_then_theFirstIsWrittenAndCountedAsReplaced() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        execute(processor, agentRoot());
        final List<Span> secondRootBatch = execute(processor, span("root-2", "", Map.of("gen_ai.operation.name", "invoke_agent")));

        assertThat(spanIds(secondRootBatch), contains("root"));
        assertThat(gauge(OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(1.0));
        verify(counter(OTelTraceRawProcessor.GENAI_HELD_ROOT_REPLACED_COUNT_METRIC_NAME)).increment();
        assertThat(spanIds(releaseByShutdown(processor)), contains("root-2"));
    }

    @Test
    void test_when_childrenAreRecorded_then_theRootEnrichmentGaugesReportTracesAndSpanIds() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(gauge(OTelTraceRawProcessor.GENAI_ROOT_ENRICHMENT_COUNT_METRIC_NAME), equalTo(0.0));
        execute(processor, agentRoot());
        execute(processor, span("tool", "root", Map.of("gen_ai.operation.name", "execute_tool")));
        execute(processor, chatSpan("chat", "tool", null, 10L, 4L));

        assertThat(gauge(OTelTraceRawProcessor.GENAI_ROOT_ENRICHMENT_COUNT_METRIC_NAME), equalTo(1.0));
        assertThat(gauge(OTelTraceRawProcessor.GENAI_ROOT_ENRICHMENT_SPAN_ID_COUNT_METRIC_NAME), greaterThan(0.0));
    }

    @Test
    void test_when_markingIsOutermost_then_aChildWrittenWhileItsRootIsHeldIsNotMarkedAndTheRootIs() {
        genAiRootMarking = GenAiRootMarking.OUTERMOST;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, agentRoot()), is(empty()));
        final List<Span> toolBatch = execute(processor, span("tool", "root", Map.of("gen_ai.operation.name", "execute_tool")));
        final List<Span> chatBatch = execute(processor, chatSpan("chat", "tool", "gpt-4o", 150L, 85L));
        final Map<String, Object> root = byId(releaseByShutdown(processor), "root").getAttributes();

        assertThat(byId(toolBatch, "tool").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), nullValue());
        assertThat(byId(chatBatch, "chat").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), nullValue());
        assertThat(root.get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(150L));
    }

    @Test
    void test_when_markingIsNone_then_aHeldRootIsEnrichedAndNothingIsMarked() {
        genAiRootMarking = GenAiRootMarking.NONE;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, agentRoot()), is(empty()));
        final List<Span> written = new ArrayList<>(execute(processor, chatSpan("chat", "root", "gpt-4o", 150L, 85L)));
        written.addAll(releaseByShutdown(processor));

        final Map<String, Object> root = byId(written, "root").getAttributes();
        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(150L));
        assertThat(written.stream().filter(span -> span.getAttributes().containsKey(DERIVED_GEN_AI_IS_ROOT_KEY)).count(),
                equalTo(0L));
    }

    @Test
    void test_when_markingIsAgentSpansAndTheRootIsReleasedIntoTheBatchOfANestedAgent_then_onlyTheRootGetsTheTotals() {
        genAiRootMarking = GenAiRootMarking.AGENT_SPANS;
        genAiRootHold = Duration.ofMillis(200);
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, agentRoot()), is(empty()));
        waitAtLeast(300);
        // The hold expired: the root joins the batch of its nested agent.
        final List<Span> batch = execute(processor,
                span("nested", "root", Map.of("gen_ai.operation.name", "invoke_agent")),
                chatSpan("chat", "nested", "gpt-4o", 150L, 85L));

        final Map<String, Object> root = byId(batch, "root").getAttributes();
        assertThat(root.get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(150L));
        final Map<String, Object> nested = byId(batch, "nested").getAttributes();
        assertThat(nested.get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
        assertThat(nested.get("gen_ai.usage.input_tokens"), nullValue());
    }

    @Test
    void test_when_markingIsAgentSpansAndANestedAgentIsWrittenWhileItsRootIsHeld_then_bothCarryTheNestedUsage() {
        // Documented: with agent_spans and a propagation that enriches the root, summing marked spans can
        // count a nested agent's usage twice when that agent was written while its root was held.
        genAiRootMarking = GenAiRootMarking.AGENT_SPANS;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, agentRoot()), is(empty()));
        final List<Span> nestedBatch = execute(processor,
                span("nested", "root", Map.of("gen_ai.operation.name", "invoke_agent")),
                chatSpan("chat2", "nested", null, 5L, 3L));
        execute(processor, chatSpan("chat1", "root", "gpt-4o", 10L, 4L));
        final Map<String, Object> root = byId(releaseByShutdown(processor), "root").getAttributes();

        assertThat(spanIds(nestedBatch), containsInAnyOrder("nested", "chat2"));
        assertThat(tokens(byId(nestedBatch, "nested").getAttributes(), "gen_ai.usage.input_tokens"), equalTo(5L));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(15L));
        assertThat(tokens(root, "gen_ai.usage.output_tokens"), equalTo(7L));
    }

    @ParameterizedTest
    @CsvSource({
            "openinference.span.kind, AGENT",
            "openinference.span.kind, CHAIN",
            "traceloop.span.kind, workflow",
            "traceloop.span.kind, agent"
    })
    void test_when_aVendorRootArrivesBeforeItsChildren_then_itIsHeldAndFilledAtTheDeadline(final String key, final String kind) {
        genAiRootHold = Duration.ofMillis(200);
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        assertThat(execute(processor, span("root", "", Map.of(key, kind))), is(empty()));
        assertThat(spanIds(execute(processor, vendorLlmSpan("llm", "root"))), contains("llm"));
        final List<Span> released = executeUntilEmitted(processor);

        assertThat(spanIds(released), contains("root"));
        final Map<String, Object> root = released.get(0).getAttributes();
        assertThat(root.get("gen_ai.operation.name"), equalTo("invoke_agent"));
        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(tokens(root, "gen_ai.usage.input_tokens"), equalTo(150L));
        assertThat(tokens(root, "gen_ai.usage.output_tokens"), equalTo(85L));
        verify(counter(OTelTraceRawProcessor.GENAI_HELD_ROOT_EXPIRED_COUNT_METRIC_NAME)).increment();
    }

    @Test
    void test_when_aVendorRootCarriesItsModelAndTokensOnlyUnderVendorKeys_then_itIsNotHeld() {
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        final List<Span> batch = execute(processor, span("root", "", Map.of(
                "openinference.span.kind", "AGENT",
                "llm.model_name", "gpt-4o",
                "llm.token_count.prompt", 12L)));

        assertThat(spanIds(batch), contains("root"));
        assertThat(batch.get(0).getAttributes().get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(gauge(OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(0.0));
    }

    @Test
    void test_when_genAiRootHoldIsTheDefault_then_aVendorRootIsWrittenImmediatelyMarkedAndALaterChildIsNot() {
        genAiRootHold = OtelTraceRawProcessorConfig.DEFAULT_GENAI_ROOT_HOLD;
        genAiRootMarking = GenAiRootMarking.OUTERMOST;
        final OTelTraceRawProcessor processor = createObjectUnderTest();

        final List<Span> rootBatch = execute(processor, span("root", "", Map.of("openinference.span.kind", "AGENT")));
        final List<Span> childBatch = execute(processor, span("agent", "missing-parent", Map.of("openinference.span.kind", "AGENT")));

        assertThat(spanIds(rootBatch), contains("root"));
        assertThat(rootBatch.get(0).getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
        assertThat(rootBatch.get(0).getAttributes().get("gen_ai.request.model"), nullValue());
        assertThat(childBatch.get(0).getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), nullValue());
    }

    private OTelTraceRawProcessor createObjectUnderTest() {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        when(config.isGenAiEnrichmentDisabled()).thenReturn(false);
        when(config.getGenAiRootPropagation()).thenReturn(genAiRootPropagation);
        when(config.getGenAiRootEnrichmentCacheMaxSize())
                .thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_ROOT_ENRICHMENT_CACHE_MAX_SIZE);
        when(config.getGenAiRootHold()).thenReturn(genAiRootHold);
        when(config.getGenAiRootHoldMaxSize()).thenReturn(genAiRootHoldMaxSize);
        when(config.getGenAiRootMarking()).thenReturn(genAiRootMarking);
        when(config.getGenAiCoverageCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE);
        return new OTelTraceRawProcessor(config, mock(PipelineDescription.class), pluginMetrics);
    }

    private static List<Span> execute(final OTelTraceRawProcessor processor, final Span... spans) {
        final List<Record<Span>> records = Arrays.stream(spans).map(Record::new).collect(Collectors.toList());
        return processor.doExecute(records).stream().map(Record::getData).collect(Collectors.toList());
    }

    private static List<Span> releaseByShutdown(final OTelTraceRawProcessor processor) {
        processor.prepareForShutdown();
        return execute(processor);
    }

    /** Runs empty batches until one writes something, as a pipeline worker does while a hold runs out. */
    private static List<Span> executeUntilEmitted(final OTelTraceRawProcessor processor) {
        final List<Span> emitted = new ArrayList<>();
        await().atMost(5, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS).until(() -> {
            emitted.addAll(execute(processor));
            return !emitted.isEmpty();
        });
        return emitted;
    }

    private static Span agentRoot() {
        return agentRoot(TRACE_ID);
    }

    private static Span agentRoot(final String traceId) {
        return span(traceId, "root", "", Map.of("gen_ai.operation.name", "invoke_agent"));
    }

    private static Span agentSpan(final String model, final long inputTokens, final long outputTokens) {
        final Map<String, Object> attributes = new HashMap<>(Map.of(
                "gen_ai.operation.name", "invoke_agent",
                "gen_ai.usage.input_tokens", inputTokens,
                "gen_ai.usage.output_tokens", outputTokens));
        if (model != null) {
            attributes.put("gen_ai.request.model", model);
        }
        return span("agent", "root", attributes);
    }

    private static Span chatSpan(final String spanId, final String parentSpanId, final String model,
                                 final long inputTokens, final long outputTokens) {
        final Map<String, Object> attributes = new HashMap<>(Map.of(
                "gen_ai.operation.name", "chat",
                "gen_ai.usage.input_tokens", inputTokens,
                "gen_ai.usage.output_tokens", outputTokens));
        if (model != null) {
            attributes.put("gen_ai.request.model", model);
        }
        return span(spanId, parentSpanId, attributes);
    }

    /** An OpenInference LLM span that reports its model and token usage only under vendor keys. */
    private static Span vendorLlmSpan(final String spanId, final String parentSpanId) {
        return span(spanId, parentSpanId, Map.of(
                "openinference.span.kind", "LLM",
                "llm.model_name", "gpt-4o",
                "llm.token_count.prompt", 150L,
                "llm.token_count.completion", 85L));
    }

    private static Span span(final String spanId, final String parentSpanId, final Map<String, Object> attributes) {
        return span(TRACE_ID, spanId, parentSpanId, attributes);
    }

    private static Span span(final String traceId, final String spanId, final String parentSpanId,
                             final Map<String, Object> attributes) {
        return JacksonSpan.builder()
                .withTraceId(traceId)
                .withSpanId(spanId)
                .withParentSpanId(parentSpanId)
                .withTraceState("")
                .withName(spanId)
                .withKind("INTERNAL")
                .withDurationInNanos(1L)
                .withStartTime("2026-01-01T00:00:00Z")
                .withEndTime("2026-01-01T00:01:00Z")
                .withTraceGroup(null)
                .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                .withAttributes(new HashMap<>(attributes))
                .build();
    }

    private static long tokens(final Map<String, Object> attributes, final String key) {
        return ((Number) attributes.get(key)).longValue();
    }

    private static List<String> spanIds(final Collection<Span> spans) {
        return spans.stream().map(Span::getSpanId).collect(Collectors.toList());
    }

    private static Span byId(final Collection<Span> spans, final String spanId) {
        return spans.stream().filter(span -> spanId.equals(span.getSpanId())).findFirst().orElseThrow();
    }

    private Counter counter(final String name) {
        return counters.get(name);
    }

    @SuppressWarnings("unchecked")
    private double gauge(final String name) {
        final ArgumentCaptor<Object> measured = ArgumentCaptor.forClass(Object.class);
        final ArgumentCaptor<ToDoubleFunction> function = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics, atLeastOnce()).gauge(eq(name), measured.capture(), function.capture());
        return function.getValue().applyAsDouble(measured.getValue());
    }

    private static void waitAtLeast(final long delayMillis) {
        final long startMillis = System.currentTimeMillis();
        await().atMost(delayMillis + 5_000, TimeUnit.MILLISECONDS)
                .until(() -> System.currentTimeMillis() - startMillis >= delayMillis);
    }
}
