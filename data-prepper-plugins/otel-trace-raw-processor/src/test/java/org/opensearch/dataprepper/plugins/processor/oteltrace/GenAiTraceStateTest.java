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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class GenAiTraceStateTest {

    @Test
    void update_accumulatesCoveredSpanIdsAcrossUpdates() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update("trace", state -> state.cover("a"));
        traceState.update("trace", state -> {
            assertThat(state.coveredSpanIds(), equalTo(Set.of("a")));
            state.cover("b");
        });

        assertThat(traceState.covered("trace"), equalTo(Set.of("a", "b")));
        assertThat(traceState.covered("other"), is(empty()));
    }

    @Test
    void update_onNone_runsWithAnEmptyStateAndKeepsNothing() {
        GenAiTraceState.NONE.update("trace", state -> {
            assertThat(state.coveredSpanIds(), is(empty()));
            state.cover("a");
        });

        assertThat(GenAiTraceState.NONE.covered("trace"), is(empty()));
        assertThat(GenAiTraceState.NONE.estimatedSize(), equalTo(0L));
    }

    @Test
    void update_forOneTrace_runsOneEnrichmentAtATime() throws Exception {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final AtomicBoolean secondRanEarly = new AtomicBoolean(false);
        final AtomicBoolean firstDone = new AtomicBoolean(false);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<?> first = executor.submit(() -> traceState.update("trace", state -> {
                firstStarted.countDown();
                try {
                    releaseFirst.await(5, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                firstDone.set(true);
                state.cover("agent");
            }));
            firstStarted.await(5, TimeUnit.SECONDS);
            final Future<?> second = executor.submit(() -> traceState.update("trace", state -> {
                secondRanEarly.set(!firstDone.get());
                assertThat(state.coveredSpanIds(), equalTo(Set.of("agent")));
                state.cover("child");
            }));
            Thread.sleep(200);
            releaseFirst.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(secondRanEarly.get(), is(false));
        assertThat(traceState.covered("trace"), equalTo(Set.of("agent", "child")));
    }

    @Test
    void spanIdCount_countsSpanIdsAcrossTraces_andEstimatedSizeCountsTraces() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update("trace1", state -> {
            state.cover("a");
            state.cover("b");
            state.cover("c");
        });
        traceState.update("trace2", state -> state.cover("d"));
        traceState.update("trace1", state -> state.cover("e"));

        assertThat(traceState.estimatedSize(), equalTo(2L));
        assertThat(traceState.spanIdCount(), equalTo(5L));
    }

    @Test
    void spanIdCount_countsRecordedParentLinksToo() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update("trace", state -> {
            state.cover("a");
            state.recordParentLink("a", "root");
            state.recordParentLink("b", "a");
        });

        assertThat(traceState.spanIdCount(), equalTo(3L));
        assertThat(traceState.query("trace", state -> state.parentSpanId("b")), equalTo("a"));
        assertThat(traceState.query("trace", state -> state.parentSpanId("unknown")), nullValue());
    }

    @Test
    void updateIfPresent_onAnUnknownTrace_createsNoEntry_andOnAKnownTraceRecords() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.updateIfPresent("trace", state -> state.recordParentLink("b", "a"));
        assertThat(traceState.estimatedSize(), equalTo(0L));

        traceState.update("trace", state -> state.cover("a"));
        traceState.updateIfPresent("trace", state -> state.recordParentLink("b", "a"));
        assertThat(traceState.query("trace", state -> state.parentSpanId("b")), equalTo("a"));
    }

    @Test
    void recordGenAiRoot_isKeptAcrossUpdates_andAloneKeepsAnEntry() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        assertThat(traceState.query("trace", GenAiTraceState.TraceState::isGenAiRootSeen), is(false));
        traceState.update("trace", GenAiTraceState.TraceState::recordGenAiRoot);
        traceState.update("trace", state -> state.cover("a"));

        assertThat(traceState.query("trace", GenAiTraceState.TraceState::isGenAiRootSeen), is(true));
    }

    @Test
    void spanIdCount_onNone_isZero() {
        assertThat(GenAiTraceState.NONE.spanIdCount(), equalTo(0L));
    }

    @Test
    void update_thatRecordsNothing_keepsNoEntry() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update("trace", state -> { });

        assertThat(traceState.covered("trace"), is(empty()));
        assertThat(traceState.estimatedSize(), equalTo(0L));
    }

    @Test
    void update_withANullTraceId_runsWithAnEmptyStateAndKeepsNothing() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update(null, state -> {
            assertThat(state.coveredSpanIds(), is(empty()));
            state.cover("a");
        });

        assertThat(traceState.covered(null), is(empty()));
        assertThat(traceState.estimatedSize(), equalTo(0L));
    }

    @Test
    void query_onAnUnknownTrace_readsAnEmptyState() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        assertThat(traceState.query("trace", state -> state.hasTokenTotals()), is(false));
        assertThat(traceState.query("trace", state -> state.propagatedValue("gen_ai.request.model")), nullValue());
    }

    @Test
    void recordPropagatedValue_firstValueWins_acrossUpdates() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update("trace", state -> {
            state.cover("a");
            state.recordPropagatedValue("gen_ai.request.model", "gpt-4o");
        });
        traceState.update("trace", state -> state.recordPropagatedValue("gen_ai.request.model", "other-model"));

        assertThat(traceState.query("trace", state -> state.propagatedValue("gen_ai.request.model")), equalTo("gpt-4o"));
    }

    @Test
    void recordTokenUsage_sumsSpansWithoutATokenReportingAncestorOrDescendant() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update("trace", state -> {
            state.recordParentLink("agent", "root");
            state.recordParentLink("chat1", "agent");
            state.recordTokenUsage("agent", 15, 7);
        });
        traceState.update("trace", state -> {
            // The agent's total already contains its own chat call: not counted again.
            state.recordTokenUsage("chat1", 10, 4);
        });
        traceState.update("trace", state -> {
            // A second agent with its own usage is counted.
            state.recordParentLink("agent2", "root");
            state.recordTokenUsage("agent2", 5, 3);
        });

        assertThat(traceState.query("trace", state -> state.hasTokenTotals()), is(true));
        assertThat(traceState.query("trace", state -> state.inputTokenTotal()), equalTo(20L));
        assertThat(traceState.query("trace", state -> state.outputTokenTotal()), equalTo(10L));
    }

    @Test
    void recordTokenUsage_skipsAnAncestorOfASpanThatAlreadyReported() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update("trace", state -> {
            state.recordParentLink("chat1", "agent");
            state.recordTokenUsage("chat1", 10, 4);
        });
        traceState.update("trace", state -> {
            // The agent arrives after the call it contains: its total would count the call twice.
            state.recordTokenUsage("agent", 15, 7);
        });

        assertThat(traceState.query("trace", state -> state.inputTokenTotal()), equalTo(10L));
        assertThat(traceState.query("trace", state -> state.outputTokenTotal()), equalTo(4L));
    }

    @Test
    void recordTokenUsage_countsASpanOnlyOnce() {
        final GenAiTraceState traceState = GenAiTraceState.create(100, Duration.ofMinutes(1));

        traceState.update("trace", state -> state.recordTokenUsage("chat1", 10, 4));
        traceState.update("trace", state -> state.recordTokenUsage("chat1", 10, 4));

        assertThat(traceState.query("trace", state -> state.inputTokenTotal()), equalTo(10L));
        assertThat(traceState.query("trace", state -> state.outputTokenTotal()), equalTo(4L));
    }
}
