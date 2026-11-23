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

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class GenAiRootEnrichmentStateTest {

    private static final String MODEL_KEY = "gen_ai.request.model";

    private String traceId;

    @BeforeEach
    void setUp() {
        traceId = UUID.randomUUID().toString();
    }

    private static GenAiRootEnrichmentState createObjectUnderTest() {
        return GenAiRootEnrichmentState.create(100, Duration.ofMinutes(1));
    }

    @Test
    void update_accumulatesAcrossUpdates_andFirstValueWins() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();

        state.update(traceId, traceState -> traceState.recordPropagatedValue(MODEL_KEY, "gpt-4o"));
        state.update(traceId, traceState -> traceState.recordPropagatedValue(MODEL_KEY, "other-model"));

        assertThat(state.query(traceId, traceState -> traceState.propagatedValue(MODEL_KEY)), equalTo("gpt-4o"));
        assertThat(state.query(UUID.randomUUID().toString(), traceState -> traceState.propagatedValue(MODEL_KEY)), nullValue());
    }

    @Test
    void update_onNone_runsWithAnEmptyStateAndKeepsNothing() {
        GenAiRootEnrichmentState.NONE.update(traceId, traceState -> {
            assertThat(traceState.hasTokenTotals(), is(false));
            traceState.recordTokenUsage("chat", 10, 4);
            assertThat(traceState.hasTokenTotals(), is(false));
        });

        assertThat(GenAiRootEnrichmentState.NONE.query(traceId, GenAiRootEnrichmentState.TraceState::hasTokenTotals), is(false));
        assertThat(GenAiRootEnrichmentState.NONE.estimatedSize(), equalTo(0L));
        assertThat(GenAiRootEnrichmentState.NONE.spanIdCount(), equalTo(0L));
    }

    @Test
    void update_withANullTraceId_keepsNothing() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();

        state.update(null, traceState -> traceState.recordTokenUsage("chat", 10, 4));

        assertThat(state.estimatedSize(), equalTo(0L));
        assertThat(state.query(null, GenAiRootEnrichmentState.TraceState::hasTokenTotals), is(false));
    }

    @Test
    void update_thatRecordsNothing_keepsNoEntry() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();

        state.update(traceId, traceState -> { });

        assertThat(state.estimatedSize(), equalTo(0L));
    }

    @Test
    void updateIfPresent_onAnUnknownTrace_createsNoEntry_andOnAKnownTraceRecords() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();

        state.updateIfPresent(traceId, traceState -> traceState.recordParentLink("tool", "agent"));
        assertThat(state.estimatedSize(), equalTo(0L));

        state.update(traceId, traceState -> traceState.recordTokenUsage("chat", 10, 4));
        state.updateIfPresent(traceId, traceState -> traceState.recordParentLink("tool", "agent"));
        assertThat(state.spanIdCount(), equalTo(2L));
    }

    @Test
    void update_forOneTrace_runsOneUpdateAtATime() throws Exception {
        final GenAiRootEnrichmentState state = createObjectUnderTest();
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final AtomicBoolean secondRanEarly = new AtomicBoolean(false);
        final AtomicBoolean firstDone = new AtomicBoolean(false);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<?> first = executor.submit(() -> state.update(traceId, traceState -> {
                firstStarted.countDown();
                try {
                    releaseFirst.await(5, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                firstDone.set(true);
                traceState.recordTokenUsage("chat1", 10, 4);
            }));
            firstStarted.await(5, TimeUnit.SECONDS);
            final Future<?> second = executor.submit(() -> state.update(traceId, traceState -> {
                secondRanEarly.set(!firstDone.get());
                traceState.recordTokenUsage("chat2", 5, 3);
            }));
            releaseFirst.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(secondRanEarly.get(), is(false));
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::inputTokenTotal), equalTo(15L));
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::outputTokenTotal), equalTo(7L));
    }

    @Test
    void spanIdCount_countsParentLinksAndTokenReporters_andEstimatedSizeCountsTraces() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();
        final String otherTraceId = UUID.randomUUID().toString();

        state.update(traceId, traceState -> {
            traceState.recordParentLink("chat", "agent");
            traceState.recordTokenUsage("chat", 10, 4);
            traceState.recordParentLink("agent", "root");
        });
        state.update(otherTraceId, traceState -> traceState.recordParentLink("tool", "agent"));

        assertThat(state.estimatedSize(), equalTo(2L));
        assertThat(state.spanIdCount(), equalTo(4L));
    }

    @Test
    void tokenTotals_whenAnAgentArrivesBeforeItsCall_countTheAgentOnly() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();

        state.update(traceId, traceState -> {
            traceState.recordParentLink("agent", "root");
            traceState.recordTokenUsage("agent", 15, 7);
        });
        state.update(traceId, traceState -> {
            traceState.recordParentLink("chat", "agent");
            traceState.recordTokenUsage("chat", 10, 4);
        });

        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::inputTokenTotal), equalTo(15L));
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::outputTokenTotal), equalTo(7L));
    }

    @Test
    void tokenTotals_whenACallArrivesBeforeItsAgent_countTheAgentOnly() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();

        state.update(traceId, traceState -> {
            traceState.recordParentLink("chat", "agent");
            traceState.recordTokenUsage("chat", 10, 4);
        });
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::inputTokenTotal), equalTo(10L));

        state.update(traceId, traceState -> {
            traceState.recordParentLink("agent", "root");
            traceState.recordTokenUsage("agent", 15, 7);
        });

        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::inputTokenTotal), equalTo(15L));
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::outputTokenTotal), equalTo(7L));
    }

    @Test
    void tokenTotals_whenALaterLinkConnectsTwoReporters_countTheTopmostOnly() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();

        state.update(traceId, traceState -> {
            traceState.recordParentLink("chat", "tool");
            traceState.recordTokenUsage("chat", 10, 4);
            traceState.recordParentLink("agent", "root");
            traceState.recordTokenUsage("agent", 15, 7);
        });
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::inputTokenTotal), equalTo(25L));

        state.updateIfPresent(traceId, traceState -> traceState.recordParentLink("tool", "agent"));

        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::inputTokenTotal), equalTo(15L));
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::outputTokenTotal), equalTo(7L));
    }

    @Test
    void tokenTotals_countASpanOnlyOnce_andSiblingsBoth() {
        final GenAiRootEnrichmentState state = createObjectUnderTest();

        state.update(traceId, traceState -> traceState.recordTokenUsage("chat1", 10, 4));
        state.update(traceId, traceState -> traceState.recordTokenUsage("chat1", 10, 4));
        state.update(traceId, traceState -> traceState.recordTokenUsage("chat2", 5, 3));

        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::hasTokenTotals), is(true));
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::inputTokenTotal), equalTo(15L));
        assertThat(state.query(traceId, GenAiRootEnrichmentState.TraceState::outputTokenTotal), equalTo(7L));
    }
}
