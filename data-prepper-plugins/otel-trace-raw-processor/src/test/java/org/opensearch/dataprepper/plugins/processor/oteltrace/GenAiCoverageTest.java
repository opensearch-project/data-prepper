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

class GenAiCoverageTest {

    @Test
    void update_mergesTheReturnedSpanIdsWithTheKnownOnes() {
        final GenAiCoverage coverage = GenAiCoverage.create(100, Duration.ofMinutes(1));

        coverage.update("trace", known -> Set.of("a"));
        coverage.update("trace", known -> {
            assertThat(known, equalTo(Set.of("a")));
            return Set.of("b");
        });

        assertThat(coverage.covered("trace"), equalTo(Set.of("a", "b")));
        assertThat(coverage.covered("other"), is(empty()));
    }

    @Test
    void update_onNone_runsWithNothingKnownAndKeepsNothing() {
        GenAiCoverage.NONE.update("trace", known -> {
            assertThat(known, is(empty()));
            return Set.of("a");
        });

        assertThat(GenAiCoverage.NONE.covered("trace"), is(empty()));
        assertThat(GenAiCoverage.NONE.estimatedSize(), equalTo(0L));
    }

    @Test
    void update_forOneTrace_runsOneEnrichmentAtATime() throws Exception {
        final GenAiCoverage coverage = GenAiCoverage.create(100, Duration.ofMinutes(1));
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final AtomicBoolean secondRanEarly = new AtomicBoolean(false);
        final AtomicBoolean firstDone = new AtomicBoolean(false);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<?> first = executor.submit(() -> coverage.update("trace", known -> {
                firstStarted.countDown();
                try {
                    releaseFirst.await(5, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                firstDone.set(true);
                return Set.of("agent");
            }));
            firstStarted.await(5, TimeUnit.SECONDS);
            final Future<?> second = executor.submit(() -> coverage.update("trace", known -> {
                secondRanEarly.set(!firstDone.get());
                assertThat(known, equalTo(Set.of("agent")));
                return Set.of("child");
            }));
            Thread.sleep(200);
            releaseFirst.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(secondRanEarly.get(), is(false));
        assertThat(coverage.covered("trace"), equalTo(Set.of("agent", "child")));
    }

    @Test
    void spanIdCount_countsSpanIdsAcrossTraces_andEstimatedSizeCountsTraces() {
        final GenAiCoverage coverage = GenAiCoverage.create(100, Duration.ofMinutes(1));

        coverage.update("trace1", known -> Set.of("a", "b", "c"));
        coverage.update("trace2", known -> Set.of("d"));
        coverage.update("trace1", known -> Set.of("e"));

        assertThat(coverage.estimatedSize(), equalTo(2L));
        assertThat(coverage.spanIdCount(), equalTo(5L));
    }

    @Test
    void spanIdCount_onNone_isZero() {
        assertThat(GenAiCoverage.NONE.spanIdCount(), equalTo(0L));
    }

    @Test
    void update_withNoNewSpanIds_keepsTheEntryUnchanged() {
        final GenAiCoverage coverage = GenAiCoverage.create(100, Duration.ofMinutes(1));

        coverage.update("trace", known -> Set.of("a", "b"));
        coverage.update("trace", known -> Set.of("a"));
        coverage.update("other", known -> Set.of());

        assertThat(coverage.covered("trace"), equalTo(Set.of("a", "b")));
        assertThat(coverage.covered("other"), is(empty()));
        assertThat(coverage.spanIdCount(), equalTo(2L));
    }

    @Test
    void update_withANullTraceId_runsWithNothingKnownAndKeepsNothing() {
        final GenAiCoverage coverage = GenAiCoverage.create(100, Duration.ofMinutes(1));

        coverage.update(null, known -> {
            assertThat(known, is(empty()));
            return Set.of("a");
        });

        assertThat(coverage.covered(null), is(empty()));
        assertThat(coverage.estimatedSize(), equalTo(0L));
    }
}
