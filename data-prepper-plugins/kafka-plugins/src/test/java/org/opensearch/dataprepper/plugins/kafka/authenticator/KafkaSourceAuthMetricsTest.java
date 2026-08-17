/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.authenticator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class KafkaSourceAuthMetricsTest {

    private String prefix;
    private SimpleMeterRegistry meterRegistry;
    private AtomicLong now;
    private KafkaSourceAuthMetrics authMetrics;

    @BeforeEach
    void setUp() {
        // PluginMetrics writes to the static Metrics.globalRegistry, whose meters outlive an
        // add/removeRegistry cycle. A globally-unique pipeline name (not just per-class-unique) avoids
        // cross-test meter-id collisions that otherwise make find() resolve a stale, removed meter.
        final String pipelineName = "test-pipeline-" + UUID.randomUUID();
        prefix = pipelineName + ".kafka.";
        meterRegistry = new SimpleMeterRegistry();
        Metrics.addRegistry(meterRegistry);
        // Controllable clock so the pull-model gauge is exercised deterministically (advance time
        // between refresh and read without sleeping).
        now = new AtomicLong(1_000_000_000_000L);
        authMetrics = new KafkaSourceAuthMetrics(PluginMetrics.fromNames("kafka", pipelineName), now::get);
    }

    @AfterEach
    void tearDown() {
        Metrics.removeRegistry(meterRegistry);
        meterRegistry.clear();
        meterRegistry.close();
    }

    private Gauge expiryGauge() {
        return meterRegistry.find(prefix + KafkaSourceAuthMetrics.TIME_TO_TOKEN_REFRESH).gauge();
    }

    @Test
    void recordRefresh_incrementsCountAndSetsExpiryHeadroom() {
        final long headroomSeconds = ThreadLocalRandom.current().nextInt(60, 7200);
        authMetrics.recordRefresh(now.get() + headroomSeconds * 1000L);

        final Counter counter = meterRegistry.find(prefix + KafkaSourceAuthMetrics.TOKEN_REFRESH_SUCCESS).counter();
        assertThat(counter, notNullValue());
        assertThat(counter.count(), equalTo(1.0));

        assertThat(expiryGauge(), notNullValue());
        assertThat(expiryGauge().value(), closeTo(headroomSeconds, 0.001));
    }

    @Test
    void timeToTokenRefresh_beforeAnyToken_isZero() {
        assertThat(expiryGauge(), notNullValue());
        assertThat(expiryGauge().value(), equalTo(0.0));
    }

    @Test
    void timeToTokenRefresh_isPullModel_countsDownAsClockAdvancesForSameToken() {
        // Guards against a snapshot-at-refresh regression: for a SINGLE refresh, the gauge value must
        // strictly decrease as the clock advances (it is computed at read time, not frozen at refresh).
        authMetrics.recordRefresh(now.get() + 3_600_000L);
        final double atMint = expiryGauge().value();

        now.addAndGet(600_000L); // 10 minutes pass, no new refresh
        final double tenMinutesLater = expiryGauge().value();

        assertThat(atMint, closeTo(3600.0, 0.001));
        assertThat(atMint, greaterThan(tenMinutesLater));
        assertThat(tenMinutesLater, closeTo(3000.0, 0.001));
    }

    @Test
    void timeToTokenRefresh_pastDeadline_flooredAtZero() {
        authMetrics.recordRefresh(now.get() - 10_000L);
        assertThat(expiryGauge().value(), equalTo(0.0));
    }

    @Test
    void recordFailure_incrementsCounterTaggedByCause() {
        authMetrics.recordFailure(KafkaSourceAuthMetrics.CAUSE_AWS_STS_ACCESS_DENIED);
        authMetrics.recordFailure(KafkaSourceAuthMetrics.CAUSE_AWS_STS_ACCESS_DENIED);
        authMetrics.recordFailure(KafkaSourceAuthMetrics.CAUSE_AZURE_TOKEN_EXCHANGE_REJECTED);

        final Counter accessDenied = meterRegistry.find(prefix + KafkaSourceAuthMetrics.TOKEN_REFRESH_FAILURES)
                .tag(KafkaSourceAuthMetrics.ERROR_TYPE_TAG, KafkaSourceAuthMetrics.CAUSE_AWS_STS_ACCESS_DENIED).counter();
        final Counter aadsts = meterRegistry.find(prefix + KafkaSourceAuthMetrics.TOKEN_REFRESH_FAILURES)
                .tag(KafkaSourceAuthMetrics.ERROR_TYPE_TAG, KafkaSourceAuthMetrics.CAUSE_AZURE_TOKEN_EXCHANGE_REJECTED).counter();

        assertThat(accessDenied, notNullValue());
        assertThat(accessDenied.count(), equalTo(2.0));
        assertThat(aadsts, notNullValue());
        assertThat(aadsts.count(), equalTo(1.0));
    }

    @Test
    void recordFailure_distinctCauses_produceDistinctCounters() {
        authMetrics.recordFailure(KafkaSourceAuthMetrics.CAUSE_NETWORK);
        final Counter network = meterRegistry.find(prefix + KafkaSourceAuthMetrics.TOKEN_REFRESH_FAILURES)
                .tag(KafkaSourceAuthMetrics.ERROR_TYPE_TAG, KafkaSourceAuthMetrics.CAUSE_NETWORK).counter();
        final Counter other = meterRegistry.find(prefix + KafkaSourceAuthMetrics.TOKEN_REFRESH_FAILURES)
                .tag(KafkaSourceAuthMetrics.ERROR_TYPE_TAG, KafkaSourceAuthMetrics.CAUSE_OTHER).counter();
        assertThat(network, notNullValue());
        assertThat(network.count(), equalTo(1.0));
        // a cause never recorded has no counter registered
        assertThat(other, nullValue());
    }
}
