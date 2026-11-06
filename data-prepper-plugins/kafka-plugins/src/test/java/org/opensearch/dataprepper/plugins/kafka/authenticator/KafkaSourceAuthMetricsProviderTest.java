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

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

class KafkaSourceAuthMetricsProviderTest {

    @Test
    void getInstance_returnsSameSingleton() {
        assertThat(KafkaSourceAuthMetricsProvider.getInstance(),
                sameInstance(KafkaSourceAuthMetricsProvider.getInstance()));
    }

    @Test
    void get_beforeSet_isNull() {
        // fresh instance via the protected test ctor to avoid cross-test static leakage
        final KafkaSourceAuthMetricsProvider provider = new KafkaSourceAuthMetricsProvider() {};
        assertThat(provider.getAuthMetrics(), nullValue());
    }

    @Test
    void set_thenGet_returnsSuppliedInstance() {
        final KafkaSourceAuthMetricsProvider provider = new KafkaSourceAuthMetricsProvider() {};
        final KafkaSourceAuthMetrics metrics = mock(KafkaSourceAuthMetrics.class);
        provider.set(metrics);
        assertThat(provider.getAuthMetrics(), sameInstance(metrics));
    }

    @Test
    void set_overwritesPrevious_lastWriterWins() {
        final KafkaSourceAuthMetricsProvider provider = new KafkaSourceAuthMetricsProvider() {};
        final KafkaSourceAuthMetrics first = mock(KafkaSourceAuthMetrics.class);
        final KafkaSourceAuthMetrics second = mock(KafkaSourceAuthMetrics.class);
        provider.set(first);
        provider.set(second);
        assertThat(provider.getAuthMetrics(), sameInstance(second));
    }
}
