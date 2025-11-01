/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusHttpSender;

import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class PrometheusSinkFlushContextTest {
    @Mock
    PrometheusHttpSender httpSender;

    private PrometheusSinkFlushContext prometheusSinkFlushContext;

    PrometheusSinkFlushContext createObjectUnderTest() {
        return new PrometheusSinkFlushContext(httpSender);
    }

    @Test
    public void testPrometheusSinkFlushContext() {
        httpSender = mock(PrometheusHttpSender.class);
        prometheusSinkFlushContext = createObjectUnderTest();
        assertThat(prometheusSinkFlushContext.getHttpSender(), sameInstance(httpSender));
    }
}
