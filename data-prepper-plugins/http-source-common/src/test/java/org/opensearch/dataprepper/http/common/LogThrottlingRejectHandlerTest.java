/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http.common;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Service;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogThrottlingRejectHandlerTest {
    private static final int TEST_MAX_PENDING_REQUEST = 1;

    @Mock
    private Service<HttpRequest, HttpResponse> service;

    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Mock
    private HttpRequest httpRequest;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter rejectedRequestsCounter;

    private LogThrottlingRejectHandler objectUnderTest;

    @BeforeEach
    public void setUp() {
        when(pluginMetrics.counter(LogThrottlingRejectHandler.REQUESTS_REJECTED)).thenReturn(rejectedRequestsCounter);
    }

    @Test
    public void testHandleRejected() throws Exception {
        // Prepare
        objectUnderTest = new LogThrottlingRejectHandler(TEST_MAX_PENDING_REQUEST, pluginMetrics);

        // When
        HttpResponse httpResponse = objectUnderTest.handleRejected(service, serviceRequestContext, httpRequest, new Throwable());
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        // Then
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, aggregatedHttpResponse.status());
        assertTrue(aggregatedHttpResponse.contentUtf8().contains(String.format("max_pending_requests:%d", TEST_MAX_PENDING_REQUEST)));
        verify(rejectedRequestsCounter, times(1)).increment();
    }
}