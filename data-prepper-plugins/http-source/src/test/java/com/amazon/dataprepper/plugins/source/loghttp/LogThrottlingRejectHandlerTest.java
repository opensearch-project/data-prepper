/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Service;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class LogThrottlingRejectHandlerTest {
    private static final int TEST_MAX_PENDING_REQUEST = 1;

    @Mock
    private Service<HttpRequest, HttpResponse> service;

    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Mock
    private HttpRequest httpRequest;

    private LogThrottlingRejectHandler objectUnderTest;

    @Test
    public void testHandleRejected() throws Exception {
        // Prepare
        objectUnderTest = new LogThrottlingRejectHandler(TEST_MAX_PENDING_REQUEST);

        // When
        HttpResponse httpResponse = objectUnderTest.handleRejected(service, serviceRequestContext, httpRequest, new Throwable());
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        // Then
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, aggregatedHttpResponse.status());
        assertTrue(aggregatedHttpResponse.contentUtf8().contains(String.format("max_pending_requests:%d", TEST_MAX_PENDING_REQUEST)));
    }
}