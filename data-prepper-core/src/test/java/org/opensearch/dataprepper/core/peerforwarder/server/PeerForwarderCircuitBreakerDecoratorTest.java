/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwarderCircuitBreakerDecoratorTest {

    @Mock
    private CircuitBreaker circuitBreaker;

    private AtomicInteger innerServiceCalls;

    @Test
    void newDecorator_throws_if_circuitBreaker_is_null() {
        assertThrows(NullPointerException.class, () -> PeerForwarderCircuitBreakerDecorator.newDecorator(null));
    }

    @Test
    void serve_returns_429_with_retry_after_without_calling_the_service_when_circuit_breaker_is_open() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(true);

        final AggregatedHttpResponse response = serveRequest();

        assertThat(response.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo(PeerForwarderCircuitBreakerDecorator.RETRY_AFTER_SECONDS));
        assertThat(response.contentUtf8(), equalTo(PeerForwarderCircuitBreakerDecorator.REJECTION_MESSAGE));
        assertThat(innerServiceCalls.get(), equalTo(0));
    }

    @Test
    void serve_delegates_to_the_service_when_circuit_breaker_is_closed() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(false);

        final AggregatedHttpResponse response = serveRequest();

        assertThat(response.status(), equalTo(HttpStatus.OK));
        assertThat(innerServiceCalls.get(), equalTo(1));
    }

    private AggregatedHttpResponse serveRequest() throws Exception {
        innerServiceCalls = new AtomicInteger();
        final HttpService innerService = (ctx, request) -> {
            innerServiceCalls.incrementAndGet();
            return HttpResponse.of(HttpStatus.OK);
        };
        final HttpService decorated = PeerForwarderCircuitBreakerDecorator.newDecorator(circuitBreaker).apply(innerService);
        final HttpRequest request = HttpRequest.of(HttpMethod.POST, "/");

        return decorated.serve(ServiceRequestContext.of(request), request).aggregate().join();
    }
}
