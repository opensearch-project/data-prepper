/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CircuitBreakerDecoratingHttpServiceTest {

    @Mock
    private CircuitBreaker circuitBreaker;

    @Test
    void newDecorator_throws_if_circuitBreaker_is_null() {
        assertThrows(NullPointerException.class, () -> CircuitBreakerDecoratingHttpService.newDecorator(null));
    }

    @Test
    void newDecorator_returns_non_null_function() {
        final Function<? super HttpService, ? extends HttpService> decorator =
                CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker);
        assertThat(decorator, notNullValue());
    }

    @Test
    void serve_returns_503_when_circuit_breaker_is_open() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(true);

        final AggregatedHttpResponse response = serveRequest();

        assertThat(response.status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        verify(circuitBreaker).isOpen();
    }

    @Test
    void serve_delegates_to_inner_service_when_circuit_breaker_is_closed() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(false);

        final AggregatedHttpResponse response = serveRequest();

        // inner service returns 200
        assertThat(response.status(), equalTo(HttpStatus.OK));
        verify(circuitBreaker).isOpen();
    }

    private AggregatedHttpResponse serveRequest() throws Exception {
        // Build a minimal Armeria service chain using the decorator
        final HttpService innerService = (ctx, req) ->
                com.linecorp.armeria.common.HttpResponse.of(HttpStatus.OK);

        final Function<? super HttpService, ? extends HttpService> decorator =
                CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker);
        final HttpService decorated = decorator.apply(innerService);

        final HttpRequest req = HttpRequest.of(HttpMethod.POST, "/");
        final ServiceRequestContext ctx = ServiceRequestContext.of(req);

        return decorated.serve(ctx, req).aggregate().join();
    }
}

