/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper;

import com.google.rpc.RetryInfo;
import com.google.rpc.Status;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
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
    void serve_returns_429_with_retry_after_for_http_request_when_circuit_breaker_is_open() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(true);

        final AggregatedHttpResponse response = serveRequest();

        assertThat(response.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
        assertThat(response.contentUtf8(), equalTo(CircuitBreakerDecoratingHttpService.REJECTION_MESSAGE));
        verify(circuitBreaker).isOpen();
    }

    @ParameterizedTest
    @ValueSource(strings = {"application/grpc", "application/grpc+proto"})
    void serve_returns_resource_exhausted_with_retry_info_for_grpc_request_when_circuit_breaker_is_open(
            final String contentType) throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(true);
        final HttpRequest req = HttpRequest.of(RequestHeaders.builder(HttpMethod.POST, "/")
                .contentType(MediaType.parse(contentType))
                .build());

        final AggregatedHttpResponse response = serveRequest(
                CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker, Duration.ofSeconds(3), Duration.ofSeconds(5), () -> { }),
                req);

        assertThat(response.status(), equalTo(HttpStatus.OK));
        assertThat(response.headers().get("grpc-status"), equalTo("8"));
        assertThat(response.headers().get("grpc-message"), equalTo(CircuitBreakerDecoratingHttpService.REJECTION_MESSAGE));
        final Status status = Status.parseFrom(Base64.getDecoder().decode(response.headers().get("grpc-status-details-bin")));
        assertThat(status.getCode(), equalTo(8));
        assertThat(status.getDetails(0).unpack(RetryInfo.class).getRetryDelay().getSeconds(), equalTo(3L));
    }

    @Test
    void serve_uses_minimum_retry_delay_rounded_up_to_whole_seconds_for_retry_after() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(true);

        final AggregatedHttpResponse response = serveRequest(
                CircuitBreakerDecoratingHttpService.newDecorator(
                        circuitBreaker, Duration.ofMillis(2500), Duration.ofSeconds(10), () -> { }),
                HttpRequest.of(HttpMethod.POST, "/"));

        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("3"));
    }

    @Test
    void serve_delegates_to_inner_service_when_circuit_breaker_is_closed() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(false);

        final AggregatedHttpResponse response = serveRequest();

        // inner service returns 200
        assertThat(response.status(), equalTo(HttpStatus.OK));
        verify(circuitBreaker).isOpen();
    }

    @Test
    void newDecorator_throws_if_onRejected_is_null() {
        assertThrows(NullPointerException.class, () -> CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker, null));
    }

    @Test
    void serve_runs_onRejected_when_circuit_breaker_is_open() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(true);
        final AtomicInteger rejected = new AtomicInteger();

        final AggregatedHttpResponse response = serveRequest(
                CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker, rejected::incrementAndGet));

        assertThat(response.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(rejected.get(), equalTo(1));
    }

    @Test
    void serve_does_not_run_onRejected_when_circuit_breaker_is_closed() throws Exception {
        when(circuitBreaker.isOpen()).thenReturn(false);
        final AtomicInteger rejected = new AtomicInteger();

        final AggregatedHttpResponse response = serveRequest(
                CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker, rejected::incrementAndGet));

        assertThat(response.status(), equalTo(HttpStatus.OK));
        assertThat(rejected.get(), equalTo(0));
    }

    private AggregatedHttpResponse serveRequest() throws Exception {
        return serveRequest(CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker));
    }

    private AggregatedHttpResponse serveRequest(final Function<? super HttpService, ? extends HttpService> decorator) throws Exception {
        return serveRequest(decorator, HttpRequest.of(HttpMethod.POST, "/"));
    }

    private AggregatedHttpResponse serveRequest(final Function<? super HttpService, ? extends HttpService> decorator,
                                                final HttpRequest req) throws Exception {
        // Build a minimal Armeria service chain using the decorator
        final HttpService innerService = (ctx, request) ->
                com.linecorp.armeria.common.HttpResponse.of(HttpStatus.OK);

        final HttpService decorated = decorator.apply(innerService);

        final ServiceRequestContext ctx = ServiceRequestContext.of(req);

        return decorated.serve(ctx, req).aggregate().join();
    }
}

