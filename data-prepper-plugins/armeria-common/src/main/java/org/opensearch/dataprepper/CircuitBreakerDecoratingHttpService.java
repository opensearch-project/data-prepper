/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.SimpleDecoratingHttpService;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import java.util.Objects;
import java.util.function.Function;

/**
 * An Armeria HTTP service decorator that rejects incoming requests with HTTP 503
 * (Service Unavailable) when the circuit breaker is open, before any request body
 * is read, decompressed, or parsed.
 *
 * This should be registered as the outermost server-level decorator so it runs
 * before authentication, decompression, and gRPC / HTTP handler logic. gRPC clients
 * receive the HTTP 503 as gRPC status UNAVAILABLE and will retry with back-off.
 *
 * Usage:
 * <pre>{@code
 * serverBuilder.decorator(CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker));
 * }</pre>
 *
 *
 * @since 2.13
 */
public final class CircuitBreakerDecoratingHttpService extends SimpleDecoratingHttpService {

    private final CircuitBreaker circuitBreaker;

    private CircuitBreakerDecoratingHttpService(final HttpService delegate, final CircuitBreaker circuitBreaker) {
        super(delegate);
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
    }

    /**
     * Creates a decorator {@link Function} that wraps a service with circuit-breaker
     * protection.
     *
     * @param circuitBreaker the circuit breaker to consult on every request
     * @return a decorator function suitable for {@link com.linecorp.armeria.server.ServerBuilder#decorator}
     */
    public static Function<? super HttpService, ? extends HttpService> newDecorator(
            final CircuitBreaker circuitBreaker) {
        Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
        return service -> new CircuitBreakerDecoratingHttpService(service, circuitBreaker);
    }

    @Override
    public HttpResponse serve(final ServiceRequestContext ctx, final HttpRequest req) throws Exception {
        if (circuitBreaker.isOpen()) {
            return HttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
        }
        return unwrap().serve(ctx, req);
    }
}

