/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.SimpleDecoratingHttpService;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

/**
 * An Armeria HTTP service decorator for the peer forwarder server that rejects forwarded batches
 * with HTTP 429 (Too Many Requests) and a {@code Retry-After} header while the circuit breaker is
 * open, before the request body is read or parsed.
 * <p>
 * Register it as a server-level decorator so it runs before the peer forwarder service.
 */
class PeerForwarderCircuitBreakerDecorator extends SimpleDecoratingHttpService {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderCircuitBreakerDecorator.class);

    static final String REJECTION_MESSAGE = "Circuit breaker is open. Request rejected before reading the body.";
    static final String RETRY_AFTER_SECONDS = "1";

    private final CircuitBreaker circuitBreaker;

    private PeerForwarderCircuitBreakerDecorator(final HttpService delegate,
                                                final CircuitBreaker circuitBreaker) {
        super(delegate);
        this.circuitBreaker = circuitBreaker;
    }

    static Function<? super HttpService, ? extends HttpService> newDecorator(final CircuitBreaker circuitBreaker) {
        Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
        return service -> new PeerForwarderCircuitBreakerDecorator(service, circuitBreaker);
    }

    @Override
    public HttpResponse serve(final ServiceRequestContext ctx, final HttpRequest req) throws Exception {
        if (circuitBreaker.isOpen()) {
            LOG.debug("Rejecting peer forwarder request: circuit breaker is open.");
            final ResponseHeaders headers = ResponseHeaders.builder(HttpStatus.TOO_MANY_REQUESTS)
                    .contentType(MediaType.PLAIN_TEXT_UTF_8)
                    .add(HttpHeaderNames.RETRY_AFTER, RETRY_AFTER_SECONDS)
                    .build();
            return HttpResponse.of(headers, HttpData.ofUtf8(REJECTION_MESSAGE));
        }
        return unwrap().serve(ctx, req);
    }
}
