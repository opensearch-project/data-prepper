/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper;

import com.google.protobuf.Any;
import com.google.rpc.RetryInfo;
import com.google.rpc.Status;
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

import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.function.Function;

import static org.opensearch.dataprepper.RetryInfoCalculator.DEFAULT_MAXIMUM_DELAY;
import static org.opensearch.dataprepper.RetryInfoCalculator.DEFAULT_MINIMUM_DELAY;

/**
 * An Armeria HTTP service decorator that rejects incoming requests while the circuit breaker
 * is open, before any request body is read, decompressed, or parsed.
 *
 * <p>gRPC requests receive a trailers-only response with gRPC status {@code RESOURCE_EXHAUSTED}
 * and a {@link RetryInfo} detail. All other requests receive HTTP 429 (Too Many Requests) with a
 * {@code Retry-After} header. Both are retryable for OTLP clients.
 *
 * <p>Register it as a server-level decorator so it runs before decompression and the gRPC / HTTP
 * handlers, which are attached to the individual services. Armeria runs server-level decorators in
 * reverse order of registration, so server-level decorators registered after this one, such as
 * HTTP authentication, run before it. They only read request headers.
 *
 * Usage:
 * <pre>{@code
 * serverBuilder.decorator(CircuitBreakerDecoratingHttpService.newDecorator(circuitBreaker));
 * }</pre>
 *
 * @since 2.17
 */
public final class CircuitBreakerDecoratingHttpService extends SimpleDecoratingHttpService {

    static final String REJECTION_MESSAGE = "Circuit breaker is open. Request rejected before reading the body.";

    private static final int GRPC_RESOURCE_EXHAUSTED = 8;
    private static final MediaType GRPC = MediaType.create("application", "grpc");

    private final CircuitBreaker circuitBreaker;
    private final RetryInfoCalculator retryInfoCalculator;
    private final Runnable onRejected;

    private CircuitBreakerDecoratingHttpService(final HttpService delegate,
                                                final CircuitBreaker circuitBreaker,
                                                final RetryInfoCalculator retryInfoCalculator,
                                                final Runnable onRejected) {
        super(delegate);
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
        this.retryInfoCalculator = Objects.requireNonNull(retryInfoCalculator, "retryInfoCalculator must not be null");
        this.onRejected = Objects.requireNonNull(onRejected, "onRejected must not be null");
    }

    /**
     * Creates a decorator {@link Function} that wraps a service with circuit-breaker
     * protection, using the default retry delays.
     *
     * @param circuitBreaker the circuit breaker to consult on every request
     * @return a decorator function suitable for {@link com.linecorp.armeria.server.ServerBuilder#decorator}
     */
    public static Function<? super HttpService, ? extends HttpService> newDecorator(
            final CircuitBreaker circuitBreaker) {
        return newDecorator(circuitBreaker, () -> { });
    }

    /**
     * Creates a decorator {@link Function} that wraps a service with circuit-breaker
     * protection, using the default retry delays, and runs {@code onRejected} for every
     * request it rejects.
     *
     * @param circuitBreaker the circuit breaker to consult on every request
     * @param onRejected called once for every rejected request, for example to log or count it
     * @return a decorator function suitable for {@link com.linecorp.armeria.server.ServerBuilder#decorator}
     */
    public static Function<? super HttpService, ? extends HttpService> newDecorator(
            final CircuitBreaker circuitBreaker, final Runnable onRejected) {
        return newDecorator(circuitBreaker, DEFAULT_MINIMUM_DELAY, DEFAULT_MAXIMUM_DELAY, onRejected);
    }

    /**
     * Creates a decorator {@link Function} that wraps a service with circuit-breaker
     * protection. The retry delay sent to clients starts at {@code minRetryDelay} and doubles
     * on consecutive rejections up to {@code maxRetryDelay}, like the gRPC exception handler.
     *
     * @param circuitBreaker the circuit breaker to consult on every request
     * @param minRetryDelay the smallest retry delay sent to clients
     * @param maxRetryDelay the largest retry delay sent to clients
     * @param onRejected called once for every rejected request, for example to log or count it
     * @return a decorator function suitable for {@link com.linecorp.armeria.server.ServerBuilder#decorator}
     */
    public static Function<? super HttpService, ? extends HttpService> newDecorator(
            final CircuitBreaker circuitBreaker,
            final Duration minRetryDelay,
            final Duration maxRetryDelay,
            final Runnable onRejected) {
        Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
        Objects.requireNonNull(minRetryDelay, "minRetryDelay must not be null");
        Objects.requireNonNull(maxRetryDelay, "maxRetryDelay must not be null");
        Objects.requireNonNull(onRejected, "onRejected must not be null");
        final RetryInfoCalculator retryInfoCalculator = new RetryInfoCalculator(minRetryDelay, maxRetryDelay);
        return service -> new CircuitBreakerDecoratingHttpService(service, circuitBreaker, retryInfoCalculator, onRejected);
    }

    @Override
    public HttpResponse serve(final ServiceRequestContext ctx, final HttpRequest req) throws Exception {
        if (circuitBreaker.isOpen()) {
            onRejected.run();
            final RetryInfo retryInfo = retryInfoCalculator.createRetryInfo();
            return isGrpc(req) ? grpcRejection(retryInfo) : httpRejection(retryInfo);
        }
        return unwrap().serve(ctx, req);
    }

    private static boolean isGrpc(final HttpRequest req) {
        final MediaType contentType = req.contentType();
        return contentType != null
                && "application".equals(contentType.type())
                && ("grpc".equals(contentType.subtype()) || contentType.subtype().startsWith("grpc+"));
    }

    private static HttpResponse grpcRejection(final RetryInfo retryInfo) {
        final Status status = Status.newBuilder()
                .setCode(GRPC_RESOURCE_EXHAUSTED)
                .setMessage(REJECTION_MESSAGE)
                .addDetails(Any.pack(retryInfo))
                .build();
        final ResponseHeaders headers = ResponseHeaders.builder(HttpStatus.OK)
                .contentType(GRPC)
                .add("grpc-status", String.valueOf(GRPC_RESOURCE_EXHAUSTED))
                .add("grpc-message", REJECTION_MESSAGE)
                .add("grpc-status-details-bin", Base64.getEncoder().withoutPadding().encodeToString(status.toByteArray()))
                .build();
        return HttpResponse.of(headers);
    }

    private static HttpResponse httpRejection(final RetryInfo retryInfo) {
        final ResponseHeaders headers = ResponseHeaders.builder(HttpStatus.TOO_MANY_REQUESTS)
                .contentType(MediaType.PLAIN_TEXT_UTF_8)
                .add(HttpHeaderNames.RETRY_AFTER, String.valueOf(retryAfterSeconds(retryInfo)))
                .build();
        return HttpResponse.of(headers, HttpData.ofUtf8(REJECTION_MESSAGE));
    }

    private static long retryAfterSeconds(final RetryInfo retryInfo) {
        final com.google.protobuf.Duration delay = retryInfo.getRetryDelay();
        final long seconds = delay.getSeconds() + (delay.getNanos() > 0 ? 1 : 0);
        return Math.max(1, seconds);
    }
}
