package org.opensearch.dataprepper.plugins.lambda.common.util;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;
import software.amazon.awssdk.services.lambda.model.ServiceException;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;


/**
 * Similar to BulkRetryStrategy in the OpenSearch sink.
 * Categorizes AWS Lambda exceptions and status codes into
 * retryable and non-retryable scenarios.
 */
public final class LambdaRetryStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(LambdaRetryStrategy.class);

    /**
     * Maximum number of backoff retries when the connection pool is exhausted. With the
     * default base delay (100ms) and maximum backoff (20s), this waits up to roughly
     * two and a half minutes in total. Connection permits turn over within the API call
     * timeout, so a recoverable system recovers well inside this window. Not configurable:
     * with invocations bounded to the connection pool size, pool exhaustion should not
     * occur from this client and this path exists only as a backup.
     */
    private static final int CONNECTION_POOL_BACKOFF_MAX_RETRIES = 10;

    /**
     * Maximum shift applied when computing the exponential backoff ceiling. Caps
     * {@code 1L << attempt} so the multiplication cannot overflow a long.
     */
    private static final int MAX_BACKOFF_SHIFT = 20;

    /**
     * Message used by the AWS SDK Netty HTTP client when a request times out waiting to
     * acquire a connection from the connection pool. The SDK identifies this failure the
     * same way: the pool throws a plain {@link TimeoutException} with this message, which
     * the SDK wraps in an untyped {@link Throwable} and then an {@link SdkClientException}.
     * There is no dedicated exception type or error code to match on.
     */
    private static final String CONNECTION_POOL_ACQUIRE_FAILURE_MESSAGE = "Acquire operation took longer";

    private LambdaRetryStrategy() {
    }

    /**
     * Possibly a set of “bad request” style errors which might fall
     */
    private static final Set<Integer> BAD_REQUEST_ERRORS = Set.of(
                    400, // Bad Request
                    422, // Unprocessable Entity
                    417, // Expectation Failed
                    406  // Not Acceptable
    );

    /**
     * Status codes which may indicate a security or policy problem, so we don't retry.
     */
    private static final Set<Integer> NOT_ALLOWED_ERRORS = Set.of(
                    401, // Unauthorized
                    403, // Forbidden
                    405  // Method Not Allowed
        );

    /**
     * Examples of input or payload errors that are likely not retryable
     * unless the pipeline itself corrects them.
     */
    private static final Set<Integer> INVALID_INPUT_ERRORS = Set.of(
                    413, // Payload Too Large
                    414, // URI Too Long
                    416  // Range Not Satisfiable
        );

    /**
     * Example of a “timeout” scenario. Lambda can return 429 for "Too Many Requests" or
     * 408 (if applicable) for timeouts in some contexts.
     * This can be considered retryable if you want to handle the throttling scenario.
     */
    private static final Set<Integer> TIMEOUT_ERRORS = Set.of(
                    408, // Request Timeout
                    429  // Too Many Requests (often used as "throttling" for Lambda)
        );

    public static boolean isRetryableStatusCode(final int statusCode) {
        return TIMEOUT_ERRORS.contains(statusCode) || (statusCode >= 500 && statusCode < 600);
    }

    /*
     * Note:isRetryable and isRetryableException should match
     */
    public static boolean isRetryableException(final Throwable t) {
        if (t instanceof TooManyRequestsException) {
            // Throttling => often can retry with backoff
            return true;
        }
        if (t instanceof ServiceException) {
            // Usually indicates a 5xx => can retry
            return true;
        }
        if (t instanceof SdkClientException) {
            // Possibly network/connection error => can retry
            return true;
        }
        return false;
    }

    /**
     * Determines if this is definitely NOT retryable (client error or permanent failure).
     * @param response the Lambda invoke response to check
     * @return true if the response indicates a non-retryable error, false otherwise
     */
    public static boolean isNonRetryable(final InvokeResponse response) {
        if(response == null) return false;

        int statusCode = response.statusCode();
        return BAD_REQUEST_ERRORS.contains(statusCode)
                || NOT_ALLOWED_ERRORS.contains(statusCode)
                || INVALID_INPUT_ERRORS.contains(statusCode);
    }

    /**
     * For convenience, you can create more fine-grained checks or
     * direct set membership checks (e.g. isBadRequest(...), isTimeout(...)) if you want.
     * @param response the Lambda invoke response to check
     * @return true if the response indicates a timeout error, false otherwise
     */
    public static boolean isTimeoutError(final InvokeResponse response) {
        return TIMEOUT_ERRORS.contains(response.statusCode());
    }

    /**
     * Determines whether a failure was caused by the HTTP client's connection pool being
     * exhausted. This failure occurs entirely on the client before the request is sent,
     * so the Lambda function was never invoked and the request is always safe to retry,
     * even for non-idempotent functions.
     *
     * <p>Checks the full cause chain, including suppressed exceptions, because the SDK may
     * surface the acquire failure as a suppressed attempt failure inside an
     * {@code ApiCallTimeoutException}. An {@link AwsServiceException} anywhere on the chain
     * means the request reached the service, so such failures are never classified as
     * connection pool exhaustion.
     *
     * @param throwable the failure to classify
     * @return true if the failure was a connection pool acquire timeout
     */
    public static boolean isConnectionPoolAcquireFailure(final Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof AwsServiceException) {
                return false;
            }
            // Must be java.util.concurrent.TimeoutException: the connection pool throws that
            // JDK type. Netty and the SDK have their own TimeoutException types which do not match.
            if (current instanceof TimeoutException && current.getMessage() != null
                    && current.getMessage().contains(CONNECTION_POOL_ACQUIRE_FAILURE_MESSAGE)) {
                return true;
            }
            for (final Throwable suppressed : current.getSuppressed()) {
                if (isConnectionPoolAcquireFailure(suppressed)) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Joins a Lambda invocation future, retrying with backoff if it failed because the
     * HTTP client's connection pool was exhausted. Such a failure occurs before the request
     * leaves the host, so the Lambda function was never invoked and retrying cannot
     * double-invoke it, even for non-idempotent functions. Any other failure is rethrown
     * for the caller's normal failure handling.
     *
     * <p>The counter is incremented once for each invocation attempt which fails due to
     * pool exhaustion, including the initial attempt.
     *
     * @param future the in-flight invocation to join
     * @param retryOperation supplies a new invocation attempt; a supplier is used rather
     *                       than a future so each attempt re-acquires a connection permit
     * @param clientOptions source of the backoff delays
     * @param poolExhaustionCounter incremented per attempt which fails with pool exhaustion
     * @param <T> the response type of the invocation
     * @return the result of the original future or of a successful retry
     * @throws CompletionException the failure, if the future failed for another reason or
     *                             all retries were exhausted
     */
    public static <T> T joinWithConnectionPoolBackoff(final CompletableFuture<T> future,
                                                      final Supplier<CompletableFuture<T>> retryOperation,
                                                      final ClientOptions clientOptions,
                                                      final Counter poolExhaustionCounter) {
        try {
            return future.join();
        } catch (final CompletionException e) {
            if (!isConnectionPoolAcquireFailure(e)) {
                throw e;
            }
            poolExhaustionCounter.increment();
            return retryOnConnectionPoolExhaustion(retryOperation, clientOptions, poolExhaustionCounter);
        }
    }

    /**
     * Retries an operation with equal-jitter exponential backoff for as long as it keeps
     * failing with connection pool exhaustion. Sleeping happens without holding a
     * connection permit, letting the pool drain.
     *
     * <p>The loop stops as soon as an attempt fails for any other reason, because a failure
     * that reached the service is no longer known to be safe to retry.
     */
    private static <T> T retryOnConnectionPoolExhaustion(final Supplier<CompletableFuture<T>> operation,
                                                         final ClientOptions clientOptions,
                                                         final Counter poolExhaustionCounter) {
        final Duration baseDelay = clientOptions.getBaseDelay();
        final Duration maxBackoff = clientOptions.getMaxBackoff();
        CompletionException lastFailure = null;
        for (int attempt = 1; attempt <= CONNECTION_POOL_BACKOFF_MAX_RETRIES; attempt++) {
            // Floor of 1ms so a zero base delay cannot collapse into a sleepless retry loop.
            final long ceilingMillis = Math.max(1L, Math.min(maxBackoff.toMillis(),
                    baseDelay.toMillis() * (1L << Math.min(attempt, MAX_BACKOFF_SHIFT))));
            final long delayMillis = ceilingMillis / 2
                    + ThreadLocalRandom.current().nextLong(ceilingMillis / 2 + 1);
            LOG.warn(NOISY, "Lambda connection pool exhausted. Backing off {}ms before retry {}/{}.",
                    delayMillis, attempt, CONNECTION_POOL_BACKOFF_MAX_RETRIES);
            try {
                Thread.sleep(delayMillis);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw lastFailure != null ? lastFailure : new CompletionException(e);
            }

            try {
                return operation.get().join();
            } catch (final CompletionException e) {
                if (!isConnectionPoolAcquireFailure(e)) {
                    throw e;
                }
                poolExhaustionCounter.increment();
                lastFailure = e;
            }
        }
        throw lastFailure;
    }

}
