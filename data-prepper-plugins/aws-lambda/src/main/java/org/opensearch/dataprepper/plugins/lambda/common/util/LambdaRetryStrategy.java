package org.opensearch.dataprepper.plugins.lambda.common.util;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;
import software.amazon.awssdk.services.lambda.model.ServiceException;

import java.util.Set;


/**
 * Similar to BulkRetryStrategy in the OpenSearch sink.
 * Categorizes AWS Lambda exceptions and status codes into
 * retryable and non-retryable scenarios.
 */
public final class LambdaRetryStrategy {

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
     */
    public static boolean isTimeoutError(final InvokeResponse response) {
        return TIMEOUT_ERRORS.contains(response.statusCode());
    }

}

