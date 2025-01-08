package org.opensearch.dataprepper.plugins.lambda.common.util;

import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;
import software.amazon.awssdk.services.lambda.model.ServiceException;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;

import static org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler.isSuccess;

/**
 * Similar to BulkRetryStrategy in the OpenSearch sink.
 * Categorizes AWS Lambda exceptions and status codes into
 * retryable and non-retryable scenarios.
 */
public final class LambdaRetryStrategy {

    private LambdaRetryStrategy() {
    }

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

    public static boolean isRetryableResponse(final InvokeResponse response) {
        int statusCode = response.statusCode();
        // Throttling or internal error then retry
        return (statusCode == 429) || (statusCode >= 500 && statusCode < 600);
    }

    /**
     * Set of status codes that should generally NOT be retried
     * because they indicate client-side or permanent errors.
     */
    private static final Set<Integer> NON_RETRY_STATUS = new HashSet<>(
            Arrays.asList(
                    400, // ExpiredTokenException
                    403, // IncompleteSignature, AccessDeniedException, AccessDeniedException
                    404, // Not Found
                    409  // Conflict
            )
    );

    /**
     * Possibly a set of “bad request” style errors which might fall
     * under the NON_RETRY_STATUS or be handled differently if you prefer.
     */
    private static final Set<Integer> BAD_REQUEST_ERRORS = new HashSet<>(
            Arrays.asList(
                    400, // Bad Request
                    422, // Unprocessable Entity
                    417, // Expectation Failed
                    406  // Not Acceptable
            )
    );

    /**
     * Status codes which may indicate a security or policy problem, so we don't retry.
     */
    private static final Set<Integer> NOT_ALLOWED_ERRORS = new HashSet<>(
            Arrays.asList(
                    401, // Unauthorized
                    403, // Forbidden
                    405  // Method Not Allowed
            )
    );

    /**
     * Examples of input or payload errors that are likely not retryable
     * unless the pipeline itself corrects them.
     */
    private static final Set<Integer> INVALID_INPUT_ERRORS = new HashSet<>(
            Arrays.asList(
                    413, // Payload Too Large
                    414, // URI Too Long
                    416  // Range Not Satisfiable
                    // ...
            )
    );

    /**
     * Example of a “timeout” scenario. Lambda can return 429 for "Too Many Requests" or
     * 408 (if applicable) for timeouts in some contexts.
     * This can be considered retryable if you want to handle the throttling scenario.
     */
    private static final Set<Integer> TIMEOUT_ERRORS = new HashSet<>(
            Arrays.asList(
                    408, // Request Timeout
                    429  // Too Many Requests (often used as "throttling" for Lambda)
            )
    );


    public static boolean isRetryable(final InvokeResponse response) {
        if(response == null) return false;
        int statusCode = response.statusCode();
        // Example logic: 429 (Too Many Requests) or 5xx => retry
        return statusCode == 429 || (statusCode >= 500 && statusCode < 600);
    }

    /**
     * Determines if this is definitely NOT retryable (client error or permanent failure).
     */
    public static boolean isNonRetryable(final InvokeResponse response) {
        if(response == null) return false;

        int statusCode = response.statusCode();
        return NON_RETRY_STATUS.contains(statusCode)
                || BAD_REQUEST_ERRORS.contains(statusCode)
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

    public static InvokeResponse retryOrFail(
            final LambdaAsyncClient lambdaAsyncClient,
            final Buffer buffer,
            final LambdaCommonConfig config,
            final InvokeResponse previousResponse,
            final Logger LOG
    ) {
        int maxRetries = config.getClientOptions().getMaxConnectionRetries();
        Duration backoff = config.getClientOptions().getBaseDelay();

        int attempt = 1;
        InvokeResponse response = previousResponse;

        do{
            LOG.warn("Retrying Lambda invocation attempt {} of {} after {} ms backoff",
                    attempt, maxRetries, backoff);
            try {
                // Sleep for backoff
                Thread.sleep(backoff.toMillis());

                // Re-invoke Lambda with the same payload
                InvokeRequest requestPayload = buffer.getRequestPayload(
                        config.getFunctionName(),
                        config.getInvocationType().getAwsLambdaValue()
                );
                // Do a synchronous call.
                response = lambdaAsyncClient.invoke(requestPayload).join();

                if (isSuccess(response)) {
                    LOG.info("Retry attempt {} succeeded with status code {}", attempt, response.statusCode());
                    return response;
                } else{
                    throw new RuntimeException();
                }
            } catch (Exception e) {
                LOG.error("Failed to invoke failed with exception {} in attempt {}", e.getMessage(), attempt);
                if(!isRetryable(response)){
                    throw new RuntimeException("Failed to invoke failed",e);
                }
            }
            attempt++;
        } while(attempt <= maxRetries && isRetryable(response));

        return response;
    }

}

