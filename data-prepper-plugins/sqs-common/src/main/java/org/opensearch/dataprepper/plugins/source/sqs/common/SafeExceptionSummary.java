/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs.common;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;

/**
 * Extracts only safe, non-sensitive fields from exceptions for logging purposes.
 * <p>
 * This class uses an allowlist approach: only explicitly safe fields are included
 * in the summary. This prevents accidental exposure of sensitive data such as
 * SQS receipt handles, IAM credentials, AWS account IDs, or customer data that
 * may appear in exception messages or stack traces.
 * <p>
 * Safe fields include: exception class name, AWS error code, HTTP status code,
 * request ID, and whether the error is retryable.
 */
public final class SafeExceptionSummary {

    private static final int MAX_CAUSE_DEPTH = 5;

    private SafeExceptionSummary() {
    }

    /**
     * Produces a safe, non-sensitive summary of the given exception.
     *
     * @param e the exception to summarize
     * @return a string containing only safe metadata about the exception
     */
    public static String summarize(final Throwable e) {
        if (e == null) {
            return "null";
        }

        final StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getSimpleName());

        if (e instanceof AwsServiceException) {
            appendAwsServiceDetails(sb, (AwsServiceException) e);
        } else if (e instanceof SdkException) {
            appendSdkDetails(sb, (SdkException) e);
        }

        appendCauseChain(sb, e);
        return sb.toString();
    }

    private static void appendAwsServiceDetails(final StringBuilder sb, final AwsServiceException e) {
        sb.append(" [");
        sb.append("statusCode=").append(e.statusCode());

        if (e.awsErrorDetails() != null) {
            if (e.awsErrorDetails().errorCode() != null) {
                sb.append(", errorCode=").append(e.awsErrorDetails().errorCode());
            }
            if (e.awsErrorDetails().serviceName() != null) {
                sb.append(", service=").append(e.awsErrorDetails().serviceName());
            }
        }

        if (e.requestId() != null) {
            sb.append(", requestId=").append(e.requestId());
        }

        sb.append(", retryable=").append(e.retryable());
        sb.append("]");
    }

    private static void appendSdkDetails(final StringBuilder sb, final SdkException e) {
        sb.append(" [retryable=").append(e.retryable()).append("]");
    }

    private static void appendCauseChain(final StringBuilder sb, final Throwable e) {
        Throwable cause = e.getCause();
        int depth = 0;
        while (cause != null && depth < MAX_CAUSE_DEPTH) {
            sb.append(" <- ").append(cause.getClass().getSimpleName());
            if (cause instanceof AwsServiceException) {
                final AwsServiceException awsCause = (AwsServiceException) cause;
                if (awsCause.awsErrorDetails() != null && awsCause.awsErrorDetails().errorCode() != null) {
                    sb.append("(").append(awsCause.awsErrorDetails().errorCode()).append(")");
                }
            }
            cause = cause.getCause();
            depth++;
        }
    }
}
