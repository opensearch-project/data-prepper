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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.model.SqsException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

class SafeExceptionSummaryTest {

    private static final String FAKE_RECEIPT_HANDLE = "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a+Tt7eyMpGd3QR1SSgp9myQ";
    private static final String FAKE_ACCOUNT_ID = "123456789012";
    private static final String FAKE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue";
    private static final String FAKE_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE";
    private static final String FAKE_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    private static final String FAKE_SESSION_TOKEN = "FwoGZXIvYXdzEBYaDHqa0AP6QEcco3u0iiLEAiEPgk7mO3Y7lg==";

    @Test
    void summarize_withNull_returnsNull() {
        assertThat(SafeExceptionSummary.summarize(null), equalTo("null"));
    }

    @Test
    void summarize_withPlainException_returnsClassName() {
        final RuntimeException e = new RuntimeException("secret data: " + FAKE_RECEIPT_HANDLE);
        final String summary = SafeExceptionSummary.summarize(e);

        assertThat(summary, containsString("RuntimeException"));
        assertThat("Receipt handle must not appear in summary", summary, not(containsString(FAKE_RECEIPT_HANDLE)));
        assertThat("Exception message must not appear in summary", summary, not(containsString("secret data")));
    }

    @Test
    void summarize_withAwsServiceException_includesSafeFieldsOnly() {
        final SqsException e = (SqsException) SqsException.builder()
                .message("User: arn:aws:iam::" + FAKE_ACCOUNT_ID + ":role/MyRole is not authorized, receiptHandle=" + FAKE_RECEIPT_HANDLE)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("AccessDenied")
                        .serviceName("SQS")
                        .build())
                .statusCode(403)
                .requestId("req-12345-abcde")
                .build();

        final String summary = SafeExceptionSummary.summarize(e);

        // Safe fields are present
        assertThat(summary, containsString("SqsException"));
        assertThat(summary, containsString("statusCode=403"));
        assertThat(summary, containsString("errorCode=AccessDenied"));
        assertThat(summary, containsString("service=SQS"));
        assertThat(summary, containsString("requestId=req-12345-abcde"));

        // Sensitive data is NOT present
        assertThat("Account ID must not appear", summary, not(containsString(FAKE_ACCOUNT_ID)));
        assertThat("Receipt handle must not appear", summary, not(containsString(FAKE_RECEIPT_HANDLE)));
        assertThat("ARN must not appear", summary, not(containsString("arn:aws:iam")));
        assertThat("Raw message must not appear", summary, not(containsString("is not authorized")));
    }

    @Test
    void summarize_withCredentialsInMessage_doesNotLeakCredentials() {
        final RuntimeException e = new RuntimeException(
                "Failed to sign request: accessKeyId=" + FAKE_ACCESS_KEY
                        + ", secretAccessKey=" + FAKE_SECRET_KEY
                        + ", sessionToken=" + FAKE_SESSION_TOKEN);

        final String summary = SafeExceptionSummary.summarize(e);

        assertThat("Access key must not appear", summary, not(containsString(FAKE_ACCESS_KEY)));
        assertThat("Secret key must not appear", summary, not(containsString(FAKE_SECRET_KEY)));
        assertThat("Session token must not appear", summary, not(containsString(FAKE_SESSION_TOKEN)));
        assertThat("Only class name should appear", summary, containsString("RuntimeException"));
    }

    @Test
    void summarize_withQueueUrlInMessage_doesNotLeakQueueUrl() {
        final RuntimeException e = new RuntimeException(
                "Failed to delete message from " + FAKE_QUEUE_URL + " with receiptHandle=" + FAKE_RECEIPT_HANDLE);

        final String summary = SafeExceptionSummary.summarize(e);

        assertThat("Queue URL must not appear", summary, not(containsString(FAKE_QUEUE_URL)));
        assertThat("Account ID must not appear", summary, not(containsString(FAKE_ACCOUNT_ID)));
        assertThat("Receipt handle must not appear", summary, not(containsString(FAKE_RECEIPT_HANDLE)));
    }

    @Test
    void summarize_withNestedCause_includesCauseClassNameOnly() {
        final RuntimeException innerCause = new RuntimeException(
                "Credential expired: accessKeyId=" + FAKE_ACCESS_KEY);
        final SdkClientException outerException = SdkClientException.builder()
                .message("Unable to execute request: " + FAKE_QUEUE_URL)
                .cause(innerCause)
                .build();

        final String summary = SafeExceptionSummary.summarize(outerException);

        assertThat(summary, containsString("SdkClientException"));
        assertThat(summary, containsString("RuntimeException"));
        assertThat("Queue URL must not appear", summary, not(containsString(FAKE_QUEUE_URL)));
        assertThat("Access key must not appear", summary, not(containsString(FAKE_ACCESS_KEY)));
        assertThat("Cause message must not appear", summary, not(containsString("Credential expired")));
    }

    @Test
    void summarize_withSdkClientException_includesRetryableFlag() {
        final SdkClientException e = SdkClientException.builder()
                .message("Connection timed out to " + FAKE_QUEUE_URL)
                .build();

        final String summary = SafeExceptionSummary.summarize(e);

        assertThat(summary, containsString("SdkClientException"));
        assertThat(summary, containsString("retryable="));
        assertThat("Queue URL must not appear", summary, not(containsString(FAKE_QUEUE_URL)));
    }

    @Test
    void summarize_withDeeplyCausedChain_limitsDepth() {
        Throwable current = new RuntimeException("leaf: " + FAKE_SECRET_KEY);
        for (int i = 0; i < 10; i++) {
            current = new RuntimeException("level " + i + ": " + FAKE_ACCESS_KEY, current);
        }

        final String summary = SafeExceptionSummary.summarize(current);

        assertThat("Secret key must not appear", summary, not(containsString(FAKE_SECRET_KEY)));
        assertThat("Access key must not appear", summary, not(containsString(FAKE_ACCESS_KEY)));
        // Should not overflow — limited to MAX_CAUSE_DEPTH
        assertThat(summary, containsString("RuntimeException"));
    }
}
