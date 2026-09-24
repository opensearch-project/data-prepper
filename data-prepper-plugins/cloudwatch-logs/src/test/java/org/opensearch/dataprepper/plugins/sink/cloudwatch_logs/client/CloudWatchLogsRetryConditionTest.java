/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CloudWatchLogsRetryConditionTest {
    private final CloudWatchLogsRetryCondition retryCondition = new CloudWatchLogsRetryCondition();

    @ParameterizedTest
    @ValueSource(ints = {500, 502, 503, 599})
    void shouldRetry_returns_true_for_5xx_errors(final int statusCode) {
        assertThat(retryCondition.shouldRetry(contextWithStatusCode(statusCode)), equalTo(true));
    }

    @Test
    void shouldRetry_returns_true_for_throttling_error_with_400_status_code() {
        final RetryPolicyContext context = mock(RetryPolicyContext.class);
        final CloudWatchLogsException exception = (CloudWatchLogsException) CloudWatchLogsException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("ThrottlingException")
                        .errorMessage("Rate exceeded")
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(400).build())
                        .build())
                .message("Rate exceeded")
                .statusCode(400)
                .build();
        when(context.exception()).thenReturn(exception);

        assertThat(retryCondition.shouldRetry(context), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 403, 408, 499})
    void shouldRetry_returns_false_for_non_throttling_4xx_errors(final int statusCode) {
        assertThat(retryCondition.shouldRetry(contextWithStatusCode(statusCode)), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {200, 300})
    void shouldRetry_returns_false_for_non_error_status_codes(final int statusCode) {
        assertThat(retryCondition.shouldRetry(contextWithStatusCode(statusCode)), equalTo(false));
    }

    @Test
    void shouldRetry_returns_false_for_client_errors_without_a_status_code() {
        final RetryPolicyContext context = mock(RetryPolicyContext.class);
        when(context.exception()).thenReturn(SdkClientException.create("Connection failed"));

        assertThat(retryCondition.shouldRetry(context), equalTo(false));
    }

    private RetryPolicyContext contextWithStatusCode(final int statusCode) {
        final RetryPolicyContext context = mock(RetryPolicyContext.class);
        final CloudWatchLogsException exception = (CloudWatchLogsException) CloudWatchLogsException.builder()
                .message("Request failed")
                .statusCode(statusCode)
                .build();
        when(context.exception()).thenReturn(exception);
        return context;
    }
}
