/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.LimitExceededException;
import software.amazon.awssdk.services.secretsmanager.model.InvalidRequestException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.aws.SecretsRefreshJob.SECRETS_REFRESH_DURATION;
import static org.opensearch.dataprepper.plugins.aws.SecretsRefreshJob.SECRETS_REFRESH_FAILURE;
import static org.opensearch.dataprepper.plugins.aws.SecretsRefreshJob.SECRETS_REFRESH_SUCCESS;
import static org.opensearch.dataprepper.plugins.aws.SecretsRefreshJob.SECRETS_SECRET_NOT_FOUND;
import static org.opensearch.dataprepper.plugins.aws.SecretsRefreshJob.SECRETS_LIMIT_EXCEEDED;
import static org.opensearch.dataprepper.plugins.aws.SecretsRefreshJob.SECRETS_ACCESS_DENIED;
import static org.opensearch.dataprepper.plugins.aws.SecretsRefreshJob.SECRET_CONFIG_ID_TAG;

@ExtendWith(MockitoExtension.class)
class SecretsRefreshJobTest {
    private static final String TEST_SECRET_CONFIG_ID = "test_secret_config_id";
    @Mock
    private SecretsSupplier secretsSupplier;

    @Mock
    private PluginConfigPublisher pluginConfigPublisher;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter secretsRefreshSuccessCounter;

    @Mock
    private Counter secretsRefreshFailureCounter;

    @Mock
    private Timer secretsRefreshTimer;

    @Mock
    private Counter secretsSecretNotFoundCounter;

    @Mock
    private Counter secretsLimitExceededCounter;

    @Mock
    private Counter secretsAccessDeniedCounter;

    private SecretsRefreshJob objectUnderTest;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counterWithTags(
                eq(SECRETS_REFRESH_SUCCESS), eq(SECRET_CONFIG_ID_TAG), eq(TEST_SECRET_CONFIG_ID)))
                .thenReturn(secretsRefreshSuccessCounter);
        when(pluginMetrics.counterWithTags(
                eq(SECRETS_REFRESH_FAILURE), eq(SECRET_CONFIG_ID_TAG), eq(TEST_SECRET_CONFIG_ID)))
                .thenReturn(secretsRefreshFailureCounter);
        doAnswer(a -> {
            a.<Runnable>getArgument(0).run();
            return null;
        }).when(secretsRefreshTimer).record(any(Runnable.class));
        when(pluginMetrics.timerWithTags(
                eq(SECRETS_REFRESH_DURATION), eq(SECRET_CONFIG_ID_TAG), eq(TEST_SECRET_CONFIG_ID)))
                .thenReturn(secretsRefreshTimer);
        when(pluginMetrics.counterWithTags(
                eq(SECRETS_SECRET_NOT_FOUND), eq(SECRET_CONFIG_ID_TAG), eq(TEST_SECRET_CONFIG_ID)))
                .thenReturn(secretsSecretNotFoundCounter);
        when(pluginMetrics.counterWithTags(
                eq(SECRETS_LIMIT_EXCEEDED), eq(SECRET_CONFIG_ID_TAG), eq(TEST_SECRET_CONFIG_ID)))
                .thenReturn(secretsLimitExceededCounter);
        when(pluginMetrics.counterWithTags(
                eq(SECRETS_ACCESS_DENIED), eq(SECRET_CONFIG_ID_TAG), eq(TEST_SECRET_CONFIG_ID)))
                .thenReturn(secretsAccessDeniedCounter);
        objectUnderTest = new SecretsRefreshJob(
                TEST_SECRET_CONFIG_ID, secretsSupplier, pluginConfigPublisher, pluginMetrics);
    }

    @Test
    void testRunWithRefreshSuccess() {
        objectUnderTest.run();

        verify(secretsRefreshTimer).record(any(Runnable.class));
        verify(secretsSupplier).refresh(TEST_SECRET_CONFIG_ID);
        verify(pluginConfigPublisher).notifyAllPluginConfigObservable();
        verify(secretsRefreshSuccessCounter).increment();
        verifyNoInteractions(secretsRefreshFailureCounter);
    }

    @Test
    void testRunWithRefreshFailure() {
        doThrow(RuntimeException.class).when(secretsSupplier).refresh(eq(TEST_SECRET_CONFIG_ID));
        objectUnderTest.run();

        verify(secretsRefreshTimer).record(any(Runnable.class));
        verify(secretsSupplier).refresh(TEST_SECRET_CONFIG_ID);
        verifyNoInteractions(pluginConfigPublisher);
        verifyNoInteractions(secretsRefreshSuccessCounter);
        verify(secretsRefreshFailureCounter).increment();
        verifyNoInteractions(secretsSecretNotFoundCounter);
        verifyNoInteractions(secretsLimitExceededCounter);
        verifyNoInteractions(secretsAccessDeniedCounter);
    }

    @Test
    void testRunWithResourceNotFoundException() {
        doThrow(ResourceNotFoundException.class).when(secretsSupplier).refresh(eq(TEST_SECRET_CONFIG_ID));
        objectUnderTest.run();

        verify(secretsRefreshTimer).record(any(Runnable.class));
        verify(secretsSupplier).refresh(TEST_SECRET_CONFIG_ID);
        verifyNoInteractions(pluginConfigPublisher);
        verifyNoInteractions(secretsRefreshSuccessCounter);
        verify(secretsRefreshFailureCounter).increment();
        verify(secretsSecretNotFoundCounter).increment();
        verifyNoInteractions(secretsLimitExceededCounter);
        verifyNoInteractions(secretsAccessDeniedCounter);
    }

    @Test
    void testRunWithLimitExceededException() {
        doThrow(LimitExceededException.class).when(secretsSupplier).refresh(eq(TEST_SECRET_CONFIG_ID));
        objectUnderTest.run();

        verify(secretsRefreshTimer).record(any(Runnable.class));
        verify(secretsSupplier).refresh(TEST_SECRET_CONFIG_ID);
        verifyNoInteractions(pluginConfigPublisher);
        verifyNoInteractions(secretsRefreshSuccessCounter);
        verify(secretsRefreshFailureCounter).increment();
        verify(secretsLimitExceededCounter).increment();
        verifyNoInteractions(secretsSecretNotFoundCounter);
        verifyNoInteractions(secretsAccessDeniedCounter);
    }

    @Test
    void testRunWithAccessDeniedException() {
        final InvalidRequestException accessDeniedException = InvalidRequestException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("AccessDeniedException")
                        .build())
                .build();
        doThrow(accessDeniedException).when(secretsSupplier).refresh(eq(TEST_SECRET_CONFIG_ID));
        
        objectUnderTest.run();

        verify(secretsRefreshTimer).record(any(Runnable.class));
        verify(secretsSupplier).refresh(TEST_SECRET_CONFIG_ID);
        verifyNoInteractions(pluginConfigPublisher);
        verifyNoInteractions(secretsRefreshSuccessCounter);
        verify(secretsRefreshFailureCounter).increment();
        verify(secretsAccessDeniedCounter).increment();
        verifyNoInteractions(secretsSecretNotFoundCounter);
        verifyNoInteractions(secretsLimitExceededCounter);
    }
}