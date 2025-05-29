/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.encryption.EncryptionRefreshJob.ENCRYPTION_ID_TAG;
import static org.opensearch.dataprepper.plugins.encryption.EncryptionRefreshJob.ENCRYPTION_REFRESH_DURATION;
import static org.opensearch.dataprepper.plugins.encryption.EncryptionRefreshJob.ENCRYPTION_REFRESH_FAILURE;
import static org.opensearch.dataprepper.plugins.encryption.EncryptionRefreshJob.ENCRYPTION_REFRESH_SUCCESS;

@ExtendWith(MockitoExtension.class)
class EncryptionRefreshJobTest {
    private static final String TEST_ENCRYPTION_ID = "test_encryption_id";

    @Mock
    private EncryptedDataKeySupplier encryptedDataKeySupplier;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter encryptionRefreshSuccessCounter;
    @Mock
    private Counter encryptionRefreshFailureCounter;
    @Mock
    private Timer encryptionRefreshTimer;

    private EncryptionRefreshJob objectUnderTest;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counterWithTags(
                eq(ENCRYPTION_REFRESH_SUCCESS), eq(ENCRYPTION_ID_TAG), eq(TEST_ENCRYPTION_ID)))
                .thenReturn(encryptionRefreshSuccessCounter);
        when(pluginMetrics.counterWithTags(
                eq(ENCRYPTION_REFRESH_FAILURE), eq(ENCRYPTION_ID_TAG), eq(TEST_ENCRYPTION_ID)))
                .thenReturn(encryptionRefreshFailureCounter);
        when(pluginMetrics.timerWithTags(
                eq(ENCRYPTION_REFRESH_DURATION), eq(ENCRYPTION_ID_TAG), eq(TEST_ENCRYPTION_ID)))
                .thenReturn(encryptionRefreshTimer);
        doAnswer(a -> {
            a.<Runnable>getArgument(0).run();
            return null;
        }).when(encryptionRefreshTimer).record(any(Runnable.class));
        objectUnderTest = new EncryptionRefreshJob(TEST_ENCRYPTION_ID, encryptedDataKeySupplier, pluginMetrics);
    }

    @Test
    void testRunWithRefreshSuccess() {
        objectUnderTest.run();

        verify(encryptionRefreshTimer).record(any(Runnable.class));
        verify(encryptedDataKeySupplier).refresh();
        verify(encryptionRefreshSuccessCounter).increment();
        verifyNoInteractions(encryptionRefreshFailureCounter);
    }

    @Test
    void testRunWithRefreshFailure() {
        doThrow(RuntimeException.class).when(encryptedDataKeySupplier).refresh();
        objectUnderTest.run();

        verify(encryptionRefreshTimer).record(any(Runnable.class));
        verify(encryptedDataKeySupplier).refresh();
        verifyNoInteractions(encryptionRefreshSuccessCounter);
        verify(encryptionRefreshFailureCounter).increment();
    }
}