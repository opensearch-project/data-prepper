/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EncryptionEngineFactoryTest {
    @Mock
    private KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;
    @Mock
    private KmsKeyProvider kmsKeyProvider;
    @Mock
    private KeyProviderFactory keyProviderFactory;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private EncryptedDataKeySupplier encryptedDataKeySupplier;

    private EncryptionEngineFactory objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = EncryptionEngineFactory.create(keyProviderFactory, pluginMetrics);
    }

    @Test
    void testCreateDefaultEncryptionEngine_with_KmsEncryptionEngineConfiguration() {
        when(keyProviderFactory.createKmsKeyProvider(kmsEncryptionEngineConfiguration, pluginMetrics)).thenReturn(kmsKeyProvider);
        assertThat(objectUnderTest.createEncryptionEngine(kmsEncryptionEngineConfiguration, encryptedDataKeySupplier),
                instanceOf(DefaultEncryptionEngine.class));
    }
}