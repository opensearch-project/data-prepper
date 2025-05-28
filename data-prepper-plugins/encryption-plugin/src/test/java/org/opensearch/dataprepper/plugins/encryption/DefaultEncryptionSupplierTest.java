/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.encryption.EncryptionEngine;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultEncryptionSupplierTest {
    private static final String TEST_ENCRYPTION_ID = "test_encryption_id";
    @Mock
    private EncryptionPluginConfig encryptionPluginConfig;
    @Mock
    private EncryptionEngineConfiguration encryptionEngineConfiguration;
    @Mock
    private EncryptionEngineFactory encryptionEngineFactory;
    @Mock
    private EncryptionEngine encryptionEngine;
    @Mock
    private EncryptedDataKeySupplierFactory encryptedDataKeySupplierFactory;
    @Mock
    private EncryptedDataKeySupplier encryptedDataKeySupplier;

    private DefaultEncryptionSupplier objectUnderTest;

    @BeforeEach
    void setUp() {
        when(encryptionPluginConfig.getEncryptionConfigurationMap())
                .thenReturn(Map.of(TEST_ENCRYPTION_ID, encryptionEngineConfiguration));
        when(encryptionEngineFactory.createEncryptionEngine(encryptionEngineConfiguration, encryptedDataKeySupplier))
                .thenReturn(encryptionEngine);
        when(encryptedDataKeySupplierFactory.createEncryptedDataKeySupplier(encryptionEngineConfiguration))
                .thenReturn(encryptedDataKeySupplier);
        objectUnderTest = new DefaultEncryptionSupplier(
                encryptionPluginConfig, encryptionEngineFactory, encryptedDataKeySupplierFactory);
    }

    @Test
    void testGetEncryptionEngine() {
        assertThat(objectUnderTest.getEncryptionEngine(TEST_ENCRYPTION_ID), is(encryptionEngine));
        assertThat(objectUnderTest.getEncryptionEngine("non-existing-id"), nullValue());
    }

    @Test
    void testGetEncryptedDataKeySupplier_exists() {
        assertThat(objectUnderTest.getEncryptedDataKeySupplier(TEST_ENCRYPTION_ID), is(encryptedDataKeySupplier));
        assertThat(objectUnderTest.getEncryptedDataKeySupplier("non-existing-id"), nullValue());
    }
}