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
import software.amazon.awssdk.services.kms.KmsClient;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyProviderFactoryTest {
    @Mock
    private KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private KmsClient kmsClient;

    private KeyProviderFactory keyProviderFactory;

    @BeforeEach
    void setUp() {
        keyProviderFactory = KeyProviderFactory.create();
    }

    @Test
    void testCreateKmsKeyProvider() {
        when(kmsEncryptionEngineConfiguration.createKmsClient()).thenReturn(kmsClient);
        assertThat(keyProviderFactory.createKmsKeyProvider(kmsEncryptionEngineConfiguration, pluginMetrics),
                instanceOf(KmsKeyProvider.class));
    }
}