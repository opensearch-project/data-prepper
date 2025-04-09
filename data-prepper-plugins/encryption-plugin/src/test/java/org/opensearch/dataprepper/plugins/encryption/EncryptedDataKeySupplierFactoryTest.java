/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EncryptedDataKeySupplierFactoryTest {
    @Mock
    private KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;

    @Test
    void testCreateStaticEncryptedDataKeySupplier() {
        final String testEncryptionKey = UUID.randomUUID().toString();
        when(kmsEncryptionEngineConfiguration.getEncryptionKey()).thenReturn(testEncryptionKey);
        final EncryptedDataKeySupplierFactory objectUnderTest = EncryptedDataKeySupplierFactory.create();
        final EncryptedDataKeySupplier encryptedDataKeySupplier = objectUnderTest.createEncryptedDataKeySupplier(
                kmsEncryptionEngineConfiguration);
        assertThat(encryptedDataKeySupplier, instanceOf(StaticEncryptedDataKeySupplier.class));
        assertThat(encryptedDataKeySupplier.retrieveValue(), equalTo(testEncryptionKey));
    }
}