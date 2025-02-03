/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EncryptionPluginTest {
    @Mock
    private ExtensionPoints extensionPoints;
    @Mock
    private ExtensionProvider.Context context;
    @Mock
    private EncryptionPluginConfig encryptionPluginConfig;

    private EncryptionPlugin objectUnderTest;

    @Test
    void testInitializationWithNonNullConfig() {
        when(encryptionPluginConfig.getEncryptionConfigurationMap()).thenReturn(Collections.emptyMap());
        objectUnderTest = new EncryptionPlugin(encryptionPluginConfig);
        objectUnderTest.apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();
        assertThat(actualExtensionProvider, instanceOf(EncryptionSupplierExtensionProvider.class));
        final Optional<EncryptionSupplier> optionalEncryptionSupplier =
                actualExtensionProvider.provideInstance(context);
        assertThat(optionalEncryptionSupplier.isPresent(), is(true));
    }

    @Test
    void testInitializationWithNullConfig() {
        objectUnderTest = new EncryptionPlugin(null);
        objectUnderTest.apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();
        assertThat(actualExtensionProvider, instanceOf(EncryptionSupplierExtensionProvider.class));
        final Optional<EncryptionSupplier> optionalEncryptionSupplier =
                actualExtensionProvider.provideInstance(context);
        assertThat(optionalEncryptionSupplier.isEmpty(), is(true));
    }
}