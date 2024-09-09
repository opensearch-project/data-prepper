/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsPluginTest {

    @Mock
    private AwsPluginConfig awsPluginConfig;

    @Mock
    private ExtensionPoints extensionPoints;

    private AwsPlugin createObjectUnderTest() {
        return new AwsPlugin(awsPluginConfig);
    }

    @Test
    void apply_should_addExtensionProvider() {
        when(awsPluginConfig.getDefaultStsConfiguration()).thenReturn(new AwsStsConfiguration());

        createObjectUnderTest().apply(extensionPoints);

        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);

        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();

        assertThat(actualExtensionProvider, instanceOf(AwsExtensionProvider.class));
    }

    @Test
    void null_aws_plugin_config_applies_extensions_correctly() {
        final AwsPlugin objectUnderTest = new AwsPlugin(null);

        objectUnderTest.apply(extensionPoints);

        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);

        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();

        assertThat(actualExtensionProvider, instanceOf(AwsExtensionProvider.class));
    }
}