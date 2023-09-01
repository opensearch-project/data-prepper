package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsSecretPluginIT {
    @Mock
    private AwsSecretPluginConfig awsSecretPluginConfig;

    @Mock
    private ExtensionPoints extensionPoints;

    @Mock
    private ExtensionProvider.Context context;

    private AwsSecretPlugin objectUnderTest;

    @Test
    void testInitializationWithNonNullConfig() {
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(Collections.emptyMap());
        objectUnderTest = new AwsSecretPlugin(awsSecretPluginConfig);
        objectUnderTest.apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();
        assertThat(actualExtensionProvider, instanceOf(AwsSecretExtensionProvider.class));
        final Optional<PluginConfigValueTranslator> optionalPluginConfigValueTranslator =
                actualExtensionProvider.provideInstance(context);
        assertThat(optionalPluginConfigValueTranslator.isPresent(), is(true));
        assertThat(optionalPluginConfigValueTranslator.get(), instanceOf(AwsSecretsPluginConfigValueTranslator.class));
    }

    @Test
    void testInitializationWithNullConfig() {
        objectUnderTest = new AwsSecretPlugin(null);
        objectUnderTest.apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();
        assertThat(actualExtensionProvider, instanceOf(AwsSecretExtensionProvider.class));
        final Optional<PluginConfigValueTranslator> optionalPluginConfigValueTranslator =
                actualExtensionProvider.provideInstance(context);
        assertThat(optionalPluginConfigValueTranslator.isPresent(), is(true));
        assertThat(optionalPluginConfigValueTranslator.get(), instanceOf(NoopPluginConfigValueTranslator.class));
    }
}