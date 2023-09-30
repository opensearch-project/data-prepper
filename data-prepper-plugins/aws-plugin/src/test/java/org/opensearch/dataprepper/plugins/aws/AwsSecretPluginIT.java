package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;
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
        verify(extensionPoints, times(2)).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final List<ExtensionProvider> actualExtensionProviders = extensionProviderArgumentCaptor.getAllValues();
        assertThat(actualExtensionProviders.get(0), instanceOf(AwsSecretsPluginConfigValueTranslatorExtensionProvider.class));
        final Optional<PluginConfigValueTranslator> optionalPluginConfigValueTranslator =
                actualExtensionProviders.get(0).provideInstance(context);
        assertThat(optionalPluginConfigValueTranslator.isPresent(), is(true));
        assertThat(optionalPluginConfigValueTranslator.get(), instanceOf(AwsSecretsPluginConfigValueTranslator.class));
        assertThat(actualExtensionProviders.get(1), instanceOf(AwsSecretsPluginConfigPublisherExtensionProvider.class));
        final Optional<PluginConfigPublisher> optionalPluginConfigPublisher =
                actualExtensionProviders.get(1).provideInstance(context);
        assertThat(optionalPluginConfigPublisher.isPresent(), is(true));
    }

    @Test
    void testInitializationWithNullConfig() {
        objectUnderTest = new AwsSecretPlugin(null);
        objectUnderTest.apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints, times(2)).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final List<ExtensionProvider> actualExtensionProviders = extensionProviderArgumentCaptor.getAllValues();
        assertThat(actualExtensionProviders.get(0), instanceOf(AwsSecretsPluginConfigValueTranslatorExtensionProvider.class));
        final Optional<PluginConfigValueTranslator> optionalPluginConfigValueTranslator =
                actualExtensionProviders.get(0).provideInstance(context);
        assertThat(optionalPluginConfigValueTranslator.isEmpty(), is(true));
        assertThat(actualExtensionProviders.get(1), instanceOf(AwsSecretsPluginConfigPublisherExtensionProvider.class));
        final Optional<PluginConfigPublisher> optionalPluginConfigPublisher =
                actualExtensionProviders.get(1).provideInstance(context);
        assertThat(optionalPluginConfigPublisher.isEmpty(), is(true));
    }
}