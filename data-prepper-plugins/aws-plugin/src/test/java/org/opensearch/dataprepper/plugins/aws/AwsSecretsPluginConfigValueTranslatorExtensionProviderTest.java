/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class AwsSecretsPluginConfigValueTranslatorExtensionProviderTest {
    @Mock
    private PluginConfigValueTranslator pluginConfigValueTranslator;

    @Mock
    private ExtensionProvider.Context context;

    private AwsSecretsPluginConfigValueTranslatorExtensionProvider createObjectUnderTest() {
        return new AwsSecretsPluginConfigValueTranslatorExtensionProvider(pluginConfigValueTranslator);
    }

    @Test
    void supportedClass_returns_PluginConfigValueTranslator() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(PluginConfigValueTranslator.class));
    }

    @Test
    void provideInstance_returns_the_PluginConfigValueTranslator_from_the_constructor() {
        final AwsSecretsPluginConfigValueTranslatorExtensionProvider objectUnderTest = createObjectUnderTest();

        final Optional<PluginConfigValueTranslator> optionalPluginConfigValueTranslator =
                objectUnderTest.provideInstance(context);
        assertThat(optionalPluginConfigValueTranslator, notNullValue());
        assertThat(optionalPluginConfigValueTranslator.isPresent(), equalTo(true));
        assertThat(optionalPluginConfigValueTranslator.get(), equalTo(pluginConfigValueTranslator));

        final Optional<PluginConfigValueTranslator> anotherOptionalPluginConfigValueTranslator =
                objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalPluginConfigValueTranslator, notNullValue());
        assertThat(anotherOptionalPluginConfigValueTranslator.isPresent(), equalTo(true));
        assertThat(anotherOptionalPluginConfigValueTranslator.get(), sameInstance(
                anotherOptionalPluginConfigValueTranslator.get()));
    }
}