/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class VariableTemplateExtensionProviderTest {

    @Mock
    private ExtensionProvider.Context context;

    @Test
    void testProvideInstance_returnsTranslator() {
        final PluginConfigValueTranslator translator = new EnvVariableTranslator();
        final VariableTemplateExtensionProvider provider = new VariableTemplateExtensionProvider(translator);

        final Optional<PluginConfigValueTranslator> result = provider.provideInstance(context);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get(), equalTo(translator));
    }

    @Test
    void testSupportedClass_returnsInterface() {
        final VariableTemplateExtensionProvider provider =
                new VariableTemplateExtensionProvider(new EnvVariableTranslator());
        assertThat(provider.supportedClass(), equalTo(PluginConfigValueTranslator.class));
    }
}
