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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class VariableTemplatePluginTest {

    @Mock
    private ExtensionPoints extensionPoints;

    @Captor
    private ArgumentCaptor<ExtensionProvider<?>> providerCaptor;

    private VariableTemplatePluginConfig configWith(
            final boolean envEnabled, final boolean fileEnabled, final boolean storeEnabled,
            final List<String> storeSources) {
        final VariableTemplatePluginConfig config = new VariableTemplatePluginConfig();
        final VariableTemplatePluginConfig.Resolvers resolvers = new VariableTemplatePluginConfig.Resolvers() {
            @Override public boolean isEnvEnabled() { return envEnabled; }
            @Override public boolean isFileEnabled() { return fileEnabled; }
            @Override public VariableTemplatePluginConfig.StoreResolverConfig getStore() {
                if (!storeEnabled) return null;
                return new VariableTemplatePluginConfig.StoreResolverConfig() {
                    @Override public boolean isEnabled() { return true; }
                    @Override public List<String> getSources() { return storeSources; }
                };
            }
        };
        return new VariableTemplatePluginConfig() {
            @Override public Resolvers getResolvers() { return resolvers; }
        };
    }

    @Test
    void testApply_nullConfig_noProviders() {
        new VariableTemplatePlugin(null).apply(extensionPoints);
        verifyNoInteractions(extensionPoints);
    }

    @Test
    void testApply_allDisabled_noProviders() {
        new VariableTemplatePlugin(new VariableTemplatePluginConfig()).apply(extensionPoints);
        verifyNoInteractions(extensionPoints);
    }

    @Test
    void testApply_envOnly_registersEnvProvider() {
        new VariableTemplatePlugin(configWith(true, false, false, null)).apply(extensionPoints);
        verify(extensionPoints, times(1)).addExtensionProvider(providerCaptor.capture());
        assertThat(providerCaptor.getValue().provideInstance(null).orElseThrow(),
                instanceOf(EnvVariableTranslator.class));
    }

    @Test
    void testApply_fileOnly_registersFileProvider() {
        new VariableTemplatePlugin(configWith(false, true, false, null)).apply(extensionPoints);
        verify(extensionPoints, times(1)).addExtensionProvider(providerCaptor.capture());
        assertThat(providerCaptor.getValue().provideInstance(null).orElseThrow(),
                instanceOf(FileVariableTranslator.class));
    }

    @Test
    void testApply_allEnabled_registersThreeProviders() {
        new VariableTemplatePlugin(configWith(true, true, true, Collections.emptyList())).apply(extensionPoints);
        verify(extensionPoints, times(3)).addExtensionProvider(providerCaptor.capture());
        final List<ExtensionProvider<?>> providers = providerCaptor.getAllValues();
        assertThat(providers.get(0).provideInstance(null).orElseThrow(), instanceOf(EnvVariableTranslator.class));
        assertThat(providers.get(1).provideInstance(null).orElseThrow(), instanceOf(FileVariableTranslator.class));
        assertThat(providers.get(2).provideInstance(null).orElseThrow(), instanceOf(StoreVariableTranslator.class));
    }

    @Test
    void testApply_storeConfigPresent_butDisabled_noStoreProvider() {
        new VariableTemplatePlugin(configWith(false, false, false, null)).apply(extensionPoints);
        verifyNoInteractions(extensionPoints);
    }

        @Test
    void testShutdown_doesNotThrow() {
        new VariableTemplatePlugin(new VariableTemplatePluginConfig()).shutdown();
    }
}
