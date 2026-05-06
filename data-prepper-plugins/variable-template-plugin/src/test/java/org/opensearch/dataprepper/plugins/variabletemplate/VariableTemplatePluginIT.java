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
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class VariableTemplatePluginIT {

    @TempDir
    Path tempDir;

    @Mock
    private ExtensionPoints extensionPoints;

    @Mock
    private ExtensionProvider.Context context;

    @Captor
    private ArgumentCaptor<ExtensionProvider<?>> providerCaptor;

    @Test
    void testAllThreeResolvers_allRegisterAndResolveCorrectly() throws IOException {
        final Path storeFile = tempDir.resolve("store.env");
        Files.writeString(storeFile, "STORE_KEY=store-value\n");

        final Path secretFile = tempDir.resolve("secret.txt");
        Files.writeString(secretFile, "file-secret\n");

        final VariableTemplatePluginConfig config = buildConfig(true, true, true,
                List.of(storeFile.toString()));
        new VariableTemplatePlugin(config).apply(extensionPoints);

        verify(extensionPoints, times(3)).addExtensionProvider(providerCaptor.capture());
        final List<ExtensionProvider<?>> providers = providerCaptor.getAllValues();

        final PluginConfigValueTranslator envTranslator = translatorFrom(providers.get(0));
        final PluginConfigValueTranslator fileTranslator = translatorFrom(providers.get(1));
        final PluginConfigValueTranslator storeTranslator = translatorFrom(providers.get(2));

        assertThat(envTranslator.getPrefix(), equalTo("env"));
        assertThat(fileTranslator.getPrefix(), equalTo("file"));
        assertThat(storeTranslator.getPrefix(), equalTo("store"));

        assertThat(envTranslator.translate("PATH"), equalTo(System.getenv("PATH")));
        assertThat(fileTranslator.translate(secretFile.toString()), equalTo("file-secret"));
        assertThat(storeTranslator.translate("STORE_KEY"), equalTo("store-value"));
    }

    @Test
    void testAllThreeResolvers_haveDistinctPrefixes() throws IOException {
        final Path storeFile = tempDir.resolve("store.env");
        Files.writeString(storeFile, "K=v\n");

        new VariableTemplatePlugin(buildConfig(true, true, true, List.of(storeFile.toString())))
                .apply(extensionPoints);

        verify(extensionPoints, times(3)).addExtensionProvider(providerCaptor.capture());
        final List<ExtensionProvider<?>> providers = providerCaptor.getAllValues();

        final String p1 = translatorFrom(providers.get(0)).getPrefix();
        final String p2 = translatorFrom(providers.get(1)).getPrefix();
        final String p3 = translatorFrom(providers.get(2)).getPrefix();

        assertThat(p1.equals(p2), is(false));
        assertThat(p1.equals(p3), is(false));
        assertThat(p2.equals(p3), is(false));
    }

    @Test
    void testEnvTranslator_missingVar_throwsIllegalArgumentException() throws IOException {
        new VariableTemplatePlugin(buildConfig(true, false, false, Collections.emptyList()))
                .apply(extensionPoints);
        verify(extensionPoints, times(1)).addExtensionProvider(providerCaptor.capture());
        assertThrows(IllegalArgumentException.class,
                () -> translatorFrom(providerCaptor.getValue()).translate("__UNSET_VAR_XYZ__"));
    }

    @Test
    void testFileTranslator_missingFile_throwsIllegalArgumentException() {
        new VariableTemplatePlugin(buildConfig(false, true, false, Collections.emptyList()))
                .apply(extensionPoints);
        verify(extensionPoints, times(1)).addExtensionProvider(providerCaptor.capture());
        assertThrows(IllegalArgumentException.class,
                () -> translatorFrom(providerCaptor.getValue()).translate(
                        tempDir.resolve("ghost.txt").toString()));
    }

    @Test
    void testStoreTranslator_missingKey_throwsIllegalArgumentException() throws IOException {
        final Path storeFile = tempDir.resolve("store.env");
        Files.writeString(storeFile, "KEY=value\n");

        new VariableTemplatePlugin(buildConfig(false, false, true, List.of(storeFile.toString())))
                .apply(extensionPoints);
        verify(extensionPoints, times(1)).addExtensionProvider(providerCaptor.capture());
        assertThrows(IllegalArgumentException.class,
                () -> translatorFrom(providerCaptor.getValue()).translate("MISSING"));
    }

    @SuppressWarnings("unchecked")
    private PluginConfigValueTranslator translatorFrom(final ExtensionProvider<?> provider) {
        return ((ExtensionProvider<PluginConfigValueTranslator>) provider)
                .provideInstance(context).orElseThrow();
    }

    private VariableTemplatePluginConfig buildConfig(
            final boolean envEnabled, final boolean fileEnabled,
            final boolean storeEnabled, final List<String> storeSources) {
        return new VariableTemplatePluginConfig() {
            @Override
            public Resolvers getResolvers() {
                return new Resolvers() {
                    @Override public boolean isEnvEnabled() { return envEnabled; }
                    @Override public boolean isFileEnabled() { return fileEnabled; }
                    @Override public StoreResolverConfig getStore() {
                        if (!storeEnabled) return null;
                        return new StoreResolverConfig() {
                            @Override public boolean isEnabled() { return true; }
                            @Override public List<String> getSources() { return storeSources; }
                        };
                    }
                };
            }
        };
    }
}
