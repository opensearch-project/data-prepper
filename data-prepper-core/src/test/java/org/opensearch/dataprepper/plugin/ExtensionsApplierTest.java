/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtensionsApplierTest {
    @Mock
    private DataPrepperExtensionPoints dataPrepperExtensionPoints;
    @Mock
    private ExtensionLoader extensionLoader;

    private ExtensionsApplier createObjectUnderTest() {
        return new ExtensionsApplier(
                dataPrepperExtensionPoints, extensionLoader);
    }

    @AfterEach
    void extensionPointsHasNoInteractions() {
        verifyNoInteractions(dataPrepperExtensionPoints);
    }

    @Test
    void applyExtensions_calls_apply_on_all_loaded_extensions() {
        final List<ExtensionPlugin> extensionPlugins = List.of(mock(ExtensionPlugin.class), mock(ExtensionPlugin.class), mock(ExtensionPlugin.class));
        when(extensionLoader.loadExtensions()).thenReturn((List) extensionPlugins);

        createObjectUnderTest().applyExtensions();

        for (ExtensionPlugin extensionPlugin : extensionPlugins) {
            verify(extensionPlugin).apply(dataPrepperExtensionPoints);
        }
    }

    @Test
    void applyExtensions_with_empty_extensions_is_ok() {
        when(extensionLoader.loadExtensions()).thenReturn(Collections.emptyList());

        createObjectUnderTest().applyExtensions();
    }
}