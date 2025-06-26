/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static org.mockito.Mockito.mock;
import java.util.Comparator;

public class PluginCreatorContextTest {
    PluginCreatorContext pluginCreatorContext;

    @BeforeEach
    void setUp() {
        pluginCreatorContext = new PluginCreatorContext();
    }
    
    @Test
    public void test_observablePluginCreator() {
        PluginCreator pluginCreator = pluginCreatorContext.observablePluginCreator();
        assertNotNull(pluginCreator);
    }

    @Test
    public void test_pluginCreator() {
        PluginConfigurationObservableRegister pluginConfigurationObservableRegister = mock(PluginConfigurationObservableRegister.class);
        PluginCreator pluginCreator = pluginCreatorContext.pluginCreator(pluginConfigurationObservableRegister);
        assertNotNull(pluginCreator);
    }

    @Test
    public void test_extensionsLoaderComparator() {
        Comparator<ExtensionLoader.ExtensionPluginWithContext> extensionsLoaderComparator = pluginCreatorContext.extensionsLoaderComparator();
        assertNotNull(extensionsLoaderComparator);
    }
}
