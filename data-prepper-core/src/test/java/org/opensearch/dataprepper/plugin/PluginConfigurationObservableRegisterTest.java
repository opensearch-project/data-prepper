/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;

import java.util.Set;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PluginConfigurationObservableRegisterTest {
    @Mock
    private PluginConfigPublisher pluginConfigPublisher;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    private PluginConfigurationObservableRegister objectUnderTest;

    @BeforeEach
    void setup() {
        objectUnderTest = new PluginConfigurationObservableRegister(Set.of(pluginConfigPublisher));
    }

    @Test
    void testRegisterPluginConfigurationObservables() {
        final Object[] constructorArgs = new Object[] { pluginSetting, pluginConfigObservable };
        objectUnderTest.registerPluginConfigurationObservables(constructorArgs);
        verify(pluginConfigPublisher).addPluginConfigObservable(pluginConfigObservable);
    }
}