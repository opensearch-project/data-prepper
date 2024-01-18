/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

class PluginConfigObservableFactoryTest {
    @Mock
    private PluginConfigurationConverter pluginConfigurationConverter;

    @Mock
    private PluginSetting pluginSetting;

    private final Class<?> baseClass = Object.class;

    private final PluginConfigurationObservableFactory objectUnderTest = new PluginConfigurationObservableFactory();

    @Test
    void testCreateDefaultPluginConfigurationObservableFactory() {
        assertThat(objectUnderTest.createDefaultPluginConfigObservable(
                pluginConfigurationConverter, baseClass, pluginSetting),
                instanceOf(PluginConfigObservable.class));
    }
}