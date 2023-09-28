package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginConfigurationObservable;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

class PluginConfigurationObservableFactoryTest {
    @Mock
    private PluginConfigurationConverter pluginConfigurationConverter;

    @Mock
    private PluginSetting pluginSetting;

    private final Class<?> baseClass = Object.class;

    private final PluginConfigurationObservableFactory objectUnderTest = new PluginConfigurationObservableFactory();

    @Test
    void testCreateDefaultPluginConfigurationObservableFactory() {
        assertThat(objectUnderTest.createDefaultPluginConfigurationObservable(
                pluginConfigurationConverter, baseClass, pluginSetting),
                instanceOf(PluginConfigurationObservable.class));
    }
}