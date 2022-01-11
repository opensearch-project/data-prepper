/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.InvalidPluginConfigurationException;
import com.amazon.dataprepper.plugins.TestPlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration test of the plugin framework. These tests should not mock any portion
 * of the plugin framework. But, they may mock inputs when appropriate.
 */
class DefaultPluginFactoryIT {

    private String pluginName;
    private String pipelineName;

    @BeforeEach
    void setUp() {
        pluginName = "test_plugin";
        pipelineName = UUID.randomUUID().toString();
    }

    private DefaultPluginFactory createObjectUnderTest() {
        return new DefaultPluginFactory();
    }

    @Test
    void loadPlugin_should_return_a_new_plugin_instance_with_the_expected_configuration() {

        final String requiredStringValue = UUID.randomUUID().toString();
        final String optionalStringValue = UUID.randomUUID().toString();

        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("required_string", requiredStringValue);
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = createPluginSettings(pluginSettingMap);

        final TestPluggableInterface plugin = createObjectUnderTest().loadPlugin(TestPluggableInterface.class, pluginSetting);

        assertThat(plugin, instanceOf(TestPlugin.class));

        final TestPlugin testPlugin = (TestPlugin) plugin;

        final TestPluginConfiguration configuration = testPlugin.getConfiguration();

        assertThat(configuration.getRequiredString(), equalTo(requiredStringValue));
        assertThat(configuration.getOptionalString(), equalTo(optionalStringValue));
    }

    @Test
    void loadPlugin_should_throw_when_a_plugin_configuration_is_invalid() {
        final String optionalStringValue = UUID.randomUUID().toString();

        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = createPluginSettings(pluginSettingMap);

        final DefaultPluginFactory objectUnderTest = createObjectUnderTest();

        final InvalidPluginConfigurationException actualException = assertThrows(InvalidPluginConfigurationException.class,
                () -> objectUnderTest.loadPlugin(TestPluggableInterface.class, pluginSetting));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), equalTo("Plugin test_plugin in pipeline " + pipelineName + " is configured incorrectly: requiredString must not be null"));
    }

    private PluginSetting createPluginSettings(final Map<String, Object> pluginSettingMap) {
        final PluginSetting pluginSetting = new PluginSetting(pluginName, pluginSettingMap);
        pluginSetting.setPipelineName(pipelineName);
        return pluginSetting;
    }
}