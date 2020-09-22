package com.amazon.situp;

import com.amazon.situp.model.configuration.Configuration;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.parser.model.PipelineConfiguration;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestDataProvider {
    public static String TEST_PLUGIN_NAME = "test-plugin";
    public static final String VALID_CONFIGURATION_FILE = "valid_pipeline_configuration.yml";
    public static final String MISSING_COMPONENT_CONFIGURATION_FILE = "missing_component_configuration.yml";
    public static String INVALID_SOURCE_VIOLATION_MESSAGE = "Invalid source configuration; Requires exactly " +
            "one valid source";
    public static String INVALID_BUFFER_VIOLATION_MESSAGE = "Invalid buffer configuration; Requires at most " +
            "one valid buffer";
    public static String INVALID_PROCESSOR_VIOLATION_MESSAGE = "Invalid processor configuration.";
    public static String INVALID_SINK_VIOLATION_MESSAGE = "Invalid sink configuration; Requires at least " +
            "one valid sink";
    public static List<PluginSetting> EMPTY_PLUGIN_SETTINGS = new ArrayList<>(0);
    public static PluginSetting VALID_PLUGIN_SETTING = new PluginSetting(TEST_PLUGIN_NAME, validSettingsForPlugin());
    public static PluginSetting PLUGIN_SETTING_WITHOUT_NAME = new PluginSetting("", validSettingsForPlugin());
    public static PluginSetting PLUGIN_WITH_NO_SETTINGS_REQUIRED = new PluginSetting(TEST_PLUGIN_NAME, null);
    public static PluginSetting PLUGIN_SETTING_WITH_NULL_NAME = new PluginSetting(null, validSettingsForPlugin());
    public static List<PluginSetting> SINGLE_PLUGIN_SETTINGS = Collections.singletonList(VALID_PLUGIN_SETTING);
    public static List<PluginSetting> MULTIPLE_PLUGIN_SETTINGS = Arrays.asList(
            VALID_PLUGIN_SETTING, VALID_PLUGIN_SETTING);
    public static List<PluginSetting> MULTIPLE_PARTIAL_INVALID_PLUGIN_SETTINGS = Arrays.asList(VALID_PLUGIN_SETTING,
            PLUGIN_SETTING_WITHOUT_NAME);
    public static List<PluginSetting> PLUGIN_SETTINGS_WITHOUT_NAME = Collections.singletonList(
            PLUGIN_SETTING_WITHOUT_NAME);
    public static List<PluginSetting> PLUGIN_SETTINGS_WITH_NULL_NAME = Collections.singletonList(
            PLUGIN_SETTING_WITH_NULL_NAME);
    public static Configuration VALID_CONFIGURATION_WITH_SINGLE_PLUGIN = new Configuration(Collections.emptyMap(),
            SINGLE_PLUGIN_SETTINGS);
    public static Configuration CONFIGURATION_WITH_EMPTY_NAME = new Configuration(Collections.emptyMap(),
            PLUGIN_SETTINGS_WITHOUT_NAME);
    public static Configuration CONFIGURATION_WITH_NULL_NAME = new Configuration(Collections.emptyMap(),
            PLUGIN_SETTINGS_WITH_NULL_NAME);
    public static Configuration CONFIGURATION_WITH_EMPTY_PLUGINS = new Configuration(Collections.emptyMap(),
            EMPTY_PLUGIN_SETTINGS);
    public static Configuration CONFIGURATION_WITH_PLUGIN_WITH_NO_SETTINGS = new Configuration(Collections.emptyMap(),
            Collections.singletonList(PLUGIN_WITH_NO_SETTINGS_REQUIRED));
    public static Configuration CONFIGURATION_WITH_MULTIPLE_PLUGINS = new Configuration(Collections.emptyMap(),
            MULTIPLE_PLUGIN_SETTINGS);
    public static Configuration CONFIGURATION_WITH_MULTIPLE_PLUGINS_SOME_INVALID = new Configuration(Collections.emptyMap(),
            MULTIPLE_PARTIAL_INVALID_PLUGIN_SETTINGS);

    public static PipelineConfiguration validPipelineConfiguration() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_PLUGIN_WITH_NO_SETTINGS,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithSourceButEmptyName() {
        return new PipelineConfiguration(
                CONFIGURATION_WITH_EMPTY_NAME,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithSourceButNullName() {
        return new PipelineConfiguration(
                CONFIGURATION_WITH_NULL_NAME,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithNoPluginsForSource() {
        return new PipelineConfiguration(
                CONFIGURATION_WITH_EMPTY_PLUGINS,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithMultipleSources() {
        return new PipelineConfiguration(
                CONFIGURATION_WITH_MULTIPLE_PLUGINS,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithNoBuffer() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_EMPTY_PLUGINS,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithBufferButEmptyName() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_EMPTY_NAME,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithBufferButNullName() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_NULL_NAME,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithMultipleBuffers() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_MULTIPLE_PLUGINS,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithNoProcessors() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_EMPTY_PLUGINS,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithMultipleProcessors() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_MULTIPLE_PLUGINS,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithMultipleProcessorsSomeInvalid() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_MULTIPLE_PLUGINS_SOME_INVALID,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithProcessorsButEmptyName() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_EMPTY_NAME,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithProcessorsButNullName() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_NULL_NAME,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN);
    }

    public static PipelineConfiguration pipelineConfigurationWithSinkButEmptyName() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_EMPTY_NAME);
    }

    public static PipelineConfiguration pipelineConfigurationWithSinkButNullName() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_NULL_NAME);
    }

    public static PipelineConfiguration pipelineConfigurationWithMultipleSinks() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_MULTIPLE_PLUGINS);
    }

    public static PipelineConfiguration pipelineConfigurationWithNoSinks() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_EMPTY_PLUGINS);
    }

    public static PipelineConfiguration pipelineConfigurationWithMultipleSinksSomeInvalid() {
        return new PipelineConfiguration(
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                VALID_CONFIGURATION_WITH_SINGLE_PLUGIN,
                CONFIGURATION_WITH_MULTIPLE_PLUGINS_SOME_INVALID);
    }

    public static PipelineConfiguration pipelineConfigurationWithAllInvalidPlugins() {
        return new PipelineConfiguration(
                CONFIGURATION_WITH_NULL_NAME,
                CONFIGURATION_WITH_NULL_NAME,
                CONFIGURATION_WITH_NULL_NAME,
                CONFIGURATION_WITH_NULL_NAME);
    }

    public static PipelineConfiguration pipelineConfigurationWithEmptyConfigurations() {
        return new PipelineConfiguration(
                CONFIGURATION_WITH_EMPTY_PLUGINS,
                CONFIGURATION_WITH_EMPTY_PLUGINS,
                CONFIGURATION_WITH_EMPTY_PLUGINS,
                CONFIGURATION_WITH_EMPTY_PLUGINS);
    }

    public static String readConfigurationFileContent(final String configurationFile) throws Exception {
        if (configurationFile == null) {
            return ""; //return empty if the configuration file is absent
        }
        final StringBuilder stringBuilder = new StringBuilder();
        try (InputStream configurationInputStream = Objects.requireNonNull(TestDataProvider.class.getClassLoader()
                .getResourceAsStream(configurationFile));
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(configurationInputStream))) {
            bufferedReader.lines().forEach(stringBuilder::append);
        }
        return stringBuilder.toString();
    }

    private static Map<String, Object> validSettingsForPlugin() {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("someProperty", "someValue");
        return settingsMap;
    }
}