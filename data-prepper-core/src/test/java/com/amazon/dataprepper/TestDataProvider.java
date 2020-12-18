package com.amazon.dataprepper;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.parser.model.PipelineConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestDataProvider {
    public static final String TEST_PIPELINE_NAME = "test-pipeline-1";
    public static final String TEST_PLUGIN_NAME_1 = "test-plugin-1";
    public static final String TEST_PLUGIN_NAME_2 = "test-plugin-2";
    public static final Integer TEST_WORKERS = 5;
    public static final Integer DEFAULT_WORKERS = 1;
    public static final Integer DEFAULT_READ_BATCH_DELAY = 3_000;
    public static final Integer TEST_DELAY = 3_000;
    public static final String VALID_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/valid_multiple_pipeline_configuration.yml";
    public static final String VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE = "src/test/resources/single_pipeline_valid_empty_source_plugin_settings.yml";
    public static final String CONNECTED_PIPELINE_ROOT_SOURCE_INCORRECT = "src/test/resources/connected_pipeline_incorrect_root_source.yml";
    public static final String CONNECTED_PIPELINE_CHILD_PIPELINE_INCORRECT = "src/test/resources/connected_pipeline_incorrect_child_pipeline.yml";
    public static final String CYCLE_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/cyclic_multiple_pipeline_configuration.yml";
    public static final String INCORRECT_SOURCE_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/incorrect_source_multiple_pipeline_configuration.yml";
    public static final String MISSING_NAME_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/missing_name_multiple_pipeline_configuration.yml";
    public static final String MISSING_PIPELINE_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/missing_pipeline_multiple_pipeline_configuration.yml";
    public static final String VALID_MULTIPLE_SINKS_CONFIG_FILE = "src/test/resources/valid_multiple_sinks.yml";
    public static final String VALID_MULTIPLE_PROCESSORS_CONFIG_FILE = "src/test/resources/valid_multiple_sinks.yml";
    public static final String NO_PIPELINES_EXECUTE_CONFIG_FILE = "src/test/resources/no_pipelines_to_execute.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE = "src/test/resources/valid_data_prepper_config.yml";
    public static final String VALID_DATA_PREPPER_DEFAULT_LOG4J_CONFIG_FILE = "src/test/resources/valid_data_prepper_config_default_log4j.yml";
    public static final String VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE = "src/test/resources/valid_data_prepper_some_default_config.yml";
    public static final String INVALID_DATA_PREPPER_CONFIG_FILE = "src/test/resources/invalid_data_prepper_config.yml";


    public static Set<String> VALID_MULTIPLE_PIPELINE_NAMES = new HashSet<>(Arrays.asList("test-pipeline-1",
            "test-pipeline-2", "test-pipeline-3"));
    public static PluginSetting VALID_PLUGIN_SETTING_1 = new PluginSetting(TEST_PLUGIN_NAME_1, validSettingsForPlugin());
    public static PluginSetting VALID_PLUGIN_SETTING_2 = new PluginSetting(TEST_PLUGIN_NAME_2, validSettingsForPlugin());

    private static Map<String, Object> validSettingsForPlugin() {
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("someProperty", "someValue");
        return settingsMap;
    }

    public static Map.Entry<String, Map<String, Object>> validSingleConfiguration() {
        return Map.entry(TEST_PLUGIN_NAME_1, validSettingsForPlugin());
    }

    public static List<Map.Entry<String, Map<String, Object>>> validMultipleConfiguration() {
        return Arrays.asList(Map.entry(TEST_PLUGIN_NAME_1, validSettingsForPlugin()), Map.entry(TEST_PLUGIN_NAME_2, validSettingsForPlugin()));
    }

    public static List<Map.Entry<String, Map<String, Object>>> validMultipleConfigurationOfSizeOne() {
        return Collections.singletonList(Map.entry(TEST_PLUGIN_NAME_1, validSettingsForPlugin()));
    }

    public static Map<String, PipelineConfiguration> readConfigFile(final String configFilePath) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory())
                .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        return objectMapper.readValue(
                new File(configFilePath),
                new TypeReference<Map<String, PipelineConfiguration>>() {
                });
    }

}