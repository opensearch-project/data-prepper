/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.SinkModel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConfigurationProvider {
    public static final String TEST_PIPELINE_NAME = "test-pipeline-1";
    public static final String TEST_PLUGIN_NAME_1 = "test-plugin-1";
    public static final String TEST_PLUGIN_NAME_2 = "test-plugin-2";
    public static final Integer TEST_WORKERS = 5;
    public static final Integer DEFAULT_WORKERS = 1;
    public static final Integer DEFAULT_READ_BATCH_DELAY = 3_000;
    public static final Integer TEST_DELAY = 3_000;
    public static final String VALID_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/valid_multiple_pipeline_configuration.yml";
    public static final String VALID_PIPELINE_CONFIG_FILE_WITH_DEPRECATED_EXTENSIONS = "src/test/resources/valid_pipeline_configuration_with_deprecated_extensions.yml";
    public static final String VALID_PIPELINE_CONFIG_FILE_WITH_EXTENSION = "src/test/resources/valid_pipeline_configuration_with_extension.yml";
    public static final String MULTI_FILE_PIPELINE_DIRECTOTRY = "src/test/resources/multi-pipelines";
    public static final String MULTI_FILE_PIPELINE_WITH_DISTRIBUTED_PIPELINE_CONFIGURATIONS_DIRECTOTRY = "src/test/resources/multi-pipelines-distributed-pipeline-configurations";
    public static final String MULTI_FILE_PIPELINE_WITH_SINGLE_PIPELINE_CONFIGURATIONS_DIRECTOTRY = "src/test/resources/multi-pipelines-single-pipeline-configurations";
    public static final String SINGLE_FILE_PIPELINE_DIRECTOTRY = "src/test/resources/single-pipeline";
    public static final String EMPTY_PIPELINE_DIRECTOTRY = "src/test/resources/no-pipelines";
    public static final String INCOMPATIBLE_VERSION_CONFIG_FILE = "src/test/resources/incompatible_version.yml";


    public static Set<String> VALID_MULTIPLE_PIPELINE_NAMES = new HashSet<>(Arrays.asList("test-pipeline-1",
            "test-pipeline-2", "test-pipeline-3"));
    public static PluginSetting VALID_PLUGIN_SETTING_1 = new PluginSetting(TEST_PLUGIN_NAME_1, validSettingsForPlugin());
    public static PluginSetting VALID_PLUGIN_SETTING_2 = new PluginSetting(TEST_PLUGIN_NAME_2, validSettingsForPlugin());

    private static Map<String, Object> validSettingsForPlugin() {
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("someProperty", "someValue");
        return settingsMap;
    }

    public static PluginModel validSingleConfiguration() {
        return validPluginModel(TEST_PLUGIN_NAME_1);
    }

    private static PluginModel validPluginModel(final String pluginName) {
        final PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn(pluginName);
        when(pluginModel.getPluginSettings()).thenReturn(validSettingsForPlugin());
        return pluginModel;
    }

    private static SinkModel validSinkModel(final String pluginName) {
        final SinkModel sinkModel = mock(SinkModel.class);
        when(sinkModel.getPluginName()).thenReturn(pluginName);
        when(sinkModel.getPluginSettings()).thenReturn(validSettingsForPlugin());
        when(sinkModel.getRoutes()).thenReturn(Collections.emptyList());
        return sinkModel;
    }

    public static List<PluginModel> validMultipleConfiguration() {
        return Arrays.asList(validPluginModel(TEST_PLUGIN_NAME_1), validPluginModel(TEST_PLUGIN_NAME_2));
    }

    public static List<SinkModel> validMultipleSinkConfiguration() {
        return Arrays.asList(validSinkModel(TEST_PLUGIN_NAME_1), validSinkModel(TEST_PLUGIN_NAME_2));
    }

    public static List<PluginModel> validMultipleConfigurationOfSizeOne() {
        return Collections.singletonList(validSingleConfiguration());
    }

    public static List<SinkModel> validMultipleSinkConfigurationOfSizeOne() {
        return Collections.singletonList(validSinkModel(TEST_PLUGIN_NAME_1));
    }

}
