/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.parser.model.PipelineConfiguration;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    public static final String MULTI_FILE_PIPELINE_DIRECTOTRY = "src/test/resources/multi-pipelines";
    public static final String SINGLE_FILE_PIPELINE_DIRECTOTRY = "src/test/resources/single-pipeline";
    public static final String EMPTY_PIPELINE_DIRECTOTRY = "src/test/resources/no-pipelines";
    public static final String VALID_MULTIPLE_SINKS_CONFIG_FILE = "src/test/resources/valid_multiple_sinks.yml";
    public static final String INCOMPATIBLE_VERSION_CONFIG_FILE = "src/test/resources/incompatible_version.yml";
    public static final String COMPATIBLE_VERSION_CONFIG_FILE = "src/test/resources/compatible_version.yml";
    public static final String VALID_MULTIPLE_PROCESSERS_CONFIG_FILE = "src/test/resources/valid_multiple_processors.yml";
    public static final String NO_PIPELINES_EXECUTE_CONFIG_FILE = "src/test/resources/no_pipelines_to_execute.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE = "src/test/resources/valid_data_prepper_config.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_TLS = "src/test/resources/valid_data_prepper_config_with_tls.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_BASIC_AUTHENTICATION = "src/test/resources/valid_data_prepper_config_with_basic_authentication.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_CAMEL_CASE_OPTIONS = "src/test/resources/valid_data_prepper_config_with_camel_case_options.yml";
    public static final String VALID_DATA_PREPPER_DEFAULT_LOG4J_CONFIG_FILE = "src/test/resources/valid_data_prepper_config_default_log4j.yml";
    public static final String VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE = "src/test/resources/valid_data_prepper_some_default_config.yml";
    public static final String VALID_DATA_PREPPER_CLOUDWATCH_METRICS_CONFIG_FILE = "src/test/resources/valid_data_prepper_cloudwatch_metrics_config.yml";
    public static final String VALID_DATA_PREPPER_MULTIPLE_METRICS_CONFIG_FILE = "src/test/resources/valid_data_prepper_multiple_metrics_config.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_TAGS = "src/test/resources/valid_data_prepper_config_with_tags.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_PROCESSOR_SHUTDOWN_TIMEOUT = "src/test/resources/valid_data_prepper_config_with_processor_shutdown_timeout.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_SINK_SHUTDOWN_TIMEOUT = "src/test/resources/valid_data_prepper_config_with_sink_shutdown_timeout.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_ISO8601_SHUTDOWN_TIMEOUTS = "src/test/resources/valid_data_prepper_config_with_iso8601_shutdown_timeouts.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_SOURCE_COORDINATION = "src/test/resources/valid_data_prepper_source_coordination_config.yml";
    public static final String INVALID_DATA_PREPPER_CONFIG_FILE = "src/test/resources/invalid_data_prepper_config.yml";
    public static final String INVALID_DATA_PREPPER_CONFIG_FILE_WITH_TAGS = "src/test/resources/invalid_data_prepper_config_with_tags.yml";
    public static final String INVALID_DATA_PREPPER_CONFIG_FILE_WITH_BAD_PROCESSOR_SHUTDOWN_TIMEOUT = "src/test/resources/invalid_data_prepper_config_with_bad_processor_shutdown_timeout.yml";
    public static final String INVALID_DATA_PREPPER_CONFIG_FILE_WITH_BAD_SINK_SHUTDOWN_TIMEOUT = "src/test/resources/invalid_data_prepper_config_with_bad_sink_shutdown_timeout.yml";
    public static final String INVALID_DATA_PREPPER_CONFIG_FILE_WITH_NEGATIVE_PROCESSOR_SHUTDOWN_TIMEOUT = "src/test/resources/invalid_data_prepper_config_with_negative_processor_shutdown_timeout.yml";
    public static final String INVALID_DATA_PREPPER_CONFIG_FILE_WITH_NEGATIVE_SINK_SHUTDOWN_TIMEOUT = "src/test/resources/invalid_data_prepper_config_with_negative_sink_shutdown_timeout.yml";
    public static final String INVALID_PORT_DATA_PREPPER_CONFIG_FILE = "src/test/resources/invalid_port_data_prepper_config.yml";
    public static final String INVALID_KEYSTORE_PASSWORD_DATA_PREPPER_CONFIG_FILE = "src/test/resources/invalid_data_prepper_config_with_bad_keystore_password.yml";
    public static final String VALID_PEER_FORWARDER_DATA_PREPPER_CONFIG_FILE = "src/test/resources/valid_data_prepper_config_wth_peer_forwarder_config.yml";
    public static final String VALID_PEER_FORWARDER_CONFIG_WITHOUT_SSL_FILE = "src/test/resources/valid_peer_forwarder_without_ssl_config.yml";
    public static final String VALID_PEER_FORWARDER_CONFIG_FILE = "src/test/resources/valid_peer_forwarder_config.yml";
    public static final String VALID_PEER_FORWARDER_CONFIG_WITH_DRAIN_TIMEOUT_FILE = "src/test/resources/valid_peer_forwarder_config_with_drain_timeout.yml";
    public static final String VALID_PEER_FORWARDER_CONFIG_WITH_ISO8601_DRAIN_TIMEOUT_FILE = "src/test/resources/valid_peer_forwarder_config_with_iso8601_drain_timeout.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_WITH_TEST_EXTENSION_FILE = "src/test/resources/valid_data_prepper_config_with_test_extension.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_PORT_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_port_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_THREAD_COUNT_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_thread_count_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_CONNECTION_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_connection_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_SSL_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_ssl_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_DISCOVERY_MODE_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_discovery_mode_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_BUFFER_SIZE_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_buffer_size_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_BATCH_SIZE_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_batch_size_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_ACM_WITHOUT_ARN_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_acm_without_arn_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_ACM_WITHOUT_REGION_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_acm_without_region_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_SERVICE_NAME_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_cloud_map_without_service_name_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_NAMESPACE_NAME_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_cloud_map_without_namespace_name_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_REGION_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_cloud_map_without_region_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_DNS_WITHOUT_DOMAIN_NAME_CONFIG_FILE = "src/test/resources/invalid_peer_forwarder_with_dns_without_domain_name_config.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_BAD_DRAIN_TIMEOUT = "src/test/resources/invalid_peer_forwarder_with_bad_drain_timeout.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_NEGATIVE_DRAIN_TIMEOUT = "src/test/resources/invalid_peer_forwarder_with_negative_drain_timeout.yml";
    public static final String INVALID_PEER_FORWARDER_WITH_ZERO_LOCAL_WRITE_TIMEOUT = "src/test/resources/invalid_peer_forwarder_with_zero_local_write_timeout.yml";
    public static final String VALID_PEER_FORWARDER_WITH_ACM_SSL_CONFIG_FILE = "src/test/resources/valid_peer_forwarder_config_with_acm_ssl.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_WITH_METRIC_FILTER = "src/test/resources/valid_data_prepper_config_with_metric_filter.yml";
    public static final String INVALID_DATA_PREPPER_CONFIG_WITH_METRIC_FILTER = "src/test/resources/invalid_data_prepper_config_with_metric_filter.yml";




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

    public static Map<String, PipelineConfiguration> readConfigFile(final String configFilePath) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory())
                .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        return objectMapper.readValue(
                new File(configFilePath),
                new TypeReference<Map<String, PipelineConfiguration>>() {
                });
    }

}
