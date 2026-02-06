/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestDataProvider {
    public static final String VALID_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/valid_multiple_pipeline_configuration.yml";
    public static final String VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE = "src/test/resources/single_pipeline_valid_empty_source_plugin_settings.yml";
    public static final String VALID_OFF_HEAP_FILE = "src/test/resources/single_pipeline_valid_off_heap_buffer.yml";
    public static final String VALID_OFF_HEAP_FILE_WITH_ACKS = "src/test/resources/multiple_pipeline_valid_off_heap_buffer_with_acks.yml";
    public static final String DISCONNECTED_VALID_OFF_HEAP_FILE_WITH_ACKS = "src/test/resources/multiple_disconnected_pipeline_valid_off_heap_buffer_with_acks.yml";
    public static final String CONNECTED_PIPELINE_ROOT_SOURCE_INCORRECT = "src/test/resources/connected_pipeline_incorrect_root_source.yml";
    public static final String CONNECTED_PIPELINE_BUFFER_INCORRECT = "src/test/resources/connected_pipeline_incorrect_buffer.yml";
    public static final String CONNECTED_PIPELINE_PROCESSOR_INCORRECT = "src/test/resources/connected_pipeline_incorrect_processor.yml";
    public static final String CONNECTED_PIPELINE_SINK_INCORRECT = "src/test/resources/connected_pipeline_incorrect_sink.yml";
    public static final String CONNECTED_PIPELINE_CHILD_PIPELINE_INCORRECT_DUE_TO_SINK = "src/test/resources/connected_pipeline_incorrect_child_pipeline_due_to_invalid_sink.yml";
    public static final String CONNECTED_PIPELINE_CHILD_PIPELINE_INCORRECT_DUE_TO_PROCESSOR = "src/test/resources/connected_pipeline_incorrect_child_pipeline_due_to_invalid_processor.yml";
    public static final String CYCLE_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/cyclic_multiple_pipeline_configuration.yml";
    public static final String INCORRECT_SOURCE_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/incorrect_source_multiple_pipeline_configuration.yml";
    public static final String MISSING_NAME_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/missing_name_multiple_pipeline_configuration.yml";
    public static final String MISSING_SOURCE_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/missing_source_multiple_pipeline_configuration.yml";
    public static final String MISSING_PIPELINE_MULTIPLE_PIPELINE_CONFIG_FILE = "src/test/resources/missing_pipeline_multiple_pipeline_configuration.yml";
    public static final String VALID_MULTIPLE_SINKS_CONFIG_FILE = "src/test/resources/valid_multiple_sinks.yml";
    public static final String VALID_MULTIPLE_SINKS_WITH_FAILURE_PIPELINE_CONFIG_FILE = "src/test/resources/valid_multiple_sinks_with_routes_with_failure_pipeline.yml";
    public static final String COMPATIBLE_VERSION_CONFIG_FILE = "src/test/resources/compatible_version.yml";
    public static final String VALID_MULTIPLE_PROCESSERS_CONFIG_FILE = "src/test/resources/valid_multiple_processors.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE = "src/test/resources/valid_data_prepper_config.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_BASIC_AUTHENTICATION = "src/test/resources/valid_data_prepper_config_with_basic_authentication.yml";
    public static final String VALID_DATA_PREPPER_CONFIG_FILE_WITH_CAMEL_CASE_OPTIONS = "src/test/resources/valid_data_prepper_config_with_camel_case_options.yml";
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
    public static final String VALID_ZERO_BUFFER_SINGLE_THREAD_CONFIG_FILE = "src/test/resources/valid_zero_buffer_single_thread.yml";
    public static final String INVALID_ZERO_BUFFER_MULTIPLE_THREADS_CONFIG_FILE = "src/test/resources/invalid_zero_buffer_multiple_threads.yml";
    public static final String INVALID_ZERO_BUFFER_WITH_SINGLE_THREAD_PROCESSOR_CONFIG_FILE = "src/test/resources/invalid_zero_buffer_with_single_thread_processor.yml";
    public static final String INVALID_ZERO_BUFFER_MULTIPLE_THREADS_NO_SINGLE_THREAD_PROCESSORS_CONFIG_FILE = "src/test/resources/invalid_zero_buffer_multiple_threads_no_single_thread_processors.yml";
    public static Set<String> VALID_MULTIPLE_PIPELINE_NAMES = new HashSet<>(Arrays.asList("test-pipeline-1",
            "test-pipeline-2", "test-pipeline-3"));
    public static final String INVALID_ONLY_HEADLESS_AND_SUBPIPELINES_CONFIG_FILE = "src/test/resources/invalid_only_headless_and_subpipelines_config.yml";
    public static final String INVALID_ONLY_HEADLESS_PIPELINES_CONFIG_FILE = "src/test/resources/invalid_only_headless_pipelines_config.yml";
    public static final String VALID_FORWARD_PIPELINE_CONFIG_FILE = "src/test/resources/valid_forward_pipeline.yml";
    public static final String VALID_PIPELINE_DLQ_CONFIG_FILE = "src/test/resources/valid_pipeline_dlq.yml";

}
