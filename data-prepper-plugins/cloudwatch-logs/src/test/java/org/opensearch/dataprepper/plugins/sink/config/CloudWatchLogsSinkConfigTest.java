/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

class CloudWatchLogsSinkConfigTest {
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private AwsConfig awsConfig;
    private ThresholdConfig thresholdConfig;
    private final String LOG_GROUP = "testLogGroup";
    private final String LOG_STREAM = "testLogStream";

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = new CloudWatchLogsSinkConfig();
        awsConfig = new AwsConfig();
        thresholdConfig = new ThresholdConfig();
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_aws_config_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getAwsConfig(), equalTo(null));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_threshold_config_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getThresholdConfig(), notNullValue());
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_buffer_type_called_SHOULD_return_default_buffer_type() {
        assertThat(new CloudWatchLogsSinkConfig().getBufferType(), equalTo(CloudWatchLogsSinkConfig.DEFAULT_BUFFER_TYPE));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_log_group_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getLogGroup(), equalTo(null));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_log_stream_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getLogStream(), equalTo(null));
    }

    @Test
    void GIVEN_empty_sink_config_WHEN_deserialized_from_json_SHOULD_return_valid_log_group_and_log_stream() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logGroup", LOG_GROUP);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logStream", LOG_STREAM);

        assertThat(cloudWatchLogsSinkConfig.getLogGroup(), equalTo(LOG_GROUP));
        assertThat(cloudWatchLogsSinkConfig.getLogStream(), equalTo(LOG_STREAM));
    }

    @Test
    void GIVEN_empty_sink_config_WHEN_deserialized_from_json_SHOULD_return_valid_threshold_config_and_aws_config() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "thresholdConfig", thresholdConfig);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "awsConfig", awsConfig);

        assertThat(cloudWatchLogsSinkConfig.getAwsConfig(), equalTo(awsConfig));
        assertThat(cloudWatchLogsSinkConfig.getThresholdConfig(), equalTo(thresholdConfig));
    }
}