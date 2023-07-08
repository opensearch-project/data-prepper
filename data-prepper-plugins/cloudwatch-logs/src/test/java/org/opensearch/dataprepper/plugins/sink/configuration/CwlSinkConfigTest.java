/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CwlSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class CwlSinkConfigTest {
    private CwlSinkConfig cwlSinkConfig;
    private AwsConfig awsConfig;
    private ThresholdConfig thresholdConfig;
    private final String LOG_GROUP = "testLogGroup";
    private final String LOG_STREAM = "testLogStream";

    @BeforeEach
    void setUp() {
        cwlSinkConfig = new CwlSinkConfig();
        awsConfig = new AwsConfig();
        thresholdConfig = new ThresholdConfig();
    }

    @Test
    void check_null_auth_config_test() {
        assertThat(new CwlSinkConfig().getAwsConfig(), equalTo(null));
    }

    @Test
    void check_null_threshold_config_test() {
        assertThat(new CwlSinkConfig().getThresholdConfig(), notNullValue());
    }

    @Test
    void check_default_buffer_type_test() {
        assertThat(new CwlSinkConfig().getBufferType(), equalTo(CwlSinkConfig.DEFAULT_BUFFER_TYPE));
    }

    @Test
    void check_null_log_group_test() {
        assertThat(new CwlSinkConfig().getLogGroup(), equalTo(null));
    }

    @Test
    void check_null_log_stream_test() {
        assertThat(new CwlSinkConfig().getLogStream(), equalTo(null));
    }

    @Test
    void check_valid_log_group_and_log_stream_test() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(cwlSinkConfig.getClass(), cwlSinkConfig, "logGroup", LOG_GROUP);
        ReflectivelySetField.setField(cwlSinkConfig.getClass(), cwlSinkConfig, "logStream", LOG_STREAM);

        assertThat(cwlSinkConfig.getLogGroup(), equalTo(LOG_GROUP));
        assertThat(cwlSinkConfig.getLogStream(), equalTo(LOG_STREAM));
    }

    @Test
    void check_valid_sub_config_test() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(cwlSinkConfig.getClass(), cwlSinkConfig, "thresholdConfig", thresholdConfig);
        ReflectivelySetField.setField(cwlSinkConfig.getClass(), cwlSinkConfig, "awsConfig", awsConfig);

        assertThat(cwlSinkConfig.getAwsConfig(), equalTo(awsConfig));
        assertThat(cwlSinkConfig.getThresholdConfig(), equalTo(thresholdConfig));
    }
}