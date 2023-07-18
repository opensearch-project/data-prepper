/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.configuration.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class SNSSinkConfigTest {
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final String DEFAULT_BYTE_SIZE = "50mb";
    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void sns_sink_default_id_test(){
        assertThat(new SNSSinkConfig().getId(),nullValue());
    }

    @Test
    void sns_sink_default_topic_test() {
        assertThat(new SNSSinkConfig().getTopicArn(),nullValue());
    }

    @Test
    void sns_sink_default_codec_test(){
        assertThat(new SNSSinkConfig().getCodec(),nullValue());
    }

    @Test
    void sns_sink_default_max_retries_test(){
        assertThat(new SNSSinkConfig().getMaxUploadRetries(),equalTo(DEFAULT_MAX_RETRIES));
    }

    @Test
    void sns_sink_default_threshold_byte_and_event_count_test() throws JsonProcessingException {
        final String thresholdOptionTest = "          event_count: 20\n";
        final ThresholdOptions thresholdOptions = objectMapper.readValue(thresholdOptionTest, ThresholdOptions.class);
        assertThat(thresholdOptions.getMaximumSize().getBytes(),equalTo(ByteCount.parse(DEFAULT_BYTE_SIZE).getBytes()));
        assertThat(thresholdOptions.getEventCount(),equalTo(20));
        assertThat(thresholdOptions.getEventCollectTimeOut(),nullValue());
    }

    @Test
    void sns_sink_default_buffer_test(){
        assertThat(new SNSSinkConfig().getBufferType(),equalTo(BufferTypeOptions.IN_MEMORY));
    }

    @Test
    void sns_sink_default_max_upload_retries_test(){
        assertThat(new SNSSinkConfig().getMaxUploadRetries(),equalTo(DEFAULT_MAX_RETRIES));
    }

    @Test
    void sns_sink_default_max_connection_retries_test(){
        assertThat(new SNSSinkConfig().getMaxConnectionRetries(),equalTo(DEFAULT_MAX_RETRIES));
    }


    @Test
    void sns_sink_pipeline_config_test() throws JsonProcessingException {
        final String config = "        topic: arn:aws:sns:ap-south-1:524239988912:my-topic\n" +
                "        id: test\n" +
                "        aws:\n" +
                "          region: ap-south-1\n" +
                "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                "          sts_header_overrides: {\"test\":\"test\"}\n" +
                "        codec:\n" +
                "          ndjson:\n" +
                "        max_retries: 10\n" +
                "        dlq_file: /test/dlq-file.log\n" +
                "        dlq:\n" +
                "          s3:\n" +
                "            bucket: test\n" +
                "            key_path_prefix: test\n" +
                "            region: ap-south-1\n" +
                "            sts_role_arn: test-role-arn\n" +
                "        threshold:\n" +
                "          event_count: 2000\n" +
                "          maximum_size: 100mb\n" +
                "        buffer_type: local_file";
        final SNSSinkConfig snsSinkConfig = objectMapper.readValue(config, SNSSinkConfig.class);
        assertThat(snsSinkConfig.getMaxUploadRetries(),equalTo(10));
        assertThat(snsSinkConfig.getTopicArn(),equalTo("arn:aws:sns:ap-south-1:524239988912:my-topic"));
        assertThat(snsSinkConfig.getId(),equalTo("test"));
        assertThat(snsSinkConfig.getAwsAuthenticationOptions().getAwsRegion(),equalTo(Region.AP_SOUTH_1));
        assertThat(snsSinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(),equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(snsSinkConfig.getThresholdOptions().getEventCount(),equalTo(2000));
        assertThat(snsSinkConfig.getThresholdOptions().getMaximumSize().getBytes(),equalTo(ByteCount.parse("100mb").getBytes()));
        assertThat(snsSinkConfig.getBufferType(),equalTo(BufferTypeOptions.LOCAL_FILE));
        assertThat(snsSinkConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(snsSinkConfig.getDlqStsRegion(),equalTo("ap-south-1"));
        assertThat(snsSinkConfig.getDlqStsRoleARN(),equalTo("test-role-arn"));
    }
}