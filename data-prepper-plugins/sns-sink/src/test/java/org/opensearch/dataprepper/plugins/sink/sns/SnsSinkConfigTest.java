/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.sns;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class SnsSinkConfigTest {
    public static final int DEFAULT_MAX_RETRIES = 5;
    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void sns_sink_default_id_test(){
        assertThat(new SnsSinkConfig().getMessageGroupId(),nullValue());
    }

    @Test
    void sns_sink_default_topic_test() {
        assertThat(new SnsSinkConfig().getTopicArn(),nullValue());
    }

    @Test
    void sns_sink_default_codec_test(){
        assertThat(new SnsSinkConfig().getCodec(),nullValue());
    }

    @Test
    void sns_sink_default_max_retries_test(){
        assertThat(new SnsSinkConfig().getMaxUploadRetries(),equalTo(DEFAULT_MAX_RETRIES));
    }

    @Test
    void sns_sink_default_max_upload_retries_test(){
        assertThat(new SnsSinkConfig().getMaxUploadRetries(),equalTo(DEFAULT_MAX_RETRIES));
    }

    @Test
    void sns_sink_default_max_connection_retries_test(){
        assertThat(new SnsSinkConfig().getMaxConnectionRetries(),equalTo(DEFAULT_MAX_RETRIES));
    }


    @Test
    void sns_sink_pipeline_config_test() throws JsonProcessingException {
        final String config = "        topic_arn: arn:aws:sns:ap-south-1:524239988912:my-topic\n" +
                "        message_group_id: test\n" +
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
                "            sts_role_arn: test-role-arn\n";
        final SnsSinkConfig snsSinkConfig = objectMapper.readValue(config, SnsSinkConfig.class);
        assertThat(snsSinkConfig.getMaxUploadRetries(),equalTo(10));
        assertThat(snsSinkConfig.getTopicArn(),equalTo("arn:aws:sns:ap-south-1:524239988912:my-topic"));
        assertThat(snsSinkConfig.getMessageGroupId(),equalTo("test"));
        assertThat(snsSinkConfig.getAwsAuthenticationOptions().getAwsRegion(),equalTo(Region.AP_SOUTH_1));
        assertThat(snsSinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(),equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(snsSinkConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(snsSinkConfig.getDlqStsRegion(),equalTo("ap-south-1"));
        assertThat(snsSinkConfig.getDlqStsRoleARN(),equalTo("test-role-arn"));
    }
}