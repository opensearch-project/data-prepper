/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNull;

class DynamoDBSourceConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void test_general_config() throws JsonProcessingException {

        final String sourceConfigurationYaml = "tables:\n" +
                "  - table_arn: \"arn:aws:dynamodb:us-west-2:123456789012:table/table-a\"\n" +
                "    export:\n" +
                "      s3_bucket: \"test-bucket\"\n" +
                "      s3_prefix: \"xxx/\"\n" +
                "    stream:\n" +
                "      start_position:  \n" +
                "  - table_arn: \"arn:aws:dynamodb:us-west-2:123456789012:table/table-b\"\n" +
                "    export:\n" +
                "      s3_bucket: \"test-bucket\"\n" +
                "      s3_prefix: \"xxx/\"\n" +
                "  - table_arn: \"arn:aws:dynamodb:us-west-2:123456789012:table/table-c\"\n" +
                "    stream:\n" +
                "      start_position: \"BEGINNING\"  \n" +
                "aws:\n" +
                "  region: \"us-west-2\"\n" +
                "  sts_role_arn: \"arn:aws:iam::123456789012:role/DataPrepperRole\"\n" +
                "coordinator:\n" +
                "  dynamodb:\n" +
                "    table_name: \"coordinator-table\"\n" +
                "    region: \"us-west-2\"";
        final DynamoDBSourceConfig sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, DynamoDBSourceConfig.class);

        assertThat(sourceConfiguration.getAwsAuthenticationConfig(), notNullValue());
        assertThat(sourceConfiguration.getCoordinationStoreConfig(), notNullValue());
        assertThat(sourceConfiguration.getTableConfigs(), notNullValue());
        assertThat(sourceConfiguration.getTableConfigs().size(), equalTo(3));

        TableConfig exportAndStreamConfig = sourceConfiguration.getTableConfigs().get(0);
        assertThat(exportAndStreamConfig.getExportConfig(), notNullValue());
        assertThat(exportAndStreamConfig.getExportConfig().getS3Bucket(), equalTo("test-bucket"));
        assertThat(exportAndStreamConfig.getExportConfig().getS3Prefix(), equalTo("xxx/"));
        assertThat(exportAndStreamConfig.getStreamConfig(), notNullValue());
        assertNull(exportAndStreamConfig.getStreamConfig().getStartPosition());


        TableConfig exportOnlyConfig = sourceConfiguration.getTableConfigs().get(1);
        assertThat(exportOnlyConfig.getExportConfig(), notNullValue());
        assertThat(exportOnlyConfig.getExportConfig().getS3Bucket(), equalTo("test-bucket"));
        assertThat(exportOnlyConfig.getExportConfig().getS3Prefix(), equalTo("xxx/"));
        assertNull(exportOnlyConfig.getStreamConfig());


        TableConfig streamOnlyConfig = sourceConfiguration.getTableConfigs().get(2);
        assertThat(streamOnlyConfig.getStreamConfig(), notNullValue());
        assertThat(streamOnlyConfig.getStreamConfig().getStartPosition(), equalTo("BEGINNING"));
        assertNull(streamOnlyConfig.getExportConfig());

        AwsAuthenticationConfig awsAuthenticationConfig = sourceConfiguration.getAwsAuthenticationConfig();
        assertThat(awsAuthenticationConfig, notNullValue());
        assertThat(awsAuthenticationConfig.getAwsRegion(), equalTo(Region.US_WEST_2));
        assertThat(awsAuthenticationConfig.getAwsStsRoleArn(), equalTo("arn:aws:iam::123456789012:role/DataPrepperRole"));
        assertNull(awsAuthenticationConfig.getAwsStsExternalId());
        assertNull(awsAuthenticationConfig.getAwsStsHeaderOverrides());

    }

}