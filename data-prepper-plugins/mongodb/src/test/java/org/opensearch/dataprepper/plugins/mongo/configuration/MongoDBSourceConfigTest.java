/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MongoDBSourceConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void username_password_only() throws JsonProcessingException {
        final String configYaml =
                "host: \"localhost\"\n" +
                "authentication:\n" +
                "  username: test\n" +
                "  password: test\n" +
                "collections:\n" +
                "  - collection: test\n";

        final MongoDBSourceConfig config = objectMapper.readValue(configYaml, MongoDBSourceConfig.class);

        config.validateAwsConfigWithUsernameAndPassword();
        assertThat(config.getAuthenticationConfig(), notNullValue());
        assertThat(config.getAuthenticationConfig().getUsername(), equalTo("test"));
        assertThat(config.getAuthenticationConfig().getPassword(), equalTo("test"));
        assertThat(config.getAwsConfig(), nullValue());
    }

    @Test
    void aws_sts_role_arn_only() throws JsonProcessingException {
        final String configYaml =
                "host: \"localhost\"\n" +
                "aws:\n" +
                "  sts_role_arn: \"arn:aws:iam::123456789012:role/test-role\"\n" +
                "collections:\n" +
                "  - collection: test\n";

        final MongoDBSourceConfig config = objectMapper.readValue(configYaml, MongoDBSourceConfig.class);

        config.validateAwsConfigWithUsernameAndPassword();
        assertThat(config.getAwsConfig(), notNullValue());
        assertThat(config.getAwsConfig().getAwsStsRoleArn(), equalTo("arn:aws:iam::123456789012:role/test-role"));
        assertThat(config.getAuthenticationConfig(), nullValue());
    }

    @Test
    void both_username_password_and_aws_is_invalid() throws JsonProcessingException {
        final String configYaml =
                "host: \"localhost\"\n" +
                "authentication:\n" +
                "  username: test\n" +
                "  password: test\n" +
                "aws:\n" +
                "  sts_role_arn: \"arn:aws:iam::123456789012:role/test-role\"\n" +
                "collections:\n" +
                "  - collection: test\n";

        final MongoDBSourceConfig config = objectMapper.readValue(configYaml, MongoDBSourceConfig.class);
        assertThrows(IllegalArgumentException.class, config::validateAwsConfigWithUsernameAndPassword);
    }

    @Test
    void neither_username_password_nor_aws_is_invalid() throws JsonProcessingException {
        final String configYaml =
                "host: \"localhost\"\n" +
                "collections:\n" +
                "  - collection: test\n";

        final MongoDBSourceConfig config = objectMapper.readValue(configYaml, MongoDBSourceConfig.class);
        assertThrows(IllegalArgumentException.class, config::validateAwsConfigWithUsernameAndPassword);
    }
}
