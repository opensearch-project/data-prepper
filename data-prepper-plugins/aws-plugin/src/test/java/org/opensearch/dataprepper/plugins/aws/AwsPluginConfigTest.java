/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AwsPluginConfigTest {

    @Test
    void testDefault() {
        final AwsPluginConfig objectUnderTest = new AwsPluginConfig();

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getDefaultStsConfiguration(), notNullValue());
        assertThat(objectUnderTest.getDefaultStsConfiguration().getAwsRegion(), nullValue());
        assertThat(objectUnderTest.getDefaultStsConfiguration().getAwsStsRoleArn(), nullValue());
    }

    @Test
    void testNamedConfiguration_via_json() throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final String json = "{\"default\": {\"region\": \"us-east-1\", \"sts_role_arn\": \"arn:aws:iam::123456789012:role/DefaultRole\"}, " +
                "\"ecs_task_role\": {\"use_aws_sdk_default\": true}}";

        final AwsPluginConfig objectUnderTest = objectMapper.readValue(json, AwsPluginConfig.class);

        assertThat(objectUnderTest.getDefaultStsConfiguration(), notNullValue());
        assertThat(objectUnderTest.getDefaultStsConfiguration().getAwsStsRoleArn(), equalTo("arn:aws:iam::123456789012:role/DefaultRole"));

        assertThat(objectUnderTest.getConfiguration("ecs_task_role"), notNullValue());
        assertTrue(objectUnderTest.getConfiguration("ecs_task_role").isUseAwsSdkDefault());
    }

    @Test
    void testNamedConfiguration_not_found_returns_null() {
        final AwsPluginConfig objectUnderTest = new AwsPluginConfig();
        assertThat(objectUnderTest.getConfiguration("nonexistent"), nullValue());
    }

    @Test
    void testListNonDefaultConfigurations_empty_by_default() {
        final AwsPluginConfig objectUnderTest = new AwsPluginConfig();
        assertTrue(objectUnderTest.listNonDefaultConfigurations().isEmpty());
    }

    @Test
    void testListNonDefaultConfigurations_returns_named_configs() throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final String json = "{\"default\": {}, \"config_a\": {\"use_aws_sdk_default\": true}, \"config_b\": {\"region\": \"eu-west-1\"}}";

        final AwsPluginConfig objectUnderTest = objectMapper.readValue(json, AwsPluginConfig.class);

        assertThat(objectUnderTest.listNonDefaultConfigurations().size(), equalTo(2));
        assertTrue(objectUnderTest.listNonDefaultConfigurations().contains("config_a"));
        assertTrue(objectUnderTest.listNonDefaultConfigurations().contains("config_b"));
    }

    @Test
    void testGetAllOtherConfigurations_returns_map() throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final String json = "{\"default\": {}, \"config_a\": {\"use_aws_sdk_default\": true}}";

        final AwsPluginConfig objectUnderTest = objectMapper.readValue(json, AwsPluginConfig.class);

        assertThat(objectUnderTest.getAllOtherConfigurations(), notNullValue());
        assertThat(objectUnderTest.getAllOtherConfigurations().size(), equalTo(1));
        assertTrue(objectUnderTest.getAllOtherConfigurations().containsKey("config_a"));
    }

}
