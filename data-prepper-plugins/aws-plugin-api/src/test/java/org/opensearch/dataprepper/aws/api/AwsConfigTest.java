/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.aws.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

public class AwsConfigTest {

    private AwsConfig awsConfig;

    @BeforeEach
    void setUp() {
        awsConfig = new AwsConfig();
    }

    @Test
    void TestConfigOptions_notNull() throws NoSuchFieldException, IllegalAccessException {

        final String testStsRoleArn = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(awsConfig, "awsStsRoleArn", testStsRoleArn);
        assertThat(awsConfig.getAwsStsRoleArn(), equalTo(testStsRoleArn));
        final String testStsExternalId = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(awsConfig, "awsStsExternalId", testStsExternalId);
        assertThat(awsConfig.getAwsStsExternalId(), equalTo(testStsExternalId));

        final Map<String, String> testStsHeaderOverrides = Map.of(RandomStringUtils.randomAlphabetic(5), RandomStringUtils.randomAlphabetic(10));
        reflectivelySetField(awsConfig, "awsStsHeaderOverrides", testStsHeaderOverrides);
        assertThat(awsConfig.getAwsStsHeaderOverrides(), equalTo(testStsHeaderOverrides));

        final String testRegion = RandomStringUtils.randomAlphabetic(8);
        reflectivelySetField(awsConfig, "awsRegion", testRegion);
        assertThat(awsConfig.getAwsRegion(), equalTo(Region.of(testRegion)));
    }

    @Test
    void TestConfigOptions_configuration_returns_value() throws NoSuchFieldException, IllegalAccessException {
        final String configName = "ecs_task_role";
        reflectivelySetField(awsConfig, "configuration", configName);
        assertThat(awsConfig.getConfiguration(), equalTo(configName));
    }

    @Test
    void TestConfigOptions_configuration_returns_null_by_default() {
        assertThat(awsConfig.getConfiguration(), equalTo(null));
    }

    @Test
    void isValidConfiguration_returns_true_when_configuration_is_null() {
        assertThat(awsConfig.isValidConfiguration(), equalTo(true));
    }

    @Test
    void isValidConfiguration_returns_true_when_only_configuration_is_set() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsConfig, "configuration", "my_config");
        assertThat(awsConfig.isValidConfiguration(), equalTo(true));
    }

    @Test
    void isValidConfiguration_returns_true_when_configuration_and_region_are_set() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsConfig, "configuration", "my_config");
        reflectivelySetField(awsConfig, "awsRegion", "us-east-1");
        assertThat(awsConfig.isValidConfiguration(), equalTo(true));
    }

    @Test
    void isValidConfiguration_returns_false_when_configuration_and_sts_role_arn_are_set() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsConfig, "configuration", "my_config");
        reflectivelySetField(awsConfig, "awsStsRoleArn", "arn:aws:iam::123456789012:role/TestRole");
        assertThat(awsConfig.isValidConfiguration(), equalTo(false));
    }

    @Test
    void isValidConfiguration_returns_false_when_configuration_and_sts_header_overrides_are_set() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsConfig, "configuration", "my_config");
        reflectivelySetField(awsConfig, "awsStsHeaderOverrides", Map.of("key", "value"));
        assertThat(awsConfig.isValidConfiguration(), equalTo(false));
    }

    @Test
    void isValidConfiguration_returns_false_when_configuration_and_sts_external_id_are_set() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsConfig, "configuration", "my_config");
        reflectivelySetField(awsConfig, "awsStsExternalId", "ext-id-123");
        assertThat(awsConfig.isValidConfiguration(), equalTo(false));
    }

    @Test
    void isValidConfiguration_returns_true_when_inline_credentials_used_without_configuration() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsConfig, "awsRegion", "us-east-1");
        reflectivelySetField(awsConfig, "awsStsRoleArn", "arn:aws:iam::123456789012:role/TestRole");
        assertThat(awsConfig.isValidConfiguration(), equalTo(true));
    }

    private void reflectivelySetField(final AwsConfig awsConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = AwsConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(awsConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}

