/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

