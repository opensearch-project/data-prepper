/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.apache.commons.lang3.RandomStringUtils;

class AwsConfigTest {

    private AwsConfig awsConfig;

    @BeforeEach
    void setUp() {
        awsConfig = new AwsConfig();
    }

    @Test
    void TestConfigOptions_notNull() throws NoSuchFieldException, IllegalAccessException {
        final AwsConfig.AwsMskConfig testMskConfig = new AwsConfig.AwsMskConfig();
        reflectivelySetField(awsConfig, "awsMskConfig", testMskConfig);
        assertThat(awsConfig.getAwsMskConfig(), equalTo(testMskConfig));

        final String testStsRoleArn = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(awsConfig, "stsRoleArn", testStsRoleArn);
        assertThat(awsConfig.getStsRoleArn(), equalTo(testStsRoleArn));

        final String testRegion = RandomStringUtils.randomAlphabetic(8);
        reflectivelySetField(awsConfig, "region", testRegion);
        assertThat(awsConfig.getRegion(), equalTo(testRegion));
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
