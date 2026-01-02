/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

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

        final Map<String, String> testStsHeaderOverrides = Map.of("header1", "value1", "header2", "value2");
        reflectivelySetField(awsConfig, "awsStsHeaderOverrides", testStsHeaderOverrides);
        assertThat(awsConfig.getAwsStsHeaderOverrides(), equalTo(testStsHeaderOverrides));
    }

    @Test
    void testStsHeaderOverridesValidation_hasMaxSizeConstraint() throws NoSuchFieldException {
        // Verify that the sts_header_overrides field has the @Size(max = 5) validation annotation
        final Field field = AwsConfig.class.getDeclaredField("awsStsHeaderOverrides");
        final jakarta.validation.constraints.Size sizeAnnotation = field.getAnnotation(jakarta.validation.constraints.Size.class);
        
        assertThat("sts_header_overrides field should have @Size annotation", sizeAnnotation != null, equalTo(true));
        assertThat("sts_header_overrides should have max size of 5", sizeAnnotation.max(), equalTo(5));
        assertThat("sts_header_overrides validation message should be correct", 
                   sizeAnnotation.message(), equalTo("sts_header_overrides supports a maximum of 5 headers to override"));
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
