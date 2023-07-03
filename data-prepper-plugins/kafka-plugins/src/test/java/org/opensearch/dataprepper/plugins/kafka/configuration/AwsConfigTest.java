/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class AwsConfigTest {

    private AwsConfig awsConfig;

    @BeforeEach
    void setUp() {
        awsConfig = new AwsConfig();
    }

    @Test
    void getMskArn_notNull() throws NoSuchFieldException, IllegalAccessException {
        final String testArn = UUID.randomUUID().toString();
        reflectivelySetField(awsConfig, "awsMskArn", testArn);
        assertThat(awsConfig.getAwsMskArn(), equalTo(testArn));
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
