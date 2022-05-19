/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import com.amazon.dataprepper.plugins.source.configuration.NotificationTypeOption;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class S3SourceConfigTest {

    @Test
    void default_compression_test() {
        assertThat(new S3SourceConfig().getCompression(), equalTo(CompressionOption.NONE));
    }

    @Nested
    class Validation {
        private S3SourceConfig s3SourceConfig;

        @BeforeEach
        void setUp() {
            s3SourceConfig = new S3SourceConfig();
        }

        @Test
        void isNotificationTypeValid_should_return_false_for_null_value_test() throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(s3SourceConfig, "notificationType", null);
            assertThat(s3SourceConfig.isNotificationTypeValid(), equalTo(false));
        }

        @Test
        void isNotificationTypeValid_should_return_true_for_not_null_value_test() throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(s3SourceConfig, "notificationType", NotificationTypeOption.SQS);
            assertThat(s3SourceConfig.isNotificationTypeValid(), equalTo(true));
        }
    }

    private void reflectivelySetField(final S3SourceConfig dateProcessorConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = S3SourceConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(dateProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }

}