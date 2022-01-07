/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AggregateProcessorConfigTest {
    @Test
    public void testDefault() {
        final AggregateProcessorConfig aggregateConfig = new AggregateProcessorConfig();

        assertThat(aggregateConfig.getWindowDuration(), equalTo(AggregateProcessorConfig.DEFAULT_WINDOW_DURATION));
        assertThat(aggregateConfig.getDbPath(), equalTo(AggregateProcessorConfig.DEFAULT_DB_PATH));
        assertThat(aggregateConfig.isDbPathValid(), equalTo(true));
    }

    @Nested
    class Validation {

        @TempDir
        File temporaryDirectory;
        private File file;

        @BeforeEach
        void setUp() throws IOException {
            file = new File(temporaryDirectory, UUID.randomUUID().toString());
            file.createNewFile();
        }

        @Test
        void isDbPathValid_should_return_true_for_existing_file() throws NoSuchFieldException, IllegalAccessException {
            final AggregateProcessorConfig aggregateConfig = new AggregateProcessorConfig();
            reflectivelySetField(aggregateConfig, "dbPath", file.getAbsolutePath());
            assertThat(aggregateConfig.isDbPathValid(), equalTo(true));
        }
    }

    private void reflectivelySetField(final AggregateProcessorConfig aggregateProcessorConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = AggregateProcessorConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(aggregateProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
