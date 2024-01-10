/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class MaxMindConfigTest {
    private MaxMindConfig maxMindConfig;

    @BeforeEach
    void setup() {
        maxMindConfig = new MaxMindConfig();
    }

    @Test
    void testDefaultConfig() {
        assertThat(maxMindConfig.getDatabasePaths().size(), equalTo(0));
        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(7)));
        assertThat(maxMindConfig.getCacheSize(), equalTo(4096));
    }

    @Test
    void testCustomConfig() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(maxMindConfig, "databaseRefreshInterval", Duration.ofDays(10));
        reflectivelySetField(maxMindConfig, "cacheSize", 2048);
        reflectivelySetField(maxMindConfig, "databasePaths", List.of("path1", "path2", "path3"));

        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(10)));
        assertThat(maxMindConfig.getCacheSize(), equalTo(2048));
        assertThat(maxMindConfig.getDatabasePaths().size(), equalTo(3));
    }

    private void reflectivelySetField(final MaxMindConfig maxMindConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = MaxMindConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(maxMindConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }

}