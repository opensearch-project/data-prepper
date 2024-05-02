/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HTTPSourceConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PLUGIN_NAME = "http";

    private static Stream<Arguments> provideCompressionOption() {
        return Stream.of(Arguments.of(CompressionOption.GZIP));
    }

    @Test
    void testDefault() {
        // Prepare
        final HTTPSourceConfig sourceConfig = new HTTPSourceConfig();

        // When/Then
        assertEquals(HTTPSourceConfig.DEFAULT_PORT, sourceConfig.getPort());
        assertEquals(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI, sourceConfig.getPath());
        assertEquals(CompressionOption.NONE, sourceConfig.getCompression());
    }

    @ParameterizedTest
    @MethodSource("provideCompressionOption")
    void testValidCompression(final CompressionOption compressionOption) {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(HTTPSourceConfig.COMPRESSION, compressionOption.name());

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final HTTPSourceConfig httpSourceConfig = OBJECT_MAPPER.convertValue(
                pluginSetting.getSettings(), HTTPSourceConfig.class);

        // When/Then
        assertEquals(compressionOption, httpSourceConfig.getCompression());
    }

    @Test
    void getPath_should_return_correct_path() throws NoSuchFieldException, IllegalAccessException {
        final HTTPSourceConfig objectUnderTest = new HTTPSourceConfig();

        reflectivelySetField(objectUnderTest, "path", "/my/custom/path");

        assertThat(objectUnderTest.isPathValid(), equalTo(true));
        assertThat(objectUnderTest.getPath(), equalTo("/my/custom/path"));
    }

    @Test
    void isPathValid_should_return_false_for_invalid_path() throws NoSuchFieldException, IllegalAccessException {
        final HTTPSourceConfig objectUnderTest = new HTTPSourceConfig();

        reflectivelySetField(objectUnderTest, "path", "my/custom/path");

        assertThat(objectUnderTest.isPathValid(), equalTo(false));
    }

    private void reflectivelySetField(final HTTPSourceConfig httpSourceConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = HTTPSourceConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(httpSourceConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
