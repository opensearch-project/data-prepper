/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator.DEFAULT_DEPRECATED_PREFIX;

@ExtendWith(MockitoExtension.class)
class VariableExpanderTest {
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapperConfiguration().extensionPluginConfigObjectMapper();
    static final JsonFactory JSON_FACTORY = new MappingJsonFactory();

    @Mock
    private PluginConfigValueTranslator pluginConfigValueTranslator;

    private VariableExpander objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new VariableExpander(OBJECT_MAPPER, Set.of(pluginConfigValueTranslator));
    }

    @ParameterizedTest
    @MethodSource("getNonStringTypeArguments")
    void testTranslateJsonParserWithNonStringValue(final Class<?> clazz, final String value, final Object expectedResult)
            throws IOException {
        final JsonParser jsonParser = JSON_FACTORY.createParser(value);
        jsonParser.nextToken();
        final Object actualResult = objectUnderTest.translate(jsonParser, clazz);
        assertThat(actualResult, equalTo(expectedResult));
    }

    @ParameterizedTest
    @MethodSource("getStringTypeArguments")
    void testTranslateJsonParserWithStringValue_no_pattern_match(
            final Class<?> clazz, final String value, final Object expectedResult) throws IOException {
        final JsonParser jsonParser = JSON_FACTORY.createParser(value);
        jsonParser.nextToken();
        final Object actualResult = objectUnderTest.translate(jsonParser, clazz);
        assertThat(actualResult, equalTo(expectedResult));
    }

    @ParameterizedTest
    @MethodSource("getStringTypeArguments")
    void testTranslateJsonParserWithStringValue_no_translator(
            final Class<?> clazz, final String value, final Object expectedResult) throws IOException {
        final JsonParser jsonParser = JSON_FACTORY.createParser(value);
        jsonParser.nextToken();
        objectUnderTest = new VariableExpander(OBJECT_MAPPER, Collections.emptySet());
        final Object actualResult = objectUnderTest.translate(jsonParser, clazz);
        assertThat(actualResult, equalTo(expectedResult));
    }

    @Test
    void testTranslateJsonParserWithStringValue_no_key_match() throws IOException {
        final String testSecretKey = "testSecretKey";
        final String testSecretReference = String.format("${{unknown.%s}}", testSecretKey);
        final JsonParser jsonParser = JSON_FACTORY.createParser(String.format("\"%s\"", testSecretReference));
        jsonParser.nextToken();
        when(pluginConfigValueTranslator.getPrefix()).thenReturn("test_prefix");
        objectUnderTest = new VariableExpander(OBJECT_MAPPER, Set.of(pluginConfigValueTranslator));
        final Object actualResult = objectUnderTest.translate(jsonParser, String.class);
        assertThat(actualResult, equalTo(testSecretReference));
    }

    @ParameterizedTest
    @MethodSource("getStringTypeArguments")
    void testTranslateJsonParserWithStringValue_translate_success(
            final Class<?> clazz, final String value, final Object expectedResult) throws IOException {
        final String testSecretKey = "testSecretKey";
        final String testTranslatorKey = "test_prefix";
        final String testSecretReference = String.format("${{%s:%s}}", testTranslatorKey, testSecretKey);
        final JsonParser jsonParser = JSON_FACTORY.createParser(String.format("\"%s\"", testSecretReference));
        jsonParser.nextToken();
        when(pluginConfigValueTranslator.getDeprecatedPrefix()).thenReturn(DEFAULT_DEPRECATED_PREFIX);
        when(pluginConfigValueTranslator.getPrefix()).thenReturn(testTranslatorKey);
        when(pluginConfigValueTranslator.translate(eq(testSecretKey))).thenReturn(value.replace("\"", ""));
        objectUnderTest = new VariableExpander(OBJECT_MAPPER, Set.of(pluginConfigValueTranslator));
        final Object actualResult = objectUnderTest.translate(jsonParser, clazz);
        assertThat(actualResult, equalTo(expectedResult));
    }

    @ParameterizedTest
    @MethodSource("getStringTypeArguments")
    void testTranslateJsonParserWithStringValue_translate_success_with_deprecated_prefix(
            final Class<?> clazz, final String value, final Object expectedResult) throws IOException {
        final String testSecretKey = "testSecretKey";
        final String testTranslatorKey = "test_prefix";
        final String testDeprecatedTranslatorKey = "test_deprecated_prefix";
        final String testSecretReference = String.format("${{%s:%s}}", testDeprecatedTranslatorKey, testSecretKey);
        final JsonParser jsonParser = JSON_FACTORY.createParser(String.format("\"%s\"", testSecretReference));
        jsonParser.nextToken();
        when(pluginConfigValueTranslator.getDeprecatedPrefix()).thenReturn(testDeprecatedTranslatorKey);
        when(pluginConfigValueTranslator.getPrefix()).thenReturn(testTranslatorKey);
        when(pluginConfigValueTranslator.translate(eq(testSecretKey))).thenReturn(value.replace("\"", ""));
        objectUnderTest = new VariableExpander(OBJECT_MAPPER, Set.of(pluginConfigValueTranslator));
        final Object actualResult = objectUnderTest.translate(jsonParser, clazz);
        assertThat(actualResult, equalTo(expectedResult));
    }

    private static Stream<Arguments> getNonStringTypeArguments() {
        return Stream.of(Arguments.of(Boolean.class, "true", true),
                Arguments.of(Short.class, "2", (short) 2),
                Arguments.of(Integer.class, "10", 10),
                Arguments.of(Long.class, "200", 200L),
                Arguments.of(Double.class, "1.23", 1.23d),
                Arguments.of(Float.class, "2.15", 2.15f),
                Arguments.of(Map.class, "{}", Collections.emptyMap()));
    }

    private static Stream<Arguments> getStringTypeArguments() {
        final String testRandomValue = "non-secret-prefix-" + RandomStringUtils.randomAlphabetic(5);
        return Stream.of(Arguments.of(String.class, String.format("\"%s\"", testRandomValue),
                        testRandomValue),
                Arguments.of(Duration.class, "\"PT15M\"", Duration.parse("PT15M")),
                Arguments.of(Boolean.class, "\"true\"", true),
                Arguments.of(Short.class, "\"2\"", (short) 2),
                Arguments.of(Integer.class, "\"10\"", 10),
                Arguments.of(Long.class, "\"200\"", 200L),
                Arguments.of(Double.class, "\"1.23\"", 1.23d),
                Arguments.of(Float.class, "\"2.15\"", 2.15f),
                Arguments.of(Character.class, "\"c\"", 'c'));
    }
}