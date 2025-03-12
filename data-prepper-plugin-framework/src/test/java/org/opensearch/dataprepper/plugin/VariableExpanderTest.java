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
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDeserializationProblemHandler;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VariableExpanderTest {
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapperConfiguration()
            .extensionPluginConfigObjectMapper(new DataPrepperDeserializationProblemHandler());
    static final JsonFactory JSON_FACTORY = new MappingJsonFactory();

    @Mock
    private PluginConfigValueTranslator pluginConfigValueTranslator;

    private VariableExpander objectUnderTest;

    private static Stream<Arguments> getNonStringTypeArguments() {
        return Stream.of(Arguments.of(Boolean.class, "true", true),
                Arguments.of(Short.class, "2", (short) 2),
                Arguments.of(Integer.class, "10", 10),
                Arguments.of(Long.class, "200", 200L),
                Arguments.of(Double.class, "1.23", 1.23d),
                Arguments.of(Float.class, "2.15", 2.15f),
                Arguments.of(BigDecimal.class, "2.15", BigDecimal.valueOf(2.15)),
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
                Arguments.of(BigDecimal.class, "\"2.15\"", BigDecimal.valueOf(2.15)),
                Arguments.of(Character.class, "\"c\"", 'c'));
    }

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
        when(pluginConfigValueTranslator.getPrefix()).thenReturn(testTranslatorKey);
        when(pluginConfigValueTranslator.translate(eq(testSecretKey))).thenReturn(value.replace("\"", ""));
        objectUnderTest = new VariableExpander(OBJECT_MAPPER, Set.of(pluginConfigValueTranslator));
        final Object actualResult = objectUnderTest.translate(jsonParser, clazz);
        assertThat(actualResult, equalTo(expectedResult));
    }

    @Test
    void testTranslateJsonParserWithSPluginConfigVariableValue_translate_success() throws IOException {
        final String testSecretKey = "testSecretKey";
        final String testTranslatorKey = "test_prefix";
        final String testSecretReference = String.format("${{%s:%s}}", testTranslatorKey, testSecretKey);
        final JsonParser jsonParser = JSON_FACTORY.createParser(String.format("\"%s\"", testSecretReference));
        jsonParser.nextToken();
        PluginConfigVariable mockPluginConfigVariable = new PluginConfigVariable() {

            String secretValue = "samplePluginConfigValue";

            @Override
            public Object getValue() {
                return secretValue;
            }

            @Override
            public void setValue(Object updatedValue) {
                this.secretValue = updatedValue.toString();
            }

            @Override
            public void refresh() {
            }

            @Override
            public boolean isUpdatable() {
                return true;
            }
        };
        when(pluginConfigValueTranslator.getPrefix()).thenReturn(testTranslatorKey);
        when(pluginConfigValueTranslator.translateToPluginConfigVariable(eq(testSecretKey)))
                .thenReturn(mockPluginConfigVariable);
        objectUnderTest = new VariableExpander(OBJECT_MAPPER, Set.of(pluginConfigValueTranslator));
        final Object actualResult = objectUnderTest.translate(jsonParser, PluginConfigVariable.class);
        assertNotNull(actualResult);
        assertThat(actualResult, equalTo(mockPluginConfigVariable));
    }

    @Test
    void testTranslateJsonParserWithSPluginConfigVariableValue_translate_failure() throws IOException {
        final String testSecretKey = "testSecretKey";
        final String testTranslatorKey = "test_prefix";
        final String testSecretReference = String.format("${{%s:%s}}", testTranslatorKey, testSecretKey);
        final JsonParser jsonParser = JSON_FACTORY.createParser(String.format("\"%s\"", testSecretReference));
        jsonParser.nextToken();
        when(pluginConfigValueTranslator.getPrefix()).thenReturn(testTranslatorKey);
        when(pluginConfigValueTranslator.translateToPluginConfigVariable(eq(testSecretKey)))
                .thenThrow(IllegalArgumentException.class);
        objectUnderTest = new VariableExpander(OBJECT_MAPPER, Set.of(pluginConfigValueTranslator));
        assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.translate(jsonParser, PluginConfigVariable.class));
    }
}