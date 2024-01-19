/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperScalarTypeDeserializerTest {
    @Mock
    private VariableExpander variableExpander;
    @Mock
    private JsonParser jsonParser;
    @Mock
    private DeserializationContext ctxt;

    private DataPrepperScalarTypeDeserializer<Object> objectUnderTest;

    @ParameterizedTest
    @MethodSource("getScalarTypeArguments")
    void testDeserialize(final Class<Object> scalarType, final Object scalarValue) throws IOException {
        when(variableExpander.translate(eq(jsonParser), eq(scalarType))).thenReturn(scalarValue);
        objectUnderTest = new DataPrepperScalarTypeDeserializer<>(variableExpander, scalarType);
        assertThat(objectUnderTest.deserialize(jsonParser, ctxt), equalTo(scalarValue));
    }

    private static Stream<Arguments> getScalarTypeArguments() {
        return Stream.of(
                Arguments.of(String.class, RandomStringUtils.randomAlphabetic(5)),
                Arguments.of(Duration.class, Duration.parse("PT15M")),
                Arguments.of(Boolean.class, true),
                Arguments.of(Short.class, (short) 2),
                Arguments.of(Integer.class, 10),
                Arguments.of(Long.class, 200L),
                Arguments.of(Double.class, 1.23d),
                Arguments.of(Float.class, 2.15f),
                Arguments.of(Character.class, 'c'));
    }
}