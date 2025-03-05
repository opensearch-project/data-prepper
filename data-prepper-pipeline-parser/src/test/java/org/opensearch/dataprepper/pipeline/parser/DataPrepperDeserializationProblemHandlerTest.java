/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.ValueInstantiator;
import com.fasterxml.jackson.databind.util.ClassUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperDeserializationProblemHandlerTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Class<?> TEST_TARGET_TYPE = Boolean.class;
    private static final String TEST_VALUE = UUID.randomUUID().toString();
    private static final String TEST_FAILURE_MESSAGE = UUID.randomUUID().toString();

    @Mock
    private DeserializationContext deserializationContext;

    @Mock
    private JsonParser jsonParser;

    @Mock
    private ValueInstantiator valueInstantiator;

    private final DataPrepperDeserializationProblemHandler objectUnderTest = new DataPrepperDeserializationProblemHandler();

    @Test
    void testHandleWeirdStringValue() {
        final IOException exception = assertThrows(IOException.class, () -> objectUnderTest.handleWeirdStringValue(
                deserializationContext, TEST_TARGET_TYPE, TEST_VALUE, TEST_FAILURE_MESSAGE));
        assertThat(exception.getMessage(), containsString(ClassUtil.nameOf(TEST_TARGET_TYPE)));
        assertThat(exception.getMessage(), containsString(TEST_VALUE));
        assertThat(exception.getMessage(), containsString(TEST_FAILURE_MESSAGE));
    }

    @Test
    void testHandleUnexpectedToken() {
        when(deserializationContext.getParser()).thenReturn(jsonParser);
        final JavaType javaType = OBJECT_MAPPER.constructType(TEST_TARGET_TYPE);
        final JsonToken jsonToken = JsonToken.END_OBJECT;
        final JsonMappingException exception = assertThrows(JsonMappingException.class, () -> objectUnderTest.handleUnexpectedToken(
                deserializationContext, javaType, jsonToken, jsonParser, TEST_FAILURE_MESSAGE));
        assertThat(exception.getMessage(), containsString(ClassUtil.getTypeDescription(javaType)));
        assertThat(exception.getMessage(), not(containsString(TEST_FAILURE_MESSAGE)));
    }

    @Test
    void testHandleMissingInstantiator() throws IOException {
        when(deserializationContext.getParser()).thenReturn(jsonParser);
        when(jsonParser.getText()).thenReturn(UUID.randomUUID().toString());
        final JsonMappingException exception = assertThrows(JsonMappingException.class, () -> objectUnderTest.handleMissingInstantiator(
                deserializationContext, TEST_TARGET_TYPE, valueInstantiator, jsonParser, TEST_FAILURE_MESSAGE));
        assertThat(exception.getMessage(), containsString(jsonParser.getText()));
        assertThat(exception.getMessage(), containsString(TEST_TARGET_TYPE.getName()));
        assertThat(exception.getMessage(), not(containsString(TEST_FAILURE_MESSAGE)));
    }
}