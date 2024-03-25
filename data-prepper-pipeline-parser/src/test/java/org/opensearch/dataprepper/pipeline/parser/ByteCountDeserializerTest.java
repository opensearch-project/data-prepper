/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.types.ByteCount;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ByteCountDeserializerTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(ByteCount.class, new ByteCountDeserializer());
        objectMapper.registerModule(simpleModule);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "1b 2b", "1vb", "bad"})
    void convert_with_invalid_values_throws(final String invalidByteString) {
        assertThrows(IllegalArgumentException.class, () -> objectMapper.convertValue(invalidByteString, ByteCount.class));
    }

    @ParameterizedTest
    @CsvSource({
            "0b, 0",
            "1b, 1",
            "2b, 2",
            "2kb, 2048",
            "1mb, 1048576"
    })
    void convert_with_valid_values_returns_expected_bytes(final String byteString, final long expectedValue) {
        final ByteCount byteCount = objectMapper.convertValue(byteString, ByteCount.class);

        assertThat(byteCount, notNullValue());
        assertThat(byteCount.getBytes(), equalTo(expectedValue));
    }
}