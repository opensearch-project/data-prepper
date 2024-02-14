/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress.encoding;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.plugins.processor.decompress.exceptions.DecodingException;

import java.util.Base64;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class Base64DecoderEngineTest {

    @ParameterizedTest
    @CsvSource(value = {"Hello world,SGVsbG8gd29ybGQ=", "Test123,VGVzdDEyMw=="})
    void decode_correctly_decodes_base64(final String expectedDecodedValue, final String base64EncodedValue) {
        final byte[] expectedDecodedBytes = expectedDecodedValue.getBytes();

        final DecoderEngine objectUnderTest = new Base64DecoderEngine();

        final byte[] decodedBytes = objectUnderTest.decode(base64EncodedValue);

        assertThat(decodedBytes, equalTo(expectedDecodedBytes));
    }

    @Test
    void decode_throws_DecodingException_when_decoding_base64_throws_exception() {
        final String encodedValue = UUID.randomUUID().toString();
        final Base64.Decoder decoder = mock(Base64.Decoder.class);
        when(decoder.decode(encodedValue)).thenThrow(RuntimeException.class);

        try(final MockedStatic<Base64> base64MockedStatic = mockStatic(Base64.class)) {
            base64MockedStatic.when(Base64::getDecoder).thenReturn(decoder);

            final DecoderEngine objectUnderTest = new Base64DecoderEngine();

            assertThrows(DecodingException.class, () -> objectUnderTest.decode(encodedValue));
        }
    }
}
