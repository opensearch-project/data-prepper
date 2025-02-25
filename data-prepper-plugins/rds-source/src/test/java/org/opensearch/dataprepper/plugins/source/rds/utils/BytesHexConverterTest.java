package org.opensearch.dataprepper.plugins.source.rds.utils;

import org.junit.jupiter.api.Test;


import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BytesHexConverterTest {
    @Test
    void test_bytes_to_hex_string() {
        final String testData = "Text with a single quote: O'Reilly";
        byte[] bytes = testData.getBytes(StandardCharsets.ISO_8859_1);
        final String expected = "54657874207769746820612073696e676c652071756f74653a204f275265696c6c79";
        final String result = BytesHexConverter.bytesToHex(bytes);
        assertThat(result, equalTo(expected));
    }
}
