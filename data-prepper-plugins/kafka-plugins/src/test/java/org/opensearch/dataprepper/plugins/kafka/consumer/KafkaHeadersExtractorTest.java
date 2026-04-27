/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class KafkaHeadersExtractorTest {
    private static String TEST_STRING_HEADER_KEY = "string-header-key";
    private static String TEST_INT_HEADER_KEY = "int-header-key";
    private static String TEST_DOUBLE_HEADER_KEY = "double-header-key";

    private String testStringHeader;
    private Random random;

    @BeforeEach
    public void setUp() {
        random = new Random();
    }

    @Test
    public void testExtractMessageHeadersWithValidStrings() {
        testStringHeader = UUID.randomUUID().toString();
        RecordHeaders headers = new RecordHeaders();

        int testIntValue = random.nextInt(500);
        //int testIntValue = 50;
        byte[] intBytes = ByteBuffer.allocate(4).putInt(testIntValue).array();

        //double testDoubleValue = random.nextDouble();
        double testDoubleValue = 4.0d;
        byte[] doubleBytes = ByteBuffer.allocate(8).putDouble(testDoubleValue).array();
        headers.add(TEST_STRING_HEADER_KEY, testStringHeader.getBytes(StandardCharsets.UTF_8));
        headers.add(TEST_INT_HEADER_KEY, intBytes);
        headers.add(TEST_DOUBLE_HEADER_KEY, doubleBytes);

        Map<String, Object> headerData = KafkaHeadersExtractor.extractMessageHeaders(headers);
        final String stringHeader = (String)headerData.get(TEST_STRING_HEADER_KEY);
        assertThat(stringHeader, equalTo(testStringHeader));
        final byte[] intValue = (byte[])headerData.get(TEST_INT_HEADER_KEY);
        assertThat(ByteBuffer.wrap(intValue).getInt(), equalTo(testIntValue));
        final byte[] doubleValue = (byte[])headerData.get(TEST_DOUBLE_HEADER_KEY);
        assertThat(ByteBuffer.wrap(doubleValue).getDouble(), equalTo(testDoubleValue));

    }
}

