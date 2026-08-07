/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaHeadersExtractor {
    public static Map<String, Object> extractMessageHeaders(Headers headers) {
        if (headers == null) {
            return null;
        }
        Map<String, Object> headerData = new HashMap<>();
        for (Header header : headers) {
            byte[] headerValue = header.value();
            if (headerValue == null) {
                headerData.put(header.key(), null);
                continue;
            }
            String strValue = new String(headerValue, StandardCharsets.UTF_8);
            if (Arrays.equals(headerValue, strValue.getBytes(StandardCharsets.UTF_8))
                    && isPrintableString(strValue)) {
                headerData.put(header.key(), strValue);
            } else {
                headerData.put(header.key(), headerValue);
            }
        }
        return headerData;
    }

    private static boolean isPrintableString(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isISOControl(c) && c != '\t' && c != '\n' && c != '\r') {
                return false;
            }
        }
        return true;
    }
}
