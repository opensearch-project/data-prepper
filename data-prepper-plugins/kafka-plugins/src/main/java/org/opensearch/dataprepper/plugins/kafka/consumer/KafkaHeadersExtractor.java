/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
        for (Header header: headers) {
            byte[] headerValue = header.value();
            if (headerValue == null) {
                headerData.put(header.key(), null);
                continue;
            }
            String strValue = new String(header.value(), StandardCharsets.UTF_8);
            System.out.println("____"+headerValue.length+"...."+strValue.getBytes().length);
            if (Arrays.equals(headerValue, strValue.getBytes())) {
                System.out.println("___1___key_"+header.key());
                headerData.put(header.key(), strValue);
                continue;
            }
            System.out.println("___2___key_"+header.key());
            headerData.put(header.key(), headerValue);
        }
        return headerData;
    }
}
