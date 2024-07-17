/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

interface ObfuscationActionTest {
    
    default Record<Event> createRecord(String message) {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("message", message);
        return new Record<>(JacksonEvent.builder()
            .withData(testData)
            .withEventType("event")
            .build());
    }    
}
