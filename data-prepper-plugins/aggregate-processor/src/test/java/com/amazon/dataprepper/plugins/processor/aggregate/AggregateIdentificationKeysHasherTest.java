/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AggregateIdentificationKeysHasherTest {
    private Event event;
    private List<String> identificationKeys;
    private AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher;

    @BeforeEach
    void setup() {
        identificationKeys = new ArrayList<>();
        identificationKeys.add("firstIdentificationKey");
        identificationKeys.add("secondIdentificationKey");
    }

    private AggregateIdentificationKeysHasher createObjectUnderTest() {
        return new AggregateIdentificationKeysHasher();
    }

    @Test
    void createIdentificationKeyHashFromEvent_returns_expected_Map() {
        aggregateIdentificationKeysHasher = createObjectUnderTest();
        final Map<Object, Object> eventMap = new HashMap<>();
        eventMap.put("firstIdentificationKey", UUID.randomUUID().toString());
        eventMap.put("secondIdentificationKey", UUID.randomUUID().toString());

        final Map<Object, Object> expectedResult = new HashMap<>(eventMap);

        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        event = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();

        final Map<Object, Object> result = aggregateIdentificationKeysHasher.createIdentificationKeyHashFromEvent(event, identificationKeys);
        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void createIdentificationKeysHashFromEvent_where_Event_does_not_contain_one_of_the_identification_keys_returns_expected_Map() {
        aggregateIdentificationKeysHasher = createObjectUnderTest();
        final Map<Object, Object> eventMap = new HashMap<>();
        eventMap.put("firstIdentificationKey", UUID.randomUUID().toString());

        final Map<Object, Object> expectedResult = new HashMap<>(eventMap);
        expectedResult.put("secondIdentificationKey", null);

        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        event = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();

        final Map<Object, Object> result = aggregateIdentificationKeysHasher.createIdentificationKeyHashFromEvent(event, identificationKeys);
        assertThat(result, equalTo(expectedResult));
    }
}
