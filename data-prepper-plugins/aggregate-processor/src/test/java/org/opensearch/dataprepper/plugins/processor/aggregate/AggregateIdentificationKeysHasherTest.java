/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
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
        return new AggregateIdentificationKeysHasher(identificationKeys);
    }

    @Test
    void createIdentificationKeysMapFromEvent_returns_expected_IdentficationKeysMap() {
        aggregateIdentificationKeysHasher = createObjectUnderTest();
        final Map<Object, Object> eventMap = new HashMap<>();
        eventMap.put("firstIdentificationKey", UUID.randomUUID().toString());
        eventMap.put("secondIdentificationKey", UUID.randomUUID().toString());

        final AggregateIdentificationKeysHasher.IdentificationKeysMap expectedResult = new AggregateIdentificationKeysHasher.IdentificationKeysMap(new HashMap<>(eventMap));

        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        event = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();

        final AggregateIdentificationKeysHasher.IdentificationKeysMap result = aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(event);
        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void createIdentificationKeysMapFromEvent_where_Event_does_not_contain_one_of_the_identification_keys_returns_expected_Map() {
        aggregateIdentificationKeysHasher = createObjectUnderTest();
        final Map<Object, Object> eventMap = new HashMap<>();
        eventMap.put("firstIdentificationKey", UUID.randomUUID().toString());

        final Map<Object, Object> mapForExpectedHash = new HashMap<>(eventMap);
        mapForExpectedHash.put("secondIdentificationKey", null);

        final AggregateIdentificationKeysHasher.IdentificationKeysMap expectedResult = new AggregateIdentificationKeysHasher.IdentificationKeysMap(mapForExpectedHash);

        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        event = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();

        final AggregateIdentificationKeysHasher.IdentificationKeysMap result = aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(event);
        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void identical_identification_hashes_but_different_objects_are_considered_equal() {
        aggregateIdentificationKeysHasher = createObjectUnderTest();
        final Map<Object, Object> eventMap = new HashMap<>();
        eventMap.put("firstIdentificationKey", UUID.randomUUID().toString());
        eventMap.put("secondIdentificationKey", UUID.randomUUID().toString());
        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        event = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();

        final AggregateIdentificationKeysHasher.IdentificationKeysMap result = aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(event);
        final AggregateIdentificationKeysHasher.IdentificationKeysMap secondResult = aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(event);

        assertThat(result, equalTo(secondResult));
    }

    @Test
    void different_identification_hashes_are_not_considered_equal() {
        aggregateIdentificationKeysHasher = createObjectUnderTest();
        final Map<Object, Object> eventMap = new HashMap<>();
        eventMap.put("firstIdentificationKey", UUID.randomUUID().toString());
        eventMap.put("secondIdentificationKey", UUID.randomUUID().toString());
        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        final Map<Object, Object> secondEventMap = new HashMap<>(eventMap);
        secondEventMap.remove("firstIdentificationKey");

        event = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();

        final Event secondEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(secondEventMap)
                .build();

        final AggregateIdentificationKeysHasher.IdentificationKeysMap result = aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(event);
        final AggregateIdentificationKeysHasher.IdentificationKeysMap secondResult = aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(secondEvent);

        assertThat(result, is(not(equalTo(secondResult))));
    }
}
