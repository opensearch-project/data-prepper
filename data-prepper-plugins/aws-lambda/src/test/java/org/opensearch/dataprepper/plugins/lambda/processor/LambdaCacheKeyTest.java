/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class LambdaCacheKeyTest {
    private static final EventFactory TEST_EVENT_FACTORY = TestEventFactory.getTestEventFactory();
    @Test
    public void testLambdaCacheKey() {
        List<Object> keys = new ArrayList<>();
        String testString = UUID.randomUUID().toString();
        keys.add(testString);
        keys.add(111);
        keys.add(2222L);
        LambdaCacheKey lambdaCacheKey = new LambdaCacheKey(keys);
        assertThat(lambdaCacheKey.length(), equalTo(testString.length() + 16));
        LambdaCacheKey lambdaCacheKey2 = new LambdaCacheKey(keys);
        assertThat(lambdaCacheKey, equalTo(lambdaCacheKey2));
    }

    @Test
    public void testLambdaCacheKeyWithEvent() {
        List<String> keyNames = new ArrayList<>();
        String testString = UUID.randomUUID().toString();
        keyNames.add("key1");
        keyNames.add("key2");
        keyNames.add("key3");
        Map<String, Object> data = new HashMap<>();
        data.put("key1", testString);
        data.put("key2", 2222L);
        data.put("key3", 12345);
        Event event = TEST_EVENT_FACTORY.eventBuilder(EventBuilder.class)
                .withData(data)
                .withEventType("event")
                .build();
        LambdaCacheKey lambdaCacheKey = new LambdaCacheKey(event, keyNames);
        assertThat(lambdaCacheKey.length(), equalTo(testString.length() + 16));
        LambdaCacheKey lambdaCacheKey2 = new LambdaCacheKey(event, keyNames);
        assertThat(lambdaCacheKey, equalTo(lambdaCacheKey2));
    }

    @Test
    public void testInvalidLambdaCacheKeyWithEvent() {
        List<String> keyNames = new ArrayList<>();
        String testString = UUID.randomUUID().toString();
        keyNames.add("key1");
        keyNames.add("key2");
        keyNames.add("key3");
        Map<String, Object> data = new HashMap<>();
        data.put("key1", testString);
        data.put("key2", 0.2d);
        data.put("key3", 12345);
        Event event = TEST_EVENT_FACTORY.eventBuilder(EventBuilder.class)
                .withData(data)
                .withEventType("event")
                .build();
        assertThrows(RuntimeException.class, () ->  new LambdaCacheKey(event, keyNames));
    }

    @Test
    public void testInvalidLambdaCacheKey() {
        List<Object> keys = new ArrayList<>();
        String testString = UUID.randomUUID().toString();
        keys.add(testString);
        keys.add(0.1d);
        keys.add(2222L);
        assertThrows(RuntimeException.class, () ->  new LambdaCacheKey(keys));
    }

    @Test
    public void testLambdaCacheKeyDifferentOrderNotEqual() {
        List<Object> keys = new ArrayList<>();
        String testString = UUID.randomUUID().toString();
        keys.add(testString);
        keys.add(11);
        keys.add(2222L);
        LambdaCacheKey lambdaCacheKey = new LambdaCacheKey(keys);
        List<Object> keys2 = new ArrayList<>();
        keys2.add(testString);
        keys2.add(2222L);
        keys2.add(11);
        LambdaCacheKey lambdaCacheKey2 = new LambdaCacheKey(keys2);
        assertThat(lambdaCacheKey, not(equalTo(lambdaCacheKey2)));
        assertThat(lambdaCacheKey, not(equalTo(new LambdaCacheKey(List.of()))));
    }


}

