/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.time.Instant;
import java.util.Map;
import java.util.Random;

public class LiveCaptureEventTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private String key1;
    private String key2;
    private Integer value1;
    private Double value2;
    private String description;
    private Instant now;
    private Event event;
    private Map<String, Object> testData;

    private LiveCaptureEvent createObjectUnderTest() {
        key1 = RandomStringUtils.randomAlphabetic(5);
        Random random = new Random();
        value1 = random.nextInt();
        key2 = RandomStringUtils.randomAlphabetic(6);
        value2 = random.nextDouble();
        testData = Map.of(key1, value1, key2, value2);
        event = JacksonEvent.builder().withEventType("event").withData(testData).build();
        description = RandomStringUtils.randomAlphabetic(20);
        now = Instant.now();
        return new LiveCaptureEvent(description, now, event);
    }

    @Test
    public void testLiveCaptureEvent() throws Exception {
        LiveCaptureEvent liveCaptureEvent = createObjectUnderTest();
        Map<String, Object> map = liveCaptureEvent.toMap();
        assertThat(map.get("description"), equalTo(description));
        assertThat(map.get("version"), equalTo(LiveCaptureEvent.CURRENT_VERSION));
        assertThat(map.get("time"), equalTo(now));
        assertThat(map.get("event"), equalTo(testData));
        Map<String, Object> mapFromJson = mapper.readValue(liveCaptureEvent.toJsonString(), new TypeReference<>() {});
        assertThat(mapFromJson.get("description"), equalTo(description));
        assertThat(mapFromJson.get("version"), equalTo(LiveCaptureEvent.CURRENT_VERSION));
        assertThat(Instant.parse((String)mapFromJson.get("time")), equalTo(now));
        Map<String, Object> testDataFromJson = mapper.readValue((String)mapFromJson.get("event"), new TypeReference<>() {});
        assertThat(testDataFromJson, equalTo(testData));
    }
}

