/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.log.JacksonLog;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Map;
import java.util.Collections;

class DefaultEventFactoryTests {
    private DefaultEventBuilder eventBuilder;
    private DefaultLogEventBuilder logEventBuilder;
    private DefaultEventFactory eventFactory;
    
    @Test
    void testDefaultEventFactory() {
        eventFactory = new DefaultEventFactory();
        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        Map<String, Object> attributes = Collections.emptyMap();
        final DefaultEventBuilder eventBuilder = (DefaultEventBuilder) eventFactory.eventBuilder(DefaultEventBuilder.class).withEventMetadataAttributes(attributes).withData(data);
        JacksonEvent event = (JacksonEvent) eventBuilder.build();
        EventMetadata eventMetadata = event.getMetadata();
        
        assertThat(eventMetadata.getTimeReceived(), not(equalTo(null)));
        assertThat(eventMetadata.getEventType(), equalTo(DefaultEventBuilder.EVENT_TYPE));
        assertThat(eventMetadata.getAttributes(), equalTo(attributes));
        assertThat(event.toMap(), equalTo(data));
    }

    @Test
    void testDefaultEventFactoryWithLogEvent() {
        eventFactory = new DefaultEventFactory();
        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        Map<String, Object> attributes = Collections.emptyMap();
        final DefaultLogEventBuilder logEventBuilder = (DefaultLogEventBuilder) eventFactory.eventBuilder(LogEventBuilder.class).withEventMetadataAttributes(attributes).withData(data);
        JacksonLog log = (JacksonLog) logEventBuilder.build();
        EventMetadata eventMetadata = log.getMetadata();
        
        assertThat(eventMetadata.getTimeReceived(), not(equalTo(null)));
        assertThat(eventMetadata.getEventType(), equalTo(DefaultLogEventBuilder.LOG_EVENT_TYPE));
        assertThat(eventMetadata.getAttributes(), equalTo(attributes));
        assertThat(log.toMap(), equalTo(data));
    }

}

