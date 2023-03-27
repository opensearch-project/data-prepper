/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Collections;
import java.util.Map;

class DefaultEventBuilderTests {
    private DefaultEventBuilder defaultEventBuilder;

    @BeforeEach
    void setup() {
        defaultEventBuilder = new DefaultEventBuilder();
    }

    @Test
    void testDefaultEventBuilder() {
        assertThat(defaultEventBuilder.getTimeReceived(), not(equalTo(null)));
    }

    @Test
    void testDefaultBaseEventBuilderWithTypeDataAndAttributes() {
        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        defaultEventBuilder.withData(data);
        Map<String, Object> metadataAttributes = Collections.emptyMap();
        defaultEventBuilder.withEventMetadataAttributes(metadataAttributes);
        JacksonEvent event = (JacksonEvent)defaultEventBuilder.build();
        EventMetadata eventMetadata = event.getMetadata();
        
        assertThat(eventMetadata.getTimeReceived(), not(equalTo(null)));
        assertThat(eventMetadata.getEventType(), equalTo(DefaultEventBuilder.EVENT_TYPE));
        assertThat(eventMetadata.getAttributes(), equalTo(metadataAttributes));
        assertThat(event.toMap(), equalTo(data));
    }
}
