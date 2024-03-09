/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class DefaultBaseEventBuilderTests {
    class TestDefaultBaseEventBuilder extends DefaultBaseEventBuilder {
        @Override
        public Event build() {
            return JacksonEvent.builder().build();
        }
    }

    private DefaultBaseEventBuilder createObjectUnderTest() {
        return new TestDefaultBaseEventBuilder();
    }

    @Test
    void testDefaultBaseEventBuilder() {
        DefaultBaseEventBuilder defaultBaseEventBuilder = createObjectUnderTest();
        assertThat(defaultBaseEventBuilder.getTimeReceived(), not(equalTo(null)));
    }

    @Test
    void testDefaultBaseEventBuilderWithTypeDataAndAttributes() {
        DefaultBaseEventBuilder defaultBaseEventBuilder = createObjectUnderTest();
        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        defaultBaseEventBuilder.withData(data);
        String testEventType = RandomStringUtils.randomAlphabetic(10);
        defaultBaseEventBuilder.withEventType(testEventType);
        Map<String, Object> metadataAttributes = Collections.emptyMap();
        defaultBaseEventBuilder.withEventMetadataAttributes(metadataAttributes);

        assertThat(defaultBaseEventBuilder.getTimeReceived(), not(equalTo(null)));
        assertThat(defaultBaseEventBuilder.getData(), equalTo(data));
        assertThat(defaultBaseEventBuilder.getEventType(), equalTo(testEventType));
        assertThat(defaultBaseEventBuilder.getEventMetadataAttributes(), equalTo(metadataAttributes));
        assertThat(defaultBaseEventBuilder.getEventMetadata().getEventType(), equalTo(testEventType));
        Instant timeReceived = defaultBaseEventBuilder.getTimeReceived();
        assertThat(defaultBaseEventBuilder.getEventMetadata().getTimeReceived(), equalTo(timeReceived));
        assertThat(defaultBaseEventBuilder.getData(), equalTo(data));
    }

    @Test
    void testDefaultBaseEventBuilderWithEventMetadata() {
        DefaultBaseEventBuilder defaultBaseEventBuilder = createObjectUnderTest();
        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        String testEventType = RandomStringUtils.randomAlphabetic(10);
        Instant timeReceived = Instant.now();
        Map<String, Object> attributes = Collections.emptyMap();
        EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                .withEventType(testEventType)
                .withTimeReceived(timeReceived)
                .withAttributes(attributes)
                .build();

        defaultBaseEventBuilder.withEventMetadata(eventMetadata);
        defaultBaseEventBuilder.withData(data);

        assertThat(defaultBaseEventBuilder.getTimeReceived(), equalTo(timeReceived));
        assertThat(defaultBaseEventBuilder.getData(), equalTo(data));
        assertThat(defaultBaseEventBuilder.getEventType(), equalTo(testEventType));
        assertThat(defaultBaseEventBuilder.getEventMetadataAttributes(), equalTo(attributes));
    }
}
