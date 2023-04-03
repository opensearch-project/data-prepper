/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Map;
import java.util.Collections;

class DefaultEventBuilderFactoryTests {
    private DefaultEventBuilderFactory defaultEventBuilderFactory;

    public DefaultEventBuilderFactory createObjectUnderTest() {
        return new DefaultEventBuilderFactory();
    }

    @BeforeEach
    public void setup() {
        defaultEventBuilderFactory = createObjectUnderTest();
    }

    @Test
    public void Test_getEventClass_returns_EventBuilder() {
        assertThat(defaultEventBuilderFactory.getEventClass(), equalTo(EventBuilder.class));
    }

    @Test
    public void testBasic() {
        DefaultBaseEventBuilder baseEventBuilder = defaultEventBuilderFactory.createNew();
        assertThat(baseEventBuilder.getEventType(), equalTo(DefaultEventBuilderFactory.EVENT_TYPE));

        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        Map<String, Object> attributes = Collections.emptyMap();
        EventBuilder eventBuilder = (EventBuilder)baseEventBuilder.withEventMetadataAttributes(attributes).withData(data);

        JacksonEvent event = (JacksonEvent) eventBuilder.build();
        EventMetadata eventMetadata = event.getMetadata();
        assertThat(eventMetadata.getTimeReceived(), not(equalTo(null)));
        assertThat(eventMetadata.getEventType(), equalTo(DefaultEventBuilderFactory.EVENT_TYPE));
        assertThat(eventMetadata.getAttributes(), equalTo(attributes));
        assertThat(event.toMap(), equalTo(data));
    }
}
