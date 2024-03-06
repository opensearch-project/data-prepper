/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.log.JacksonLog;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class DefaultLogEventBuilderFactoryTests {
    private DefaultLogEventBuilderFactory defaultLogEventBuilderFactory;

    public DefaultLogEventBuilderFactory createObjectUnderTest() {
        return new DefaultLogEventBuilderFactory();
    }

    @BeforeEach
    public void setup() {
        defaultLogEventBuilderFactory = createObjectUnderTest();
    }

    @Test
    public void Test_getEventClass_returns_LogEventBuilder() {
        assertThat(defaultLogEventBuilderFactory.getEventClass(), equalTo(LogEventBuilder.class));
    }

    @Test
    public void testBasic() {
        DefaultBaseEventBuilder baseEventBuilder = defaultLogEventBuilderFactory.createNew();
        assertThat(baseEventBuilder.getEventType(), equalTo(DefaultLogEventBuilderFactory.LOG_EVENT_TYPE));

        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        Map<String, Object> attributes = Collections.emptyMap();
        LogEventBuilder eventBuilder = (LogEventBuilder) baseEventBuilder.withEventMetadataAttributes(attributes).withData(data);

        JacksonLog log = (JacksonLog) eventBuilder.build();
        EventMetadata eventMetadata = log.getMetadata();
        assertThat(eventMetadata.getTimeReceived(), not(equalTo(null)));
        assertThat(eventMetadata.getEventType(), equalTo(DefaultLogEventBuilderFactory.LOG_EVENT_TYPE));
        assertThat(eventMetadata.getAttributes(), equalTo(attributes));
        assertThat(log.toMap(), equalTo(data));
    }
}

