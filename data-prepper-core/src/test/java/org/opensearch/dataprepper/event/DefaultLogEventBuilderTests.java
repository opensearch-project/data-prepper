/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.RandomStringUtils;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

import java.util.Collections;
import java.util.Map;

class DefaultLogEventBuilderTests {

    private DefaultLogEventBuilder createObjectUnderTest(Map<String, Object> attributes, Map<String, Object> data) {
        return (DefaultLogEventBuilder) new DefaultLogEventBuilder()
                                        .withEventMetadataAttributes(attributes)
                                        .withData(data);
    }

    @Test
    void testDefaultLogEventBuilder() {
        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> metadataAttributes = Collections.emptyMap();
        Map<String, Object> data = Map.of(testKey, testValue);
        DefaultLogEventBuilder defaultLogEventBuilder = createObjectUnderTest(metadataAttributes, data);
        JacksonLog log = (JacksonLog)defaultLogEventBuilder.build();
        EventMetadata eventMetadata = log.getMetadata();
        
        assertThat(eventMetadata.getTimeReceived(), not(equalTo(null)));
        assertThat(eventMetadata.getEventType(), equalTo(DefaultLogEventBuilder.LOG_EVENT_TYPE));
        assertThat(eventMetadata.getAttributes(), equalTo(metadataAttributes));
        assertThat(log.toMap(), equalTo(data));
    }
}
