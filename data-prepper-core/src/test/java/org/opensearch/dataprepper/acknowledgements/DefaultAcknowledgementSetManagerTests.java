/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.event.DefaultEventBuilder;
import org.opensearch.dataprepper.event.DefaultEventFactory;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import java.util.Map;
import java.util.Collections;
import java.time.Duration;
import org.apache.commons.lang3.RandomStringUtils;

@ExtendWith(MockitoExtension.class)
class DefaultAcknowledgementSetManagerTests {
    private static final Duration TEST_TIMEOUT_MS = Duration.ofMillis(1000);
    DefaultAcknowledgementSetManager acknowledgementSetManager;

    JacksonEvent event1;
    JacksonEvent event2;
    JacksonEvent event3;
    EventHandle eventHandle1;
    EventHandle eventHandle2;
    EventHandle eventHandle3;
    Boolean result;

    @BeforeEach
    void setup() {
        DefaultEventFactory eventFactory = new DefaultEventFactory();
        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        Map<String, Object> attributes = Collections.emptyMap();
        final DefaultEventBuilder eventBuilder = (DefaultEventBuilder) eventFactory.eventBuilder(DefaultEventBuilder.class).withEventMetadataAttributes(attributes).withData(data);
        event1 = (JacksonEvent) eventBuilder.build();
        event2 = (JacksonEvent) eventBuilder.build();
        event3 = (JacksonEvent) eventBuilder.build();
        acknowledgementSetManager = createObjectUnderTest();
        AcknowledgementSet acknowledgementSet1 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT_MS);
        acknowledgementSet1.add(event1);
        acknowledgementSet1.add(event2);
        eventHandle1 = event1.getEventHandle();
        eventHandle2 = event2.getEventHandle();
    }

    DefaultAcknowledgementSetManager createObjectUnderTest() {
        return new DefaultAcknowledgementSetManager(Duration.ofMillis(TEST_TIMEOUT_MS.toMillis() * 2));
    }

    @Test
    void testBasic() {
        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        acknowledgementSetManager.releaseEventReference(eventHandle1, true);
        try {
            Thread.sleep(TEST_TIMEOUT_MS.toMillis() * 5);
        } catch (Exception e){}
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(true));
    }

    @Test
    void testExpirations() {
        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        try {
            Thread.sleep(TEST_TIMEOUT_MS.toMillis() * 5);
        } catch (Exception e){}
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(null));
    }

    @Test
    void testMultipleAcknowledgementSets() {
        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT_MS);
        acknowledgementSet2.add(event3);
        eventHandle3 = event3.getEventHandle();

        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        acknowledgementSetManager.releaseEventReference(eventHandle3, true);
        try {
            Thread.sleep(TEST_TIMEOUT_MS.toMillis() * 5);
        } catch (Exception e){}
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(true));
    }
}
