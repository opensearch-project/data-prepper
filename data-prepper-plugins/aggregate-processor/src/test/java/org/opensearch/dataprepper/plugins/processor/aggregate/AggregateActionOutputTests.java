/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.Event;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.junit.jupiter.api.BeforeEach;
import static org.mockito.Mockito.mock;

import java.util.List;

public class AggregateActionOutputTests {
    @Mock
    private Event event1;

    @BeforeEach
    void setUp() {
        event1 = mock(Event.class);
    }

    AggregateActionOutput createObjectUnderTest(List<Event> events) {
        return new AggregateActionOutput(events);
    }

    @Test
    void test_with_empty_list() {
        AggregateActionOutput actionOutput = createObjectUnderTest(List.of());
        assertThat(actionOutput.getEvents().size(), equalTo(0));
    }

    @Test
    void test_with_non_empty_list() {
        List<Event> eventList = List.of(event1);
        AggregateActionOutput actionOutput = createObjectUnderTest(eventList);
        assertThat(actionOutput.getEvents().size(), equalTo(1));
        assertThat(actionOutput.getEvents(), equalTo(eventList));
    }

}
