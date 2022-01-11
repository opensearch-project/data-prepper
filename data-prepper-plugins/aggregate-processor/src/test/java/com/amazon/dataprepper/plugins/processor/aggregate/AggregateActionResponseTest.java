/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AggregateActionResponseTest {

    @Test
    void emptyEventResponse_returns_correct_AggregateActionResponse() {
        final AggregateActionResponse emptyEventResponse = AggregateActionResponse.emptyEventResponse();

        assertThat(emptyEventResponse.getEvent(), equalTo(Optional.empty()));
    }

    @Test
    void AggregateActionResponse_fromEvent_returns_correct_AggregateActionResponse() {
        final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build();

        final AggregateActionResponse aggregateActionResponse = AggregateActionResponse.fromEvent(event);

        assertThat(aggregateActionResponse.getEvent().isPresent(), equalTo(true));
        assertThat(aggregateActionResponse.getEvent().get(), equalTo(event));
    }
}
