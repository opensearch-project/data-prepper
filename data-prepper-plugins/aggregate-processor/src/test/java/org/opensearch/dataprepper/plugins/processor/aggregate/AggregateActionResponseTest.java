/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AggregateActionResponseTest {

    @Test
    void nullEventResponse_returns_correct_AggregateActionResponse() {
        final AggregateActionResponse emptyEventResponse = AggregateActionResponse.nullEventResponse();

        assertThat(emptyEventResponse.getEvent(), equalTo(null));
    }

    @Test
    void AggregateActionResponse_fromEvent_returns_correct_AggregateActionResponse() {
        final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build();

        final AggregateActionResponse aggregateActionResponse = AggregateActionResponse.fromEvent(event);

        assertThat(aggregateActionResponse.getEvent(), equalTo(event));
    }
}
