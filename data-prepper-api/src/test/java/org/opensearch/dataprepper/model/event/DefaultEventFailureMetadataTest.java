/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import java.util.UUID;

public class DefaultEventFailureMetadataTest {
    
    @Test
    public void testDefaultEventFailureMetadata() {
        String eventType = UUID.randomUUID().toString();

        Event event = JacksonEvent.builder()
                .withEventType(eventType)
                .build();

        EventFailureMetadata eventFailureMetadata = new DefaultEventFailureMetadata(event);
        eventFailureMetadata.with("key1", "value1").with("key2", 2);
        assertThat(event.get(DefaultEventFailureMetadata.FAILURE_METADATA+"/key1", String.class), equalTo("value1"));
        assertThat(event.get(DefaultEventFailureMetadata.FAILURE_METADATA+"/key2", Integer.class), equalTo(2));
    }
}
