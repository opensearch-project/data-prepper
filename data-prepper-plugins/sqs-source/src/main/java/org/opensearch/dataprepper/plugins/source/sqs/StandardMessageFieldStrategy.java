/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import java.util.Collections;
import java.util.List;

public class StandardMessageFieldStrategy implements MessageFieldStrategy {
    @Override
    public List<Event> parseEvents(final String messageBody) {
        final Event event = JacksonEvent.builder()
                .withEventType("DOCUMENT")
                .withData(Collections.singletonMap("message", messageBody))
                .build();
        return Collections.singletonList(event);
    }
}
