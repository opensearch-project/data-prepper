/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.List;
import java.util.function.BiConsumer;

public interface ResponseEventHandlingStrategy {

    List<Record<Event>> handleEvents(List<Event> parsedEvents, List<Record<Event>> originalRecords, BiConsumer<Event, Event> consumer);
}
