/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

public interface SinkDLQHandler {
    void flushDLQList();
    void addFailedEventsToDLQ(final List<Event> events, final Throwable ex);
    
}
