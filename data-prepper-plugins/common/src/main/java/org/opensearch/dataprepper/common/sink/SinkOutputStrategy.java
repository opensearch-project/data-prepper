/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;


import java.util.Collection;

public interface SinkOutputStrategy {
    public void execute(Collection<Record<Event>> records);
    public void flushDLQList();
    public void addEventToDLQList(final Event event, Throwable ex);
}


