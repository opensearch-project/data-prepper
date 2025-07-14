/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.failures;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
public interface FailurePipeline {
    void sendFailedEvents(Collection<Record<Event>> events);
}
