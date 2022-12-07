/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import java.util.List;
import java.util.Collection;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

public interface AnomalyDetectorMode {
    public void initialize(List<String> keys);
    public Collection<Record<Event>> handleEvents(Collection<Record<Event>> record);
}
