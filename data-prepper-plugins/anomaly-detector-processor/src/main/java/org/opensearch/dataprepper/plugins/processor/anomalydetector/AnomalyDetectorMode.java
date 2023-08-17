/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import java.util.List;
import java.util.Collection;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

/**
 * Interface for creating custom anomaly detector modes to be used with the {@link AnomalyDetectorProcessor}.
 * @since 2.1
 */
public interface AnomalyDetectorMode {
    /**
     * Initializes the mode with the keys provided.
     * 
     * @param keys List of keys which are used as dimensions in the anomaly detector
     * @since 2.1
     * 
     * @param verbose Optional, when true, RCF will turn off Auto-Adjust, and anomalies will be continually detected after a level shift
     */
    void initialize(List<String> keys, boolean verbose);

    /**
     * handles a collection of records
     *
     * @param records The collection of records to be passed to the anomaly detector to indentify anomalies
     * @return The list of anomaly events if anomalies found. Returns empty list if no anomalies are found.
     * @since 2.1
     */
    Collection<Record<Event>> handleEvents(Collection<Record<Event>> records);
}
