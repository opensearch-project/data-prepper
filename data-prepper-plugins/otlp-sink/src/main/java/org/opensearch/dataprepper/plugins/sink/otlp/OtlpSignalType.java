/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.trace.Span;

/**
 * Enum representing the different OTLP signal types.
 */
public enum OtlpSignalType {
    TRACE,
    METRIC,
    LOG,
    UNKNOWN;

    /**
     * Determines the signal type from an Event object.
     *
     * @param event the event to classify
     * @return the signal type
     */
    public static OtlpSignalType fromEvent(final Event event) {
        if (event instanceof Span) {
            return TRACE;
        } else if (event instanceof Metric) {
            return METRIC;
        } else if (event instanceof Log) {
            return LOG;
        }
        return UNKNOWN;
    }
}
