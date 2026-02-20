/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;

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

    /**
     * Creates a signal handler for this signal type.
     *
     * @param encoder the encoder to use
     * @return the signal handler
     */
    public OtlpSignalHandler createHandler(final OTelProtoStandardCodec.OTelProtoEncoder encoder) {
        switch (this) {
            case TRACE:
                return new OtlpTraceHandler(encoder);
            case METRIC:
                return new OtlpMetricHandler(encoder);
            case LOG:
                return new OtlpLogHandler(encoder);
            default:
                throw new IllegalStateException("Cannot create handler for signal type: " + this);
        }
    }
}
