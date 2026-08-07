 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.metric.Metric;

public class PrometheusSinkBufferEntry implements SinkBufferEntry {

    final Event event;
    final PrometheusTimeSeries timeSeries;

    public PrometheusSinkBufferEntry(final Event event, final boolean sanitizeNames) throws Exception {
        this.event = event;
        timeSeries = getTimeSeriesForEvent(sanitizeNames);
    }

    public PrometheusTimeSeries getTimeSeries() {
        return timeSeries;
    }

    @Override
    public long getEstimatedSize() {
        return timeSeries.getSize();
    }

    @Override
    public boolean exceedsMaxEventSizeThreshold() {
        return false;
    }

    @Override
    public Event getEvent() {
        return event;
    }

    private PrometheusTimeSeries getTimeSeriesForEvent(final boolean sanitizeNames) throws Exception {
        if (event.getMetadata().getEventType().equals(EventType.METRIC.toString())) {
            return new PrometheusTimeSeries((Metric)event, sanitizeNames);
        }
        throw new RuntimeException("Not metric type");
    }

}
