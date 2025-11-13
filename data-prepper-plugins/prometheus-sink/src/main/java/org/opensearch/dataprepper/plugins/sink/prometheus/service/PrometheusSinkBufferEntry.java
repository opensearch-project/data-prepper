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
import org.opensearch.dataprepper.model.metric.ExponentialHistogram;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;

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
            try {
                PrometheusTimeSeries timeSeries = new PrometheusTimeSeries((Metric)event, sanitizeNames);
                if (event instanceof Gauge) {
                    final Gauge gauge = (Gauge) event;
                    timeSeries.addGaugeMetric(gauge);
                } else if (event instanceof Sum) {
                    final Sum sum = (Sum) event;
                    timeSeries.addSumMetric(sum);
                } else if (event instanceof Summary) {
                    final Summary summary = (Summary) event;
                    timeSeries.addSummaryMetric(summary);
                } else if (event instanceof Histogram) {
                    final Histogram histogram = (Histogram) event;
                    timeSeries.addHistogramMetric(histogram);
                } else if (event instanceof ExponentialHistogram) {
                    final ExponentialHistogram exponentialHistogram = (ExponentialHistogram) event;
                    timeSeries.addExponentialHistogramMetric(exponentialHistogram);
                } else {
                    throw new RuntimeException("Unknown metric type");
                }
                return timeSeries;
            } catch (Exception e) {
                throw e;
            }
        }
        throw new RuntimeException("Not metric type");
    }

}
