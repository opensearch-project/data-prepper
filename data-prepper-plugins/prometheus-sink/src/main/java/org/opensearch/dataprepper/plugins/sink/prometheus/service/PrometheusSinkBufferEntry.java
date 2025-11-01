/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;

public class PrometheusSinkBufferEntry implements SinkBufferEntry {

    final Event event;
    final PrometheusTimeSeries timeSeries;

    public PrometheusSinkBufferEntry(final Event event) throws Exception {
        this.event = event;
        timeSeries = getTimeSeriesForEvent();
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

    private PrometheusTimeSeries getTimeSeriesForEvent() throws Exception {
        if (event.getMetadata().getEventType().equals("METRIC")) {
            try {
                PrometheusTimeSeries timeSeries = new PrometheusTimeSeries((JacksonMetric)event);
                if (event instanceof JacksonGauge) {
                    final JacksonGauge jacksonGauge = (JacksonGauge) event;
                    timeSeries.addGaugeMetric(jacksonGauge);
                } else if (event instanceof JacksonSum) {
                    final JacksonSum jacksonSum = (JacksonSum) event;
                    timeSeries.addSumMetric(jacksonSum);
                } else if (event instanceof JacksonSummary) {
                    final JacksonSummary jacksonSummary = (JacksonSummary) event;
                    timeSeries.addSummaryMetric(jacksonSummary);
                } else if (event instanceof JacksonHistogram) {
                    final JacksonHistogram jacksonHistogram = (JacksonHistogram) event;
                    timeSeries.addHistogramMetric(jacksonHistogram);
                } else if (event instanceof JacksonExponentialHistogram) {
                    final JacksonExponentialHistogram jacksonExponentialHistogram = (JacksonExponentialHistogram) event;
                    timeSeries.addExponentialHistogramMetric(jacksonExponentialHistogram);
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
