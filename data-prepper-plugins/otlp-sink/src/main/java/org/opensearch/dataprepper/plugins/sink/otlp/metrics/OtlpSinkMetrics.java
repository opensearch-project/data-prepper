/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.metrics;

import org.opensearch.dataprepper.metrics.PluginMetrics;

import javax.annotation.Nonnull;
import java.time.Duration;

/**
 * Metrics class for the otlp-sink
 */
public class OtlpSinkMetrics {

    private final PluginMetrics pluginMetrics;

    /**
     * Constructor for OtlpSinkMetrics
     *
     * @param pluginMetrics The plugin metrics instance
     */
    public OtlpSinkMetrics(@Nonnull final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
    }

    public void incrementRecordsIn(long count) {
        pluginMetrics.counter("recordsIn").increment(count);
    }

    public void incrementRecordsOut(long count) {
        pluginMetrics.counter("recordsOut").increment(count);
    }

    public void incrementDroppedRecords(long count) {
        pluginMetrics.counter("droppedRecords").increment(count);
    }

    public void incrementErrorsCount() {
        pluginMetrics.counter("errorsCount").increment(1);
    }

    public void incrementPayloadSize(long bytes) {
        pluginMetrics.summary("payloadSize").record(bytes);
    }

    public void recordDeliveryLatency(long durationMillis) {
        pluginMetrics.timer("deliveryLatency").record(Duration.ofMillis(durationMillis));
    }

    public void recordHttpLatency(long durationMillis) {
        pluginMetrics.timer("httpLatency").record(Duration.ofMillis(durationMillis));
    }

    public void incrementRetriesCount() {
        pluginMetrics.counter("retriesCount").increment(1);
    }

    public void incrementRejectedSpansCount(long count) {
        pluginMetrics.counter("rejectedSpansCount").increment(count);
    }

    /**
     * Records the response code in the metrics.
     * Group response codes by category: 2xx, 4xx, 5xx, etc.
     *
     * @param statusCode The HTTP response code.
     */
    public void recordResponseCode(final int statusCode) {
        String codeCategory = (statusCode / 100) + "xx";
        pluginMetrics.counter("http_" + codeCategory + "_responses").increment();
    }
}
