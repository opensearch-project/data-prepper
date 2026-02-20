/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;

/**
 * A central metrics facade for the OTLP sink plugin.
 * <p>
 * All OTLP sink components should use this class to record and expose common metrics
 * such as record counts, latencies, payload sizes, and queue statistics.
 * </p>
 */
public class OtlpSinkMetrics {

    private final PluginMetrics pluginMetrics;
    private final Timer httpLatency;
    private final DistributionSummary payloadSize;
    private final DistributionSummary payloadGzipSize;

    /**
     * Constructor for OtlpSinkMetrics
     *
     * @param pluginMetrics The plugin metrics
     * @param pluginSetting The plugin setting
     */
    public OtlpSinkMetrics(@Nonnull final PluginMetrics pluginMetrics, @Nonnull final PluginSetting pluginSetting) {
        this.pluginMetrics = pluginMetrics;

        httpLatency = pluginMetrics.timer("httpLatency");

        payloadSize = pluginMetrics.summary("payloadSize");
        payloadGzipSize = pluginMetrics.summary("payloadGzipSize");
    }

    public void incrementRecordsOut(final long count) {
        pluginMetrics.counter("recordsOut").increment(count);
    }

    public void incrementErrorsCount() {
        pluginMetrics.counter("errorsCount").increment(1);
    }

    public void incrementPayloadSize(final long bytes) {
        payloadSize.record(bytes);
    }

    public void incrementPayloadGzipSize(final long bytes) {
        payloadGzipSize.record(bytes);
    }

    public void recordHttpLatency(final long durationMillis) {
        httpLatency.record(Duration.ofMillis(durationMillis));
    }

    public void registerQueueGauges(final BlockingQueue<?> queue) {
        pluginMetrics.gauge("queueSize", queue, BlockingQueue::size);
        pluginMetrics.gauge("queueCapacity", queue, q -> q.remainingCapacity() + q.size());
    }

    /**
     * Increments the count of records that were explicitly rejected by the OTLP endpoint.
     * This applies to all signal types: spans, metrics, and logs.
     *
     * @param count The number of records rejected.
     */
    public void incrementRejectedRecordsCount(final long count) {
        pluginMetrics.counter("rejectedRecordsCount").increment(count);
    }

    /**
     * Increments the count of records that failed to be processed by the sink.
     * This applies to all signal types: spans, metrics, and logs.
     *
     * @param count The number of records failed.
     */
    public void incrementFailedRecordsCount(final long count) {
        pluginMetrics.counter("failedRecordsCount").increment(count);
    }

    /**
     * Records the response code in the metrics.
     * Group response codes by category: 2xx, 4xx, 5xx, etc.
     *
     * @param statusCode The HTTP response code.
     */
    public void recordResponseCode(final int statusCode) {
        final String codeCategory = (statusCode / 100) + "xx";
        pluginMetrics.counter("http" + codeCategory + "Responses").increment();
    }
}
