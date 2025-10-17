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
     * Increments the count of spans that were explicitly rejected by the OTLP endpoint.
     *
     * @param count The number of spans rejected.
     */
    public void incrementRejectedSpansCount(final long count) {
        pluginMetrics.counter("rejectedSpansCount").increment(count);
    }

    /**
     * Increments the count of spans that failed to be processed by the sink.
     *
     * @param count The number of spans failed.
     */
    public void incrementFailedSpansCount(final long count) {
        pluginMetrics.counter("failedSpansCount").increment(count);
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
