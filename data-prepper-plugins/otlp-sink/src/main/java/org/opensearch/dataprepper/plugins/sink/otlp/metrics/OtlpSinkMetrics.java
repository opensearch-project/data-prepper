/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
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
    private final Timer deliveryLatency;
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

        final String pipelineName = pluginSetting.getPipelineName();
        final String pluginName = pluginSetting.getName();

        httpLatency = buildLatencyTimer(pipelineName, pluginName, "httpLatency");
        deliveryLatency = buildLatencyTimer(pipelineName, pluginName, "deliveryLatency");

        payloadSize = buildDistributionSummary(pipelineName, pluginName, "payloadSize");
        payloadGzipSize = buildDistributionSummary(pipelineName, pluginName, "payloadGzipSize");
    }

    /**
     * Builds a timer for latency metrics with percentiles
     *
     * @param pipelineName The pipeline name
     * @param pluginName   The plugin name
     * @param metricName   The metric name
     * @return The timer
     */
    private static Timer buildLatencyTimer(@Nonnull final String pipelineName, @Nonnull final String pluginName, @Nonnull final String metricName) {
        return Timer.builder(String.format("%s_%s_%s", pipelineName, pluginName, metricName))
                .publishPercentiles(0.5, 0.9, 0.95, 0.99, 1.0)
                .publishPercentileHistogram(true)
                .distributionStatisticBufferLength(1024)
                .distributionStatisticExpiry(Duration.ofMinutes(10))
                .register(Metrics.globalRegistry);
    }

    /**
     * Builds a distribution summary for payload size metrics with percentiles
     *
     * @param pipelineName The pipeline name
     * @param pluginName   The plugin name
     * @param metricName   The metric name
     * @return The distribution summary
     */
    private static DistributionSummary buildDistributionSummary(@Nonnull final String pipelineName, @Nonnull final String pluginName, @Nonnull final String metricName) {
        return DistributionSummary.builder(String.format("%s_%s_%s", pipelineName, pluginName, metricName))
                .baseUnit("bytes")
                .publishPercentiles(0.5, 0.9, 0.95, 0.99, 1.0)
                .publishPercentileHistogram(true)
                .distributionStatisticBufferLength(1024)
                .distributionStatisticExpiry(Duration.ofMinutes(10))
                .register(Metrics.globalRegistry);
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

    public void recordDeliveryLatency(final long durationMillis) {
        deliveryLatency.record(Duration.ofMillis(durationMillis));
    }

    public void recordHttpLatency(final long durationMillis) {
        httpLatency.record(Duration.ofMillis(durationMillis));
    }

    public void incrementRetriesCount() {
        pluginMetrics.counter("retriesCount").increment(1);
    }

    public void incrementRejectedSpansCount(final long count) {
        pluginMetrics.counter("rejectedSpansCount").increment(count);
    }

    public void registerQueueGauges(final BlockingQueue<?> queue) {
        pluginMetrics.gauge("queueSize", queue, BlockingQueue::size);
        pluginMetrics.gauge("queueCapacity", queue, q -> q.remainingCapacity() + q.size());
    }

    /**
     * Records the response code in the metrics.
     * Group response codes by category: 2xx, 4xx, 5xx, etc.
     *
     * @param statusCode The HTTP response code.
     */
    public void recordResponseCode(final int statusCode) {
        final String codeCategory = (statusCode / 100) + "xx";
        pluginMetrics.counter("http_" + codeCategory + "_responses").increment();
    }
}
