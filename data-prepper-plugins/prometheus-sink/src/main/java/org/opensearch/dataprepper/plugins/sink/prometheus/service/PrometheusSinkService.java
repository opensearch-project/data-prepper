/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import com.google.common.annotations.VisibleForTesting;

import org.opensearch.dataprepper.common.sink.DefaultSinkOutputStrategy;
import org.opensearch.dataprepper.common.sink.DefaultSinkBuffer;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.ReentrantLockStrategy;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusHttpSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class PrometheusSinkService extends DefaultSinkOutputStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkService.class);
    public static final String PROMETHEUS_SINK_RECORDS_SUCCESS_COUNTER = "prometheusSinkRecordsNumberOfSuccessful";

    public static final String PROMETHEUS_SINK_RECORDS_FAILED_COUNTER = "prometheusSinkRecordsNumberOfFailed";
    private final PrometheusHttpSender httpSender;
    private final PluginSetting pluginSetting;
    private final List<Record<Event>> dlqRecords;
    private final boolean sanitizeNames;
    private HeadlessPipeline dlqPipeline;
    private boolean dropIfNoDLQConfigured;

    public PrometheusSinkService(final PrometheusSinkConfiguration prometheusSinkConfiguration,
                                 final SinkMetrics sinkMetrics,
                                 final PrometheusHttpSender httpSender,
                                 final HeadlessPipeline dlqPipeline,
                                 final PluginMetrics pluginMetrics,
                                 final PluginSetting pluginSetting) {
        super(new ReentrantLockStrategy(),
              new DefaultSinkBuffer(prometheusSinkConfiguration.getThresholdConfig().getMaxEvents(),
                  prometheusSinkConfiguration.getThresholdConfig().getMaxRequestSizeBytes(),
                  prometheusSinkConfiguration.getThresholdConfig().getFlushIntervalMs(),
                  new PrometheusSinkBufferWriter(sinkMetrics)),
              new PrometheusSinkFlushContext(httpSender),
              sinkMetrics);
        sanitizeNames = prometheusSinkConfiguration.getSanitizeNames();
        this.dropIfNoDLQConfigured = false;
        this.dlqPipeline = dlqPipeline;
        this.dlqRecords = new ArrayList<>();
        this.httpSender = httpSender;
        this.pluginSetting = pluginSetting;
    }

    public void addFailedEventsToDLQ(final List<Event> events, final Throwable ex) {
        for (final Event event: events) {
            dlqRecords.add(new Record<>(event));
        }
    }

    public SinkBufferEntry getSinkBufferEntry(final Event event) throws Exception {
        return new PrometheusSinkBufferEntry(event, sanitizeNames);
    }

    @VisibleForTesting
    public void setDlqPipeline(HeadlessPipeline pipeline) {
        this.dlqPipeline = pipeline;
    }

    public void flushDlqList() {
        if (dlqRecords.size() == 0) {
            return;
        }
        if (dlqPipeline != null) {
            dlqPipeline.sendEvents(dlqRecords);
        }
        dlqRecords.clear();
    }


    public void output(final Collection<Record<Event>> records) {
        execute(records);
    }

    public void addFailedEventsToDlq(final List<Event> failedEvents, final Throwable ex, final int statusCode) {
        for (final Event event: failedEvents) {
            if (dlqPipeline == null) {
                event.getEventHandle().release(dropIfNoDLQConfigured);
                continue;
            }
            event.updateFailureMetadata()
                .with("statusCode", statusCode)
                .with("pluginName", pluginSetting.getName())
                .with("pipelineName", pluginSetting.getPipelineName());
            if (ex != null) {
                event.updateFailureMetadata()
                    .with("message",  ex.getMessage());
            }
            dlqRecords.add(new Record<>(event));
        }
    }
}
