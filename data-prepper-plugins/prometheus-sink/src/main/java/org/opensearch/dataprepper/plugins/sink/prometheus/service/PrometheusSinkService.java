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

import com.google.common.annotations.VisibleForTesting;

import org.opensearch.dataprepper.common.sink.DefaultSinkOutputStrategy;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.ReentrantLockStrategy;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
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
    static final String PLUGIN_NAME = "prometheus";
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkService.class);
    public static final String PROMETHEUS_SINK_RECORDS_SUCCESS_COUNTER = "prometheusSinkRecordsNumberOfSuccessful";

    public static final String PROMETHEUS_SINK_RECORDS_FAILED_COUNTER = "prometheusSinkRecordsNumberOfFailed";
    private final PrometheusHttpSender httpSender;
    private final PipelineDescription pipelineDescription;
    private final List<Record<Event>> dlqRecords;
    private final boolean sanitizeNames;
    private HeadlessPipeline dlqPipeline;
    private boolean dropIfNoDLQConfigured;
    private String pluginName;

    public PrometheusSinkService(final PrometheusSinkConfiguration prometheusSinkConfiguration,
                                 final SinkMetrics sinkMetrics,
                                 final PrometheusHttpSender httpSender,
                                 final HeadlessPipeline dlqPipeline,
                                 final PipelineDescription pipelineDescription) {
        super(new ReentrantLockStrategy(),
              new PrometheusSinkBuffer(prometheusSinkConfiguration.getThresholdConfig().getMaxEvents(),
                  prometheusSinkConfiguration.getThresholdConfig().getMaxRequestSizeBytes(),
                  prometheusSinkConfiguration.getThresholdConfig().getFlushIntervalMs(),
                  new PrometheusSinkBufferWriter(prometheusSinkConfiguration, sinkMetrics)),
              new PrometheusSinkFlushContext(httpSender),
              sinkMetrics);
        sanitizeNames = prometheusSinkConfiguration.getSanitizeNames();
        this.dropIfNoDLQConfigured = false;
        this.dlqPipeline = dlqPipeline;
        this.dlqRecords = new ArrayList<>();
        this.httpSender = httpSender;
        this.pipelineDescription = pipelineDescription;
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
        if (dlqRecords.isEmpty()) {
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
                .with("pluginName", PLUGIN_NAME)
                .with("pipelineName", pipelineDescription.getPipelineName());
            if (ex != null) {
                event.updateFailureMetadata()
                    .with("message",  ex.getMessage());
            }
            dlqRecords.add(new Record<>(event));
        }
    }
}
