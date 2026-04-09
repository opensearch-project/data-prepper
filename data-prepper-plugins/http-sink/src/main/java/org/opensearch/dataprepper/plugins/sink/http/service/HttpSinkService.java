/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import org.opensearch.dataprepper.common.sink.DefaultSinkOutputStrategy;
import org.opensearch.dataprepper.common.sink.ReentrantLockStrategy;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.sink.http.HttpSinkSender;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HttpSinkService extends DefaultSinkOutputStrategy {
    static final String PLUGIN_NAME = "http";
    private final List<Record<Event>> dlqRecords;
    private final PipelineDescription pipelineDescription;
    private final OutputCodec codec;
    private final OutputCodecContext codecContext;
    private HeadlessPipeline dlqPipeline;
    private boolean dropIfNoDLQConfigured;

    public HttpSinkService(final HttpSinkConfiguration httpSinkConfiguration,
                           final SinkMetrics sinkMetrics,
                           final HttpSinkSender httpSender,
                           final PipelineDescription pipelineDescription,
                           final OutputCodec codec,
                           final OutputCodecContext codecContext) {
        super(new ReentrantLockStrategy(),
              new HttpSinkBuffer(
                  httpSinkConfiguration.getThresholdOptions().getMaxEvents(),
                  httpSinkConfiguration.getThresholdOptions().getMaxRequestSize().getBytes(),
                  httpSinkConfiguration.getThresholdOptions().getFlushTimeOut().toMillis(),
                  new HttpSinkBufferWriter(sinkMetrics)),
              new HttpSinkFlushContext(httpSender, codec, codecContext),
              sinkMetrics);
        this.dlqRecords = new ArrayList<>();
        this.pipelineDescription = pipelineDescription;
        this.codec = codec;
        this.codecContext = codecContext;
        this.dropIfNoDLQConfigured = false;
    }

    @Override
    public SinkBufferEntry getSinkBufferEntry(final Event event) throws Exception {
        return new HttpSinkBufferEntry(event, codec, codecContext);
    }

    public void setDlqPipeline(final HeadlessPipeline pipeline) {
        this.dlqPipeline = pipeline;
    }

    @Override
    public void flushDlqList() {
        if (dlqRecords.isEmpty()) {
            return;
        }
        if (dlqPipeline != null) {
            dlqPipeline.sendEvents(dlqRecords);
        }
        dlqRecords.clear();
    }

    @Override
    public void addFailedEventsToDlq(final List<Event> failedEvents, final Throwable ex, final int statusCode) {
        for (final Event event : failedEvents) {
            if (dlqPipeline == null) {
                event.getEventHandle().release(dropIfNoDLQConfigured);
                continue;
            }
            event.updateFailureMetadata()
                .with("statusCode", statusCode)
                .withPluginName(PLUGIN_NAME)
                .withPipelineName(pipelineDescription.getPipelineName());
            if (ex != null) {
                event.updateFailureMetadata()
                    .withErrorMessage(ex.getMessage());
            }
            dlqRecords.add(new Record<>(event));
        }
    }

    public void output(final Collection<Record<Event>> records) {
        execute(records);
    }
}

