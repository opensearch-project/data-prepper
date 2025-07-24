/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.core.pipeline.common.FutureHelper;
import org.opensearch.dataprepper.core.pipeline.common.FutureHelperResult;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class PipelineRunnerImpl implements PipelineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineRunnerImpl.class);
    private static final String INVALID_EVENT_HANDLES = "invalidEventHandles";
    private final Pipeline pipeline;
    private final PluginMetrics pluginMetrics;
    private final List<Processor> processors;
    private boolean isEmptyRecordsLogged = false;
    @VisibleForTesting final Counter invalidEventHandlesCounter;

    public PipelineRunnerImpl(final Pipeline pipeline, final List<Processor> processors) {
        this.pipeline = pipeline;
        this.pluginMetrics = PluginMetrics.fromNames("PipelineRunner", pipeline.getName());
        this.processors = processors;
        this.invalidEventHandlesCounter = pluginMetrics.counter(INVALID_EVENT_HANDLES);
    }

    @Override
    public void runAllProcessorsAndPublishToSinks() {
        final Map.Entry<Collection, CheckpointState> recordsReadFromBuffer = readFromBuffer(getBuffer(), getPipeline());
        Collection records = recordsReadFromBuffer.getKey();
        final CheckpointState checkpointState = recordsReadFromBuffer.getValue();
        records = runProcessorsAndProcessAcknowledgements(processors, records);
        postToSink(getPipeline(), records);
        // Checkpoint the current batch read from the buffer after being processed by processors and sinks.
        getBuffer().checkpoint(checkpointState);
    }

    @VisibleForTesting
    Map.Entry<Collection, CheckpointState> readFromBuffer(Buffer buffer, Pipeline pipeline) {
        final Map.Entry<Collection, CheckpointState> readResult = buffer.read(pipeline.getReadBatchTimeoutInMillis());
        Collection records = readResult.getKey();
        //TODO Hacky way to avoid logging continuously - Will be removed as part of metrics implementation
        if (records.isEmpty()) {
            if(!isEmptyRecordsLogged) {
                LOG.debug(" {} Worker: No records received from buffer", pipeline.getName());
                isEmptyRecordsLogged = true;
            }
        } else {
            LOG.debug(" {} Worker: Processing {} records from buffer", pipeline.getName(), records.size());
        }
        return readResult;
    }

    @VisibleForTesting
    void processAcknowledgements(List<Event> inputEvents, Collection<Record<Event>> outputRecords) {
        Set<Event> outputEventsSet = outputRecords.stream().map(Record::getData).collect(Collectors.toSet());
        // For each event in the input events list that is not present in the output events, send positive acknowledgement, if acknowledgements are enabled for it
        inputEvents.forEach(event -> {
            EventHandle eventHandle = event.getEventHandle();
            if (eventHandle != null && eventHandle instanceof DefaultEventHandle) {
                InternalEventHandle internalEventHandle = (InternalEventHandle)(DefaultEventHandle)eventHandle;
                if (!outputEventsSet.contains(event)) {
                    eventHandle.release(true);
                }
            } else if (eventHandle != null) {
                invalidEventHandlesCounter.increment();
            }
        });
    }

    @VisibleForTesting
    Collection runProcessorsAndProcessAcknowledgements(List<Processor> processors, Collection records) {
        //Should Empty list from buffer should be sent to the processors? For now sending as the Stateful processors expects it.
        for (final Processor processor : processors) {

            List<Event> inputEvents = null;
            if (getPipeline().areAcknowledgementsEnabled()) {
                inputEvents = ((List<Record<Event>>) records).stream().map(Record::getData).collect(Collectors.toList());
            }

            try {
                records = processor.execute(records);
                // acknowledge missing events only if the processor is not holding events
                if (!processor.holdsEvents() && inputEvents != null) {
                    processAcknowledgements(inputEvents, records);
                }
            } catch (final Exception e) {
                LOG.error("A processor threw an exception. This batch of Events will be dropped, and their EventHandles will be released: ", e);
                if (inputEvents != null) {
                    processAcknowledgements(inputEvents, Collections.emptyList());
                }

                records = Collections.emptyList();
                break;
            }
        }
        return records;
    }

    /**
     * TODO Add isolator pattern - Fail if one of the Sink fails [isolator Pattern]
     * Uses the pipeline method to publish to sinks, waits for each of the sink result to be true before attempting to
     * process more records from buffer.
     */

    @VisibleForTesting
    boolean postToSink(final Pipeline pipeline, final Collection<Record> records) {
        LOG.debug("Pipeline Worker: Submitting {} processed records to sinks", records.size());
        final List<Future<Void>> sinkFutures = pipeline.publishToSinks(records);
        final FutureHelperResult<Void> futureResults = FutureHelper.awaitFuturesIndefinitely(sinkFutures);
        return futureResults.getFailedReasons().size() == 0;
    }

    @Override
    public Pipeline getPipeline() {
        return pipeline;
    }

    @VisibleForTesting
    Buffer getBuffer() {
        return getPipeline().getBuffer();
    }
}
