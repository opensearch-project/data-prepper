/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

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

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProcessWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessWorker.class);

    private static final String INVALID_EVENT_HANDLES = "invalidEventHandles";
    private final Buffer readBuffer;
    private final List<Processor> processors;
    private final Pipeline pipeline;
    private boolean isEmptyRecordsLogged = false;
    private PluginMetrics pluginMetrics;
    private final Counter invalidEventHandlesCounter;
    private boolean acknowledgementsEnabled;

    public ProcessWorker(
            final Buffer readBuffer,
            final List<Processor> processors,
            final Pipeline pipeline) {
        this.readBuffer = readBuffer;
        this.processors = processors;
        this.pipeline = pipeline;
        this.pluginMetrics = PluginMetrics.fromNames("ProcessWorker", pipeline.getName());
        this.invalidEventHandlesCounter = pluginMetrics.counter(INVALID_EVENT_HANDLES);
        this.acknowledgementsEnabled = pipeline.getSource().areAcknowledgementsEnabled() || readBuffer.areAcknowledgementsEnabled();
    }

    @Override
    public void run() {
        try {
            // Phase 1 - execute until stop requested
            while (!pipeline.isStopRequested()) {
                doRun();
            }
            executeShutdownProcess();
        } catch (final Exception e) {
            LOG.error("Encountered exception during pipeline {} processing", pipeline.getName(), e);
        }
    }

    private void executeShutdownProcess() {
        LOG.info("Processor shutdown phase 1 complete.");

        // Phase 2 - execute until buffers are empty
        LOG.info("Beginning processor shutdown phase 2, iterating until buffers empty.");
        while (!isBufferReadyForShutdown()) {
            doRun();
        }
        LOG.info("Processor shutdown phase 2 complete.");

        // Phase 3 - execute until peer forwarder drain period expires (best effort to process all peer forwarder data)
        final long drainTimeoutExpiration = System.currentTimeMillis() + pipeline.getPeerForwarderDrainTimeout().toMillis();
        LOG.info("Beginning processor shutdown phase 3, iterating until {}.", drainTimeoutExpiration);
        while (System.currentTimeMillis() < drainTimeoutExpiration) {
            doRun();
        }
        LOG.info("Processor shutdown phase 3 complete.");

        // Phase 4 - prepare processors for shutdown
        LOG.info("Beginning processor shutdown phase 4, preparing processors for shutdown.");
        processors.forEach(Processor::prepareForShutdown);
        LOG.info("Processor shutdown phase 4 complete.");

        // Phase 5 - execute until processors are ready to shutdown
        LOG.info("Beginning processor shutdown phase 5, iterating until processors are ready to shutdown.");
        while (!areComponentsReadyForShutdown()) {
            doRun();
        }
        LOG.info("Processor shutdown phase 5 complete.");
    }

    private void processAcknowledgements(List<Event> inputEvents, Collection<Record<Event>> outputRecords) {
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
                throw new RuntimeException("Unexpected EventHandle");
            }
        });
    }

    private void doRun() {
        final Map.Entry<Collection, CheckpointState> readResult = readBuffer.read(pipeline.getReadBatchTimeoutInMillis());
        Collection records = readResult.getKey();
        final CheckpointState checkpointState = readResult.getValue();
        //TODO Hacky way to avoid logging continuously - Will be removed as part of metrics implementation
        if (records.isEmpty()) {
            if(!isEmptyRecordsLogged) {
                LOG.debug(" {} Worker: No records received from buffer", pipeline.getName());
                isEmptyRecordsLogged = true;
            }
        } else {
            LOG.debug(" {} Worker: Processing {} records from buffer", pipeline.getName(), records.size());
        }
        //Should Empty list from buffer should be sent to the processors? For now sending as the Stateful processors expects it.
        for (final Processor processor : processors) {

            List<Event> inputEvents = null;
            if (acknowledgementsEnabled) {
                inputEvents = ((List<Record<Event>>) records).stream().map(Record::getData).collect(Collectors.toList());
            }

            try {
                records = processor.execute(records);
                if (inputEvents != null) {
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

        postToSink(records);
        // Checkpoint the current batch read from the buffer after being processed by processors and sinks.
        readBuffer.checkpoint(checkpointState);
    }

    private boolean areComponentsReadyForShutdown() {
        return isBufferReadyForShutdown() && processors.stream()
                .map(Processor::isReadyForShutdown)
                .allMatch(result -> result == true);
    }

    private boolean isBufferReadyForShutdown() {
        final boolean isBufferEmpty = readBuffer.isEmpty();
        final boolean forceStopReadingBuffers = pipeline.isForceStopReadingBuffers();
        final boolean isBufferReadyForShutdown = isBufferEmpty || forceStopReadingBuffers;
        LOG.debug("isBufferReadyForShutdown={}, isBufferEmpty={}, forceStopReadingBuffers={}", isBufferReadyForShutdown, isBufferEmpty, forceStopReadingBuffers);
        return isBufferReadyForShutdown;
    }

    /**
     * TODO Add isolator pattern - Fail if one of the Sink fails [isolator Pattern]
     * Uses the pipeline method to publish to sinks, waits for each of the sink result to be true before attempting to
     * process more records from buffer.
     */
    private boolean postToSink(final Collection<Record> records) {
        LOG.debug("Pipeline Worker: Submitting {} processed records to sinks", records.size());
        final List<Future<Void>> sinkFutures = pipeline.publishToSinks(records);
        final FutureHelperResult<Void> futureResults = FutureHelper.awaitFuturesIndefinitely(sinkFutures);
        return futureResults.getFailedReasons().size() == 0;
    }
}
