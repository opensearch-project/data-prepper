package com.amazon.dataprepper.pipeline;

import com.amazon.dataprepper.model.CheckpointState;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.pipeline.common.FutureHelper;
import com.amazon.dataprepper.pipeline.common.FutureHelperResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProcessWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessWorker.class);

    private final Buffer readBuffer;
    private final List<Prepper> preppers;
    private final Collection<Sink> sinks;
    private final Pipeline pipeline;
    private boolean isEmptyRecordsLogged = false;

    public ProcessWorker(
            final Buffer readBuffer,
            final List<Prepper> preppers,
            final Collection<Sink> sinks,
            final Pipeline pipeline) {
        this.readBuffer = readBuffer;
        this.preppers = preppers;
        this.sinks = sinks;
        this.pipeline = pipeline;
    }

    @Override
    public void run() {
        try {
            do {
                final Map.Entry<Collection, CheckpointState> readResult = readBuffer.read(pipeline.getReadBatchTimeoutInMillis());
                Collection records = readResult.getKey();
                final CheckpointState checkpointState = readResult.getValue();
                //TODO Hacky way to avoid logging continuously - Will be removed as part of metrics implementation
                if (records.isEmpty()) {
                    if(!isEmptyRecordsLogged) {
                        LOG.info(" {} Worker: No records received from buffer", pipeline.getName());
                        isEmptyRecordsLogged = true;
                    }
                } else {
                    LOG.info(" {} Worker: Processing {} records from buffer", pipeline.getName(), records.size());
                }
                //Should Empty list from buffer should be sent to the preppers? For now sending as the Stateful preppers expects it.
                for (final Prepper prepper : preppers) {
                    records = prepper.execute(records);
                }
                if (!records.isEmpty()) {
                    postToSink(records);
                }
                // Checkpoint the current batch read from the buffer after being processed by prepper and sinks.
                readBuffer.checkpoint(checkpointState);
            } while (!shouldStop());
        } catch (final Exception e) {
            LOG.error("Encountered exception during pipeline {} processing", pipeline.getName(), e);
        }
    }

    /**
     * Shutdown should be handled end to end.
     *
     * @return
     */
    private boolean shouldStop() {
        return pipeline.isStopRequested() && areComponentsReadyForShutdown();
    }

    private boolean areComponentsReadyForShutdown() {
        return readBuffer.isEmpty() && preppers.stream()
                .map(Prepper::isReadyForShutdown)
                .allMatch(result -> result == true);
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
