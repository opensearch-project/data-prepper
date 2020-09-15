package com.amazon.situp.pipeline;

import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.sink.Sink;
import com.amazon.situp.pipeline.common.FutureHelper;
import com.amazon.situp.pipeline.common.FutureHelperResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProcessWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessWorker.class);

    private final Buffer readBuffer;
    private final List<Processor> processors;
    private final Collection<Sink> sinks;
    private final Pipeline pipeline;
    private boolean isQueueEmpty = false;

    public ProcessWorker(
            final Buffer readBuffer,
            final List<Processor> processors,
            final Collection<Sink> sinks,
            final Pipeline pipeline) {
        this.readBuffer = readBuffer;
        this.processors = processors;
        this.sinks = sinks;
        this.pipeline = pipeline;
    }

    @Override
    public void run() {
        try {
            do {
                Thread.sleep(0);
                Collection records = readBuffer.read(pipeline.getReadBatchTimeoutInMillis());
                if (records != null && !records.isEmpty()) {
                    LOG.debug("Pipeline Worker: Processing {} records from buffer", records.size());
                    for (final Processor processor : processors) {
                        records = processor.execute(records);
                    }
                    postToSink(records); //TODO use the response to ack the buffer
                } else {
                    isQueueEmpty = true;
                }
            } while (!pipeline.isStopRequested() || !isBufferEmpty()); //If pipeline is stopped, we try to empty the
            // already buffered records ?
        } catch (final Exception ex) {
            LOG.error("Encountered exception during pipeline processing", ex); //do not halt the execution
        }
    }

    /**
     * TODO Implement this from Buffer [Probably AtomicBoolean]
     *
     * @return
     */
    private boolean isBufferEmpty() {
        return isQueueEmpty;
    }

    /**
     * TODO Add isolator pattern - Fail if one of the Sink fails [isolator Pattern]
     * Uses the pipeline method to publish to sinks, waits for each of the sink result to be true before attempting to
     * process more records from buffer.
     */
    private boolean postToSink(final Collection<Record> records) {
        LOG.debug("Pipeline Worker: Submitting {} processed records to sinks", records.size());
        boolean someSinksFailed;
        final List<Future<Boolean>> sinkFutures = pipeline.publishToSinks(records);
        final FutureHelperResult<Boolean> futureResults = FutureHelper.awaitFuturesIndefinitely(sinkFutures);
        someSinksFailed = futureResults.getFailedReasons().size() != 0;
        for(Boolean sinkResult : futureResults.getSuccessfulResults()) {
            if(!sinkResult) {
                return false;
            }
        }
        return !someSinksFailed;
    }
}
