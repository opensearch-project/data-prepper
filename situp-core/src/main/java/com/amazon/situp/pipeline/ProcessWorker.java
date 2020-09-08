package com.amazon.situp.pipeline;

import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProcessWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

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
            boolean isHalted = false;
            do {
                isHalted = isHalted || pipeline.isStopRequested();
                Thread.sleep(0);
                Collection records = readBuffer.readBatch();
                if (records != null && !records.isEmpty()) {
                    LOG.debug("Pipeline Worker: Processing {} records from buffer", records.size());
                    for (final Processor processor : processors) {
                        records = processor.execute(records);
                    }
                    postToSink(records);
                } else {
                    isQueueEmpty = true;
                }
            } while (!isHalted || !isBufferEmpty()); //If pipeline is stopped, we try to empty the already buffered records ?
        } catch (final Exception ex) {
            throw new IllegalStateException(ex);
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
     */
    private boolean postToSink(Collection<Record> records) {
        LOG.debug("Pipeline Worker: Submitting {} processed records to sink", records.size());
        sinks.forEach(sink -> sink.output(records));
        return true;
    }
}
