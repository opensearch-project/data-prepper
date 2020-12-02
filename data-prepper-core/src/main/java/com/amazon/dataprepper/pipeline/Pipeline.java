package com.amazon.dataprepper.pipeline;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;
import com.amazon.dataprepper.pipeline.common.PipelineThreadFactory;
import com.amazon.dataprepper.pipeline.common.PipelineThreadPoolExecutor;
import com.amazon.dataprepper.plugins.buffer.BlockingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data using
 * {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private static final List<Processor> EMPTY_PROCESSOR_LIST = new ArrayList<>(0);
    private static final int PROCESSOR_DEFAULT_TERMINATION_IN_MILLISECONDS = 5000;
    private boolean stopRequested;

    private final String name;
    private final Source source;
    private final Buffer buffer;
    private final List<Processor> processors;
    private final List<Sink> sinks;
    private final int processorThreads;
    private final int readBatchTimeoutInMillis;
    private final ExecutorService processorSinkExecutorService;

    /**
     * Constructs a {@link Pipeline} object with provided {@link #name}, {@link Source}, {@link Collection} of
     * {@link Sink}, processorThreads, readBatchTimeout and default {@link Buffer}, {@link Processor}.
     *
     * @param name                     name of the pipeline
     * @param source                   source from where the pipeline reads the records
     * @param sinks                    collection of sink's to which the transformed records need to be posted
     * @param processorThreads         configured or default threads to parallelize processor work
     * @param readBatchTimeoutInMillis configured or default timeout for reading batch of records from buffer
     */
    public Pipeline(
            @Nonnull final String name,
            @Nonnull final Source source,
            @Nonnull final List<Sink> sinks,
            final int processorThreads,
            final int readBatchTimeoutInMillis) {
        this(name, source, new BlockingBuffer(name), EMPTY_PROCESSOR_LIST, sinks, processorThreads, readBatchTimeoutInMillis);
    }

    /**
     * Constructs a {@link Pipeline} object with provided {@link Source}, {@link #name}, {@link Collection} of
     * {@link Sink}, {@link Buffer} and list of {@link Processor}. On {@link #execute()} the engine will read records
     * {@link Record} from provided {@link Source}, buffers the records in {@link Buffer}, applies List of
     * {@link Processor} sequentially (in the given order) and outputs the processed records to collection of
     * {@link Sink}
     *
     * @param name                     name of the pipeline
     * @param source                   source from where the pipeline reads the records
     * @param buffer                   buffer for the source to queue records
     * @param processors               processor that is applied to records
     * @param sinks                    sink to which the transformed records are posted
     * @param processorThreads         configured or default threads to parallelize processor work
     * @param readBatchTimeoutInMillis configured or default timeout for reading batch of records from buffer
     */
    public Pipeline(
            @Nonnull final String name,
            @Nonnull final Source source,
            @Nonnull final Buffer buffer,
            @Nonnull final List<Processor> processors,
            @Nonnull final List<Sink> sinks,
            final int processorThreads,
            final int readBatchTimeoutInMillis) {
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        this.processors = processors;
        this.sinks = sinks;
        this.processorThreads = processorThreads;
        this.readBatchTimeoutInMillis = readBatchTimeoutInMillis;
        int coreThreads = sinks.size() + processorThreads; //TODO We may have to update this after benchmark tests
        this.processorSinkExecutorService = PipelineThreadPoolExecutor.newFixedThreadPool(coreThreads,
                new PipelineThreadFactory(format("%s-process-worker", name)), this);
        stopRequested = false;
    }


    /**
     * @return Unique name of this pipeline.
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return {@link Source} of this pipeline.
     */
    public Source getSource() {
        return this.source;
    }

    /**
     * @return {@link Buffer} of this pipeline.
     */
    public Buffer getBuffer() {
        return this.buffer;
    }

    /**
     * @return {@link Sink} of this pipeline.
     */
    public Collection<Sink> getSinks() {
        return this.sinks;
    }

    public boolean isStopRequested() {
        return stopRequested;
    }

    /**
     * @return a list of {@link Processor} of this pipeline or an empty list .
     */
    List<Processor> getProcessors() {
        return processors;
    }

    public int getReadBatchTimeoutInMillis() {
        return readBatchTimeoutInMillis;
    }

    /**
     * Executes the current pipeline i.e. reads the data from {@link Source}, executes optional {@link Processor} on the
     * read data and outputs to {@link Sink}.
     */
    public void execute() {
        LOG.info("Pipeline [{}] - Initiating pipeline execution", name);
        try {
            source.start(buffer);
            LOG.info("Pipeline [{}] - Submitting request to initiate the pipeline processing", name);
            for (int i = 0; i < processorThreads; i++) {
                processorSinkExecutorService.submit(new ProcessWorker(buffer, processors, sinks, this));
            }
        } catch (Exception ex) {
            //source failed to start - Cannot proceed further with the current pipeline, skipping further execution
            LOG.error("Pipeline [{}] encountered exception while starting the source, skipping execution", name, ex);
        }
    }

    /**
     * Initiates shutdown of the pipeline.
     */
    public void shutdown() {
        shutdown(PROCESSOR_DEFAULT_TERMINATION_IN_MILLISECONDS);
    }

    /**
     * Initiates shutdown of the pipeline by notifying the components to stop processing.
     *
     * @param processorTimeout the maximum time to wait after initiating shutdown to forcefully shutdown process worker
     */
    public void shutdown(int processorTimeout) {
        LOG.info("Pipeline [{}] - Received shutdown signal with timeout {}, will initiate the shutdown process",
                name, processorTimeout);
        try {
            source.stop();
            stopRequested = true;
        } catch (Exception ex) {
            LOG.error("Pipeline [{}] - Encountered exception while stopping the source, " +
                    "proceeding with termination of process workers", name);
        }
        shutdownExecutorService(processorSinkExecutorService, processorTimeout);
    }

    private void shutdownExecutorService(final ExecutorService executorService, int timeoutForTerminationInMillis) {
        LOG.info("Pipeline [{}] - Shutting down process workers", name);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(timeoutForTerminationInMillis, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            LOG.info("Pipeline [{}] - Encountered interruption terminating the pipeline execution, " +
                    "Attempting to force the termination", name);
            executorService.shutdownNow();
        }
    }

    /**
     * Submits the provided collection of records to output to each sink. Collects the future from each sink and returns
     * them as list of futures
     *
     * @param records records that needs to published to each sink
     * @return List of Future, each future for each sink
     */
    public List<Future<Void>> publishToSinks(final Collection<Record> records) {
        final int sinksSize = sinks.size();
        List<Future<Void>> sinkFutures = new ArrayList<>(sinksSize);
        for (int i = 0; i < sinksSize; i++) {
            int finalI = i;
            sinkFutures.add(processorSinkExecutorService.submit(() -> sinks.get(finalI).output(records), null));
        }
        return sinkFutures;
    }
}
