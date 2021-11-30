/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.pipeline;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;
import com.amazon.dataprepper.pipeline.common.PipelineThreadFactory;
import com.amazon.dataprepper.pipeline.common.PipelineThreadPoolExecutor;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data using
 * {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private static final int PROCESSOR_DEFAULT_TERMINATION_IN_MILLISECONDS = 10_000;
    private volatile boolean stopRequested;

    private final String name;
    private final Source source;
    private final Buffer buffer;
    private final List<List<Processor>> processorSets;
    private final List<Sink> sinks;
    private final int processorThreads;
    private final int readBatchTimeoutInMillis;
    private final ExecutorService processorExecutorService;
    private final ExecutorService sinkExecutorService;

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
     * @param processorSets               processor sets that will be applied to records. Each set includes either a single shared processor instance
     *                                  or multiple instances with each to be accessed only by a single {@link ProcessWorker}.
     * @param sinks                    sink to which the transformed records are posted
     * @param processorThreads         configured or default threads to parallelize processor work
     * @param readBatchTimeoutInMillis configured or default timeout for reading batch of records from buffer
     */
    public Pipeline(
            @Nonnull final String name,
            @Nonnull final Source source,
            @Nonnull final Buffer buffer,
            @Nonnull final List<List<Processor>> processorSets,
            @Nonnull final List<Sink> sinks,
            final int processorThreads,
            final int readBatchTimeoutInMillis) {
        Preconditions.checkArgument(processorSets.stream().allMatch(
                processorSet -> Objects.nonNull(processorSet) && (processorSet.size() == 1 || processorSet.size() == processorThreads)));
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        this.processorSets = processorSets;
        this.sinks = sinks;
        this.processorThreads = processorThreads;
        this.readBatchTimeoutInMillis = readBatchTimeoutInMillis;
        this.processorExecutorService = PipelineThreadPoolExecutor.newFixedThreadPool(processorThreads,
                new PipelineThreadFactory(format("%s-processor-worker", name)), this);

        // TODO: allow this to be configurable as well?
        this.sinkExecutorService = PipelineThreadPoolExecutor.newFixedThreadPool(processorThreads,
                new PipelineThreadFactory(format("%s-sink-worker", name)), this);

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
    List<List<Processor>> getProcessorSets() {
        return processorSets;
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
                final int finalI = i;
                final List<Processor> processors = processorSets.stream().map(
                        processorSet -> {
                            if (processorSet.size() == 1) {
                                return processorSet.get(0);
                            } else {
                                return processorSet.get(finalI);
                            }
                        }
                ).collect(Collectors.toList());
                processorExecutorService.submit(new ProcessWorker(buffer, processors, sinks, this));
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
     * Initiates shutdown of the pipeline by:
     * 1. Stopping the source to prevent new items from being consumed
     * 2. Notifying processors to prepare for shutdown (e.g. flushing batched items)
     * 3. Waiting for ProcessWorkers to exit their run loop (only after buffer/processors are empty)
     * 4. Stopping the ProcessWorkers if they are unable to exit gracefully
     * 5. Shutting down processors and sinks
     * 6. Stopping the sink ExecutorService
     *
     * @param processorTimeout the maximum time to wait after initiating shutdown to forcefully shutdown process worker
     */
    public void shutdown(int processorTimeout) {
        LOG.info("Pipeline [{}] - Received shutdown signal with timeout {}, will initiate the shutdown process",
                name, processorTimeout);
        try {
            source.stop();
            stopRequested = true;
            processorSets.forEach(processorSet -> processorSet.forEach(Processor::prepareForShutdown));
        } catch (Exception ex) {
            LOG.error("Pipeline [{}] - Encountered exception while stopping the source, " +
                    "proceeding with termination of process workers", name);
        }

        shutdownExecutorService(processorExecutorService, processorTimeout);

        processorSets.forEach(processorSet -> processorSet.forEach(Processor::shutdown));
        sinks.forEach(Sink::shutdown);

        shutdownExecutorService(sinkExecutorService, processorTimeout);
    }

    private void shutdownExecutorService(final ExecutorService executorService, int timeoutForTerminationInMillis) {
        LOG.info("Pipeline [{}] - Shutting down process workers", name);

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(timeoutForTerminationInMillis, TimeUnit.MILLISECONDS)) {
                LOG.warn("Pipeline [{}] - Workers did not terminate in time, forcing termination", name);
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
            sinkFutures.add(sinkExecutorService.submit(() -> sinks.get(finalI).output(records), null));
        }
        return sinkFutures;
    }
}
