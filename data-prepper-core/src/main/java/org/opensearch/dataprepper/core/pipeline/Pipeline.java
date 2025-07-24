/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.opensearch.dataprepper.DataPrepperShutdownOptions;
import org.opensearch.dataprepper.core.acknowledgements.InactiveAcknowledgementSetManager;
import org.opensearch.dataprepper.core.parser.DataFlowComponent;
import org.opensearch.dataprepper.core.pipeline.common.PipelineThreadFactory;
import org.opensearch.dataprepper.core.pipeline.common.PipelineThreadPoolExecutor;
import org.opensearch.dataprepper.core.pipeline.router.Router;
import org.opensearch.dataprepper.core.pipeline.router.RouterCopyRecordStrategy;
import org.opensearch.dataprepper.core.pipeline.router.RouterGetRecordStrategy;
import org.opensearch.dataprepper.core.sourcecoordination.SourceCoordinatorFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.UsesEnhancedSourceCoordination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data using
 * {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private static final int SINK_LOGGING_FREQUENCY = (int) Duration.ofSeconds(60).toMillis();
    private final PipelineShutdown pipelineShutdown;

    private final String name;
    private final Source source;
    private final Buffer buffer;
    private final List<List<Processor>> processorSets;
    private final List<DataFlowComponent<Sink>> sinks;
    private final Router router;

    private final SourceCoordinatorFactory sourceCoordinatorFactory;
    private final int processorThreads;
    private final int readBatchTimeoutInMillis;
    private final Duration processorShutdownTimeout;
    private final Duration sinkShutdownTimeout;
    private final Duration peerForwarderDrainTimeout;
    private final ExecutorService processorExecutorService;
    private final ExecutorService sinkExecutorService;
    private final EventFactory eventFactory;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final List<PipelineObserver> observers = Collections.synchronizedList(new LinkedList<>());

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
     * @param router                   router object for routing in the pipeline
     * @param eventFactory             event factory to create events
     * @param acknowledgementSetManager   acknowledgement set manager
     * @param sourceCoordinatorFactory source coordinator factory that enables coordination between different instances/threads of sources
     * @param processorThreads         configured or default threads to parallelize processor work
     * @param readBatchTimeoutInMillis configured or default timeout for reading batch of records from buffer
     * @param processorShutdownTimeout configured or default timeout before forcefully terminating the processor workers
     * @param peerForwarderDrainTimeout configured or default timeout before considering the peer forwarder drained and ready for termination
     * @param sinkShutdownTimeout      configured or default timeout before forcefully terminating the sink workers
     */
    public Pipeline(
            @Nonnull final String name,
            @Nonnull final Source source,
            @Nonnull final Buffer buffer,
            @Nonnull final List<List<Processor>> processorSets,
            @Nonnull final List<DataFlowComponent<Sink>> sinks,
            @Nonnull final Router router,
            @Nonnull final EventFactory eventFactory,
            @Nonnull final AcknowledgementSetManager acknowledgementSetManager,
            final SourceCoordinatorFactory sourceCoordinatorFactory,
            final int processorThreads,
            final int readBatchTimeoutInMillis,
            final Duration processorShutdownTimeout,
            final Duration sinkShutdownTimeout,
            final Duration peerForwarderDrainTimeout) {
        Preconditions.checkArgument(processorSets.stream().allMatch(
                processorSet -> Objects.nonNull(processorSet) && (processorSet.size() == 1 || processorSet.size() == processorThreads)));
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        this.processorSets = processorSets;
        this.sinks = sinks;
        this.router = router;
        this.sourceCoordinatorFactory = sourceCoordinatorFactory;
        this.processorThreads = processorThreads;
        this.eventFactory = eventFactory;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.readBatchTimeoutInMillis = readBatchTimeoutInMillis;
        this.processorShutdownTimeout = processorShutdownTimeout;
        this.sinkShutdownTimeout = sinkShutdownTimeout;
        this.peerForwarderDrainTimeout = peerForwarderDrainTimeout;
        this.processorExecutorService = PipelineThreadPoolExecutor.newFixedThreadPool(processorThreads,
                new PipelineThreadFactory(format("%s-processor-worker", name)), this);

        // TODO: allow this to be configurable as well?
        this.sinkExecutorService = PipelineThreadPoolExecutor.newFixedThreadPool(processorThreads,
                new PipelineThreadFactory(format("%s-sink-worker", name)), this);

        this.pipelineShutdown = new PipelineShutdown(name, buffer);
    }

    AcknowledgementSetManager getAcknowledgementSetManager() {
        return acknowledgementSetManager;
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
    // TODO: We can probably remove this
    public Collection<Sink> getSinks() {
        return this.sinks.stream()
                .map(DataFlowComponent::getComponent)
                .collect(Collectors.toList());
    }

    public boolean isStopRequested() {
        return pipelineShutdown.isStopRequested();
    }

    public boolean isForceStopReadingBuffers() {
        return pipelineShutdown.isForceStopReadingBuffers();
    }

    public Duration getPeerForwarderDrainTimeout() {
        return peerForwarderDrainTimeout;
    }

    /**
     * @return a list of {@link Processor} of this pipeline or an empty list .
     */
    List<List<Processor>> getProcessorSets() {
        return processorSets;
    }

    /**
     * @return a flat list of {@link Processor} of this pipeline or an empty list.
     */
    @VisibleForTesting
    public List<Processor> getProcessors() {
        return getProcessorSets().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public int getReadBatchTimeoutInMillis() {
        return readBatchTimeoutInMillis;
    }

    public boolean isReady() {
        for (final Sink sink: getSinks()) {
            if (!sink.isReady()) {
                LOG.info("Pipeline [{}] - sink is not ready for execution, retrying", name);
                sink.initialize();
                if (!sink.isReady()) {
                    return false;
                }
            }
        }
        return true;
    }

    // This method needs to be synchronzied with shutdown
    private synchronized void startSourceAndProcessors() {
        if (isStopRequested()) {
            return;
        }
        LOG.info("Pipeline [{}] Sink is ready, starting source...", name);
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
            processorExecutorService.submit(new ProcessWorker(buffer, processors, this));
        }
    }

    /**
     * Executes the current pipeline i.e. reads the data from {@link Source}, executes optional {@link Processor} on the
     * read data and outputs to {@link Sink}.
     */
    public void execute() {
        LOG.info("Pipeline [{}] - Initiating pipeline execution", name);
        try {
            if (source instanceof UsesSourceCoordination) {
                final Class<?> partionProgressModelClass = ((UsesSourceCoordination) source).getPartitionProgressStateClass();
                final SourceCoordinator sourceCoordinator = sourceCoordinatorFactory.provideSourceCoordinator(partionProgressModelClass, name);
                ((UsesSourceCoordination) source).setSourceCoordinator(sourceCoordinator);
            } else if (source instanceof UsesEnhancedSourceCoordination) {
                final Function<SourcePartitionStoreItem, EnhancedSourcePartition> partitionFactory = ((UsesEnhancedSourceCoordination) source).getPartitionFactory();
                final EnhancedSourceCoordinator enhancedSourceCoordinator = sourceCoordinatorFactory.provideEnhancedSourceCoordinator(partitionFactory, name);
                ((UsesEnhancedSourceCoordination) source).setEnhancedSourceCoordinator(enhancedSourceCoordinator);
            }

            sinkExecutorService.submit(() -> {
                long retryCount = 0;
                final long sleepIfNotReadyTime = 200;
                while (!isReady() && !isStopRequested()) {
                    if (retryCount++ % (SINK_LOGGING_FREQUENCY / sleepIfNotReadyTime) == 0) {
                        LOG.info("Pipeline [{}] Waiting for Sink to be ready", name);
                    }
                    try {
                        Thread.sleep(sleepIfNotReadyTime);
                    } catch (Exception e){}
                }
                startSourceAndProcessors();
            }, null);
        } catch (Exception ex) {
            //source failed to start - Cannot proceed further with the current pipeline, skipping further execution
            LOG.error("Pipeline [{}] encountered exception while starting the source, skipping execution", name, ex);
        }
    }

    public synchronized void shutdown() {
        shutdown(DataPrepperShutdownOptions.defaultOptions());
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
     * @param dataPrepperShutdownOptions options for shutdown behavior
     * @see DataPrepperShutdownOptions
     */
    public synchronized void shutdown(final DataPrepperShutdownOptions dataPrepperShutdownOptions) {
        LOG.info("Pipeline [{}] - Received shutdown signal with buffer drain timeout {}, processor shutdown timeout {}, " +
                        "and sink shutdown timeout {}. Initiating the shutdown process",
                name, buffer.getDrainTimeout(), processorShutdownTimeout, sinkShutdownTimeout);
        try {
            source.stop();
        } catch (Exception ex) {
            LOG.error("Pipeline [{}] - Encountered exception while stopping the source, " +
                    "proceeding with termination of process workers", name, ex);
        }

        pipelineShutdown.shutdown(dataPrepperShutdownOptions);

        shutdownExecutorService(processorExecutorService, pipelineShutdown.getBufferDrainTimeout().plus(processorShutdownTimeout), "processor");

        processorSets.forEach(processorSet -> processorSet.forEach(Processor::shutdown));
        buffer.shutdown();

        sinks.stream()
                .map(DataFlowComponent::getComponent)
                .forEach(Sink::shutdown);

        shutdownExecutorService(sinkExecutorService, sinkShutdownTimeout, "sink");

        LOG.info("Pipeline [{}] - Pipeline fully shutdown.", name);

        observers.forEach(observer -> observer.shutdown(this));
    }

    public void addShutdownObserver(final PipelineObserver pipelineObserver) {
        observers.add(pipelineObserver);
    }

    public void removeShutdownObserver(final PipelineObserver pipelineObserver) {
        observers.remove(pipelineObserver);
    }

    private void shutdownExecutorService(final ExecutorService executorService, final Duration timeoutForTermination, final String workerName) {
        LOG.info("Pipeline [{}] - Shutting down {} process workers.", name, workerName);

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(timeoutForTermination.toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warn("Pipeline [{}] - Workers did not terminate in {}, forcing termination of {} workers.", name, timeoutForTermination, workerName);
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            LOG.info("Pipeline [{}] - Encountered interruption terminating the pipeline execution, " +
                    "Attempting to force the termination of {} workers.", name, workerName);
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
        final List<Future<Void>> sinkFutures = new ArrayList<>(sinksSize);

        final RouterGetRecordStrategy getRecordStrategy =
                new RouterCopyRecordStrategy(eventFactory,
                (source.areAcknowledgementsEnabled() || buffer.areAcknowledgementsEnabled()) ?
                    acknowledgementSetManager :
                    InactiveAcknowledgementSetManager.getInstance(),
                sinks);
        router.route(records, sinks, getRecordStrategy, (sink, events) ->
                sinkFutures.add(sinkExecutorService.submit(() -> {
                    sink.updateLatencyMetrics(events);
                    sink.output(events);
                }, null))
            );
        return sinkFutures;
    }

    public boolean areAcknowledgementsEnabled() {
        return source.areAcknowledgementsEnabled() || buffer.areAcknowledgementsEnabled();
    }
}
