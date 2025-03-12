/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.processor.Processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProcessWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessWorker.class);

    private final Buffer readBuffer;
    private final List<Processor> processors;
    private final Pipeline pipeline;

    private final PipelineRunner pipelineRunner;

    public ProcessWorker(
            final Buffer readBuffer,
            final List<Processor> processors,
            final Pipeline pipeline) {
        this.readBuffer = readBuffer;
        this.processors = processors;
        this.pipeline = pipeline;
        this.pipelineRunner = new PipelineRunnerImpl(pipeline);
    }

    public ProcessWorker(PipelineRunner pipelineRunner) {
        this.pipelineRunner = pipelineRunner;
        this.readBuffer = pipelineRunner.getPipeline().getBuffer();
        this.processors = pipelineRunner.getPipeline().getProcessors();
        this.pipeline = pipelineRunner.getPipeline();
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

    private void doRun() {
        pipelineRunner.runAllProcessorsAndPublishToSinks();
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
}
