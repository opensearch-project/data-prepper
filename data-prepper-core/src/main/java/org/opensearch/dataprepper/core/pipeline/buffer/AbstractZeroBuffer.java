package org.opensearch.dataprepper.core.pipeline.buffer;

import org.opensearch.dataprepper.core.pipeline.PipelineRunner;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

/**
 * Represents the base class for zero buffer implementation and implements {@link Buffer} interface.
 * It provides a common functionality to run all processors and sinks within the same thread context.
 */
public abstract class AbstractZeroBuffer <T extends Record<?>> implements Buffer<T> {
    private PipelineRunner pipelineRunner;

    protected void runAllProcessorsAndPublishToSinks() {
        // TODO : Implement functionality to call the processors and sinks within the same context
        getPipelineRunner().runAllProcessorsAndPublishToSinks();
    }

    public PipelineRunner getPipelineRunner() {
        return pipelineRunner;
    }

    public void setPipelineRunner(PipelineRunner pipelineRunner) {
        this.pipelineRunner = pipelineRunner;
    }
}
