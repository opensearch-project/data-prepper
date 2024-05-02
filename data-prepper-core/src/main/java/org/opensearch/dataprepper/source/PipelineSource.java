package org.opensearch.dataprepper.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.pipeline.Pipeline;

public abstract class PipelineSource <T extends Record<?>> implements Source<T> {
    private Pipeline pipeline;

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public abstract void start(Buffer<T> buffer);
}
