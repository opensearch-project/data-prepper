package org.opensearch.dataprepper.core.pipeline;

public interface SupportsPipelineRunner {
    PipelineRunner getPipelineRunner();

    void setPipelineRunner(PipelineRunner pipelineRunner);
}
