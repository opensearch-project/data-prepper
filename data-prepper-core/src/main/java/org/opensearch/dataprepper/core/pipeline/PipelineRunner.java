package org.opensearch.dataprepper.core.pipeline;

public interface PipelineRunner {
    void runAllProcessorsAndPublishToSinks();
}
