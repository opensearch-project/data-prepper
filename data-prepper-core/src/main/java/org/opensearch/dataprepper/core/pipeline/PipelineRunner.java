/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

/**
 * Pipeline Runner interface encapsulates the functionalities of reading from buffer,
 * executing the processors and publishing to sinks to provide both synchronous and
 * asynchronous mechanism for running a pipeline.
 */
public interface PipelineRunner {
    void runAllProcessorsAndPublishToSinks();

    Pipeline getPipeline();
}
