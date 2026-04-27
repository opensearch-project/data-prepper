/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
