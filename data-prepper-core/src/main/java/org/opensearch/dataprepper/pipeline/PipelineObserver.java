/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

/**
 * Allows an observer to observe major changes in pipelines.
 */
public interface PipelineObserver {
    void shutdown(Pipeline pipeline);
}
