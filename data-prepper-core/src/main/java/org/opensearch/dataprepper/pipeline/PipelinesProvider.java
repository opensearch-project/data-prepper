/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import java.util.Map;

/**
 * Interface for a provider of available Data Prepper Pipelines.
 */
public interface PipelinesProvider {
    Map<String, Pipeline> getTransformationPipelines();
}
