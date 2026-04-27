/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.pipeline;

import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.util.Map;

/**
 * Interface for a provider of available Data Prepper Pipelines.
 */
public interface PipelinesProvider {
    Map<String, Pipeline> getTransformationPipelines();
    PipelinesDataFlowModel getPipelinesDataFlowModel();
}
