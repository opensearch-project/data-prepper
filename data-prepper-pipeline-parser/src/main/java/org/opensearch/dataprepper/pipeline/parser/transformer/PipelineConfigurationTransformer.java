/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

public interface PipelineConfigurationTransformer {

    PipelinesDataFlowModel transformConfiguration(PipelinesDataFlowModel pipelinesDataFlowModel);

}
