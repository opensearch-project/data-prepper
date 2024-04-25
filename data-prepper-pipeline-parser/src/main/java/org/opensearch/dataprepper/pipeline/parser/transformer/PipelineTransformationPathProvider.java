/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

public interface PipelineTransformationPathProvider {

    String getTransformationTemplateDirectoryLocation();

    String getTransformationRulesDirectoryLocation();

}
