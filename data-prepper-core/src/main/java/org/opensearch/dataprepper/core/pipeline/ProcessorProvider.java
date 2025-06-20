/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import org.opensearch.dataprepper.model.processor.Processor;

import java.util.List;

public interface ProcessorProvider {
    List<Processor> getProcessors();
}
