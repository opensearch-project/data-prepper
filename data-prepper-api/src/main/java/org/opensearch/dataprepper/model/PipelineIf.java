/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model;

import org.opensearch.dataprepper.model.source.Source;

public interface PipelineIf {
    Source getSource();
}

