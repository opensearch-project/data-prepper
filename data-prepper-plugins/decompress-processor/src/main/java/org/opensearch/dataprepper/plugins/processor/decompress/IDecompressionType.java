/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import org.opensearch.dataprepper.model.codec.DecompressionEngine;

public interface IDecompressionType {
    public DecompressionEngine getDecompressionEngine();
}
