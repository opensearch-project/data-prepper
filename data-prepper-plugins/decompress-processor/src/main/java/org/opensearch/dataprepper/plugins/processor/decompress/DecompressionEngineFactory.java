/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import org.opensearch.dataprepper.model.codec.DecompressionEngine;

public interface DecompressionEngineFactory {
    public DecompressionEngine getDecompressionEngine();
}
