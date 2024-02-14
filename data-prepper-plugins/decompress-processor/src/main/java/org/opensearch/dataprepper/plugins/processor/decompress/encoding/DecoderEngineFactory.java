/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress.encoding;

public interface DecoderEngineFactory {
    DecoderEngine getDecoderEngine();
}
