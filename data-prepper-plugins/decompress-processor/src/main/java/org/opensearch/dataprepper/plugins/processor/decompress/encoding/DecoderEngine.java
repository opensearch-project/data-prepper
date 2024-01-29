/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress.encoding;

import org.opensearch.dataprepper.plugins.processor.decompress.exceptions.DecodingException;

public interface DecoderEngine {
    byte[] decode(final String encodedValue) throws DecodingException;
}
