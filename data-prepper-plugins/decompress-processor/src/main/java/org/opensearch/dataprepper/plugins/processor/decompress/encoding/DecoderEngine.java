/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.decompress.encoding;

import org.opensearch.dataprepper.plugins.processor.decompress.exceptions.DecodingException;

public interface DecoderEngine {
    byte[] decode(final String encodedValue) throws DecodingException;
}
