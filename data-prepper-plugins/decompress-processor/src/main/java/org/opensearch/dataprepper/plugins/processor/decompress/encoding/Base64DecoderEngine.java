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

import java.util.Base64;

public class Base64DecoderEngine implements DecoderEngine {
    @Override
    public byte[] decode(final String encodedValue) {
        try {
            return Base64.getDecoder().decode(encodedValue);
        } catch (final Exception e) {
            throw new DecodingException(String.format("There was an error decoding with the base64 encoding type: %s", e.getMessage()));
        }
    }
}
