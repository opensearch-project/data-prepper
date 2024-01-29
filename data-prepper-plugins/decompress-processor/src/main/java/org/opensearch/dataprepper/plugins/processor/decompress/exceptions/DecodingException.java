/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress.exceptions;

public class DecodingException extends RuntimeException {
    public DecodingException(final String message) {
        super(message);
    }
}
