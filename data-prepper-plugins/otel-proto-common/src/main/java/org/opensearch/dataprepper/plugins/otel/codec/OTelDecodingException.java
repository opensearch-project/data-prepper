/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

/**
 * An exception representing that opentelemetry protobuf data type was unable to
 * be converted to java Object.
 *
 * @since 1.3
 */
public class OTelDecodingException extends RuntimeException {
    public OTelDecodingException(Throwable cause) {
        super(cause);
    }

    public OTelDecodingException(String message) {
        super(message);
    }
}
