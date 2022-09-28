/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.exception;

/**
 * This exception is thrown when the fingerprint associated with a certificate cannot be parsed
 *
 * @since 2.0
 */
public class CertificateFingerprintParsingException extends RuntimeException {

    public CertificateFingerprintParsingException(final String errorMessage, final Throwable cause) {
        super(errorMessage, cause);
    }
}
