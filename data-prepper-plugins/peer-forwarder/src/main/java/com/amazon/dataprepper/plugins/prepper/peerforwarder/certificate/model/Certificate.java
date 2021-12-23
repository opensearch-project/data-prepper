/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model;

import static java.util.Objects.requireNonNull;

public class Certificate {
    /**
     * The base64 PEM-encoded certificate.
     */
    private String certificate;

    public Certificate(final String certificate) {
        this.certificate = requireNonNull(certificate, "certificate must not be null");
    }

    public String getCertificate() {
        return certificate;
    }
}
