/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;

public interface CertificateProvider {
    Certificate getCertificate();
}
