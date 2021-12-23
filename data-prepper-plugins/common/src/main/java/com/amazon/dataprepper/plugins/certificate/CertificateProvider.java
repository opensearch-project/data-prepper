/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.certificate;

import com.amazon.dataprepper.plugins.certificate.model.Certificate;

public interface CertificateProvider {
    Certificate getCertificate();
}
