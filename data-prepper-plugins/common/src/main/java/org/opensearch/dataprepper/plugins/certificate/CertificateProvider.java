/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate;

import org.opensearch.dataprepper.plugins.certificate.model.Certificate;

public interface CertificateProvider {
    Certificate getCertificate();
}
