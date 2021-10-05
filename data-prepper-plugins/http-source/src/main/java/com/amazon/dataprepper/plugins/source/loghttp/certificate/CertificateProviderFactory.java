/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp.certificate;

import com.amazon.dataprepper.plugins.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.certificate.file.FileCertificateProvider;
import com.amazon.dataprepper.plugins.source.loghttp.HTTPSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CertificateProviderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CertificateProviderFactory.class);

    final HTTPSourceConfig httpSourceConfig;
    public CertificateProviderFactory(final HTTPSourceConfig httpSourceConfig) {
        this.httpSourceConfig = httpSourceConfig;
    }

    public CertificateProvider getCertificateProvider() {
        LOG.info("Using local file system to get certificate and private key for SSL/TLS.");
        return new FileCertificateProvider(httpSourceConfig.getSslCert(), httpSourceConfig.getSslKey());
    }
}
