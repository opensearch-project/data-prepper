/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
        // TODO: support more certificate providers
        LOG.info("Using local file system to get certificate and private key for SSL/TLS.");
        return new FileCertificateProvider(httpSourceConfig.getSslCertificateFile(), httpSourceConfig.getSslKeyFile());
    }
}
