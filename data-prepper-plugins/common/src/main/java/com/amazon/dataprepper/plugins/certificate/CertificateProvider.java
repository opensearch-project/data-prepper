package com.amazon.dataprepper.plugins.certificate;

import com.amazon.dataprepper.plugins.certificate.model.Certificate;

public interface CertificateProvider {
    Certificate getCertificate();
}
