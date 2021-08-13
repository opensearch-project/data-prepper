package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.acm;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import com.amazonaws.services.certificatemanager.AWSCertificateManager;
import com.amazonaws.services.certificatemanager.model.GetCertificateRequest;
import com.amazonaws.services.certificatemanager.model.GetCertificateResult;
import com.amazonaws.services.certificatemanager.model.InvalidArnException;
import com.amazonaws.services.certificatemanager.model.RequestInProgressException;
import com.amazonaws.services.certificatemanager.model.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ACMCertificateProvider implements CertificateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ACMCertificateProvider.class);
    private static final long SLEEP_INTERVAL = 10000L;
    private final AWSCertificateManager awsCertificateManager;
    private final String acmArn;
    private final long totalTimeout;
    public ACMCertificateProvider(final AWSCertificateManager awsCertificateManager,
                                  final String acmArn,
                                  final long totalTimeout) {
        this.awsCertificateManager = Objects.requireNonNull(awsCertificateManager);
        this.acmArn = Objects.requireNonNull(acmArn);
        this.totalTimeout = totalTimeout;
    }

    public Certificate getCertificate() {
        GetCertificateResult getCertificateResult = null;
        long timeSlept = 0L;

        while (getCertificateResult == null && timeSlept < totalTimeout) {
            try {
                final GetCertificateRequest getCertificateRequest = new GetCertificateRequest()
                        .withCertificateArn(acmArn);
                getCertificateResult = awsCertificateManager.getCertificate(getCertificateRequest);

            } catch (final RequestInProgressException ex) {
                try {
                    Thread.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException iex) {
                    throw new RuntimeException(iex);
                }
            } catch (final ResourceNotFoundException | InvalidArnException ex) {
                LOG.error("Exception retrieving the certificate with arn: {}", acmArn, ex);
                throw ex;
            }
            timeSlept += SLEEP_INTERVAL;
        }
        if(getCertificateResult != null) {
            return new Certificate(getCertificateResult.getCertificate());
        } else {
            throw new IllegalStateException(String.format("Exception retrieving certificate results. Time spent retrieving certificate is %d ms and total time out set is %d ms.", timeSlept, totalTimeout));
        }
    }
}
