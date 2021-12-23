/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.acm;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import com.amazonaws.services.certificatemanager.AWSCertificateManager;
import com.amazonaws.services.certificatemanager.model.GetCertificateRequest;
import com.amazonaws.services.certificatemanager.model.GetCertificateResult;
import com.amazonaws.services.certificatemanager.model.InvalidArnException;
import com.amazonaws.services.certificatemanager.model.RequestInProgressException;
import com.amazonaws.services.certificatemanager.model.ResourceNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ACMCertificateProviderTest {
    private static final String acmCertificateArn = "arn:aws:acm:us-east-1:account:certificate/1234-567-856456";
    private static final long acmCertIssueTimeOutMillis = 2000L;
    @Mock
    private AWSCertificateManager  awsCertificateManager;

    @Mock
    private GetCertificateResult getCertificateResult;

    private ACMCertificateProvider acmCertificateProvider;

    @Before
    public void beforeEach() {
        acmCertificateProvider = new ACMCertificateProvider(awsCertificateManager, acmCertificateArn, acmCertIssueTimeOutMillis);
    }

    @Test
    public void getACMCertificateSuccess() {
        final String certificateContent = UUID.randomUUID().toString();
        when(getCertificateResult.getCertificate()).thenReturn(certificateContent);
        when(awsCertificateManager.getCertificate(any(GetCertificateRequest.class))).thenReturn(getCertificateResult);
        final Certificate certificate = acmCertificateProvider.getCertificate();
        assertThat(certificate.getCertificate(), is(certificateContent));
    }

    @Test
    public void getACMCertificateRequestInProgressException() {
        when(awsCertificateManager.getCertificate(any(GetCertificateRequest.class))).thenThrow(new RequestInProgressException("Request in progress."));
        assertThrows(IllegalStateException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateResourceNotFoundException() {
        when(awsCertificateManager.getCertificate(any(GetCertificateRequest.class))).thenThrow(new ResourceNotFoundException("Resource not found."));
        assertThrows(ResourceNotFoundException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateInvalidArnException() {
        when(awsCertificateManager.getCertificate(any(GetCertificateRequest.class))).thenThrow(new InvalidArnException("Invalid certificate arn."));
        assertThrows(InvalidArnException.class, () -> acmCertificateProvider.getCertificate());
    }
}
