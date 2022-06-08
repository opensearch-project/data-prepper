/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.acm;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import software.amazon.awssdk.services.acm.AcmClient;
import software.amazon.awssdk.services.acm.model.GetCertificateRequest;
import software.amazon.awssdk.services.acm.model.GetCertificateResponse;
import software.amazon.awssdk.services.acm.model.InvalidArnException;
import software.amazon.awssdk.services.acm.model.RequestInProgressException;
import software.amazon.awssdk.services.acm.model.ResourceNotFoundException;

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
    private AcmClient  acmClient;

    @Mock
    private GetCertificateResponse getCertificateResponse;

    private ACMCertificateProvider acmCertificateProvider;

    @Before
    public void beforeEach() {
        acmCertificateProvider = new ACMCertificateProvider(acmClient, acmCertificateArn, acmCertIssueTimeOutMillis);
    }

    @Test
    public void getACMCertificateSuccess() {
        final String certificateContent = UUID.randomUUID().toString();
        when(getCertificateResponse.certificate()).thenReturn(certificateContent);
        when(acmClient.getCertificate(any(GetCertificateRequest.class))).thenReturn(getCertificateResponse);
        final Certificate certificate = acmCertificateProvider.getCertificate();
        assertThat(certificate.getCertificate(), is(certificateContent));
    }

    @Test
    public void getACMCertificateRequestInProgressException() {
        when(acmClient.getCertificate(any(GetCertificateRequest.class))).thenThrow(RequestInProgressException.builder().message("Request in progress.").build());
        assertThrows(IllegalStateException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateResourceNotFoundException() {
        when(acmClient.getCertificate(any(GetCertificateRequest.class))).thenThrow(ResourceNotFoundException.builder().message("Resource not found.").build());
        assertThrows(ResourceNotFoundException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateInvalidArnException() {
        when(acmClient.getCertificate(any(GetCertificateRequest.class))).thenThrow(InvalidArnException.builder().message("Invalid certificate arn.").build());
        assertThrows(InvalidArnException.class, () -> acmCertificateProvider.getCertificate());
    }
}
