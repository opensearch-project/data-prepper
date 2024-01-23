/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.acm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import software.amazon.awssdk.services.acm.AcmClient;
import software.amazon.awssdk.services.acm.model.ExportCertificateRequest;
import software.amazon.awssdk.services.acm.model.ExportCertificateResponse;
import software.amazon.awssdk.services.acm.model.InvalidArnException;
import software.amazon.awssdk.services.acm.model.RequestInProgressException;
import software.amazon.awssdk.services.acm.model.ResourceNotFoundException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ACMCertificateProviderTest {
    private static final String acmCertificateArn = "arn:aws:acm:us-east-1:account:certificate/1234-567-856456";
    private static final long acmCertIssueTimeOutMillis = 2000L;
    private static final String acmPrivateKeyPassword = "password";
    @Mock
    private AcmClient acmClient;
    @Mock
    private ExportCertificateResponse exportCertificateResponse;
    private ACMCertificateProvider acmCertificateProvider;

    @BeforeEach
    public void beforeEach() {
        acmCertificateProvider = new ACMCertificateProvider(acmClient, acmCertificateArn, acmCertIssueTimeOutMillis, acmPrivateKeyPassword);
    }

    @Test
    public void getACMCertificateWithEncryptedPrivateKeySuccess() throws IOException {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final Path encryptedKeyFilePath = Path.of("data/certificate/test_encrypted_key.key");
        final Path decryptedKeyFilePath = Path.of("data/certificate/test_decrypted_key.key");
        final String certAsString = Files.readString(certFilePath);
        final String encryptedKeyAsString = Files.readString(encryptedKeyFilePath);
        final String decryptedKeyAsString = Files.readString(decryptedKeyFilePath);
        when(exportCertificateResponse.certificate()).thenReturn(certAsString);
        when(exportCertificateResponse.privateKey()).thenReturn(encryptedKeyAsString);
        when(acmClient.exportCertificate(any(ExportCertificateRequest.class))).thenReturn(exportCertificateResponse);
        final Certificate certificate = acmCertificateProvider.getCertificate();
        assertThat(certificate.getCertificate(), is(certAsString));
        assertThat(certificate.getPrivateKey(), is(decryptedKeyAsString));
    }

    @Test
    public void getACMCertificateWithUnencryptedPrivateKeySuccess() throws IOException {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final Path decryptedKeyFilePath = Path.of("data/certificate/test_decrypted_key.key");
        final String certAsString = Files.readString(certFilePath);
        final String decryptedKeyAsString = Files.readString(decryptedKeyFilePath);
        when(exportCertificateResponse.certificate()).thenReturn(certAsString);
        when(exportCertificateResponse.privateKey()).thenReturn(decryptedKeyAsString);
        when(acmClient.exportCertificate(any(ExportCertificateRequest.class))).thenReturn(exportCertificateResponse);
        final Certificate certificate = acmCertificateProvider.getCertificate();
        assertThat(certificate.getCertificate(), is(certAsString));
        assertThat(certificate.getPrivateKey(), is(decryptedKeyAsString));
    }

    @Test
    public void getACMCertificateWithInvalidPrivateKeyException() {
        when(exportCertificateResponse.privateKey()).thenReturn(UUID.randomUUID().toString());
        when(acmClient.exportCertificate(any(ExportCertificateRequest.class))).thenReturn(exportCertificateResponse);
        assertThrows(RuntimeException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateRequestInProgressException() {
        when(acmClient.exportCertificate(any(ExportCertificateRequest.class))).thenThrow(RequestInProgressException.builder().message("Request in progress.").build());
        assertThrows(IllegalStateException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateResourceNotFoundException() {
        when(acmClient.exportCertificate(any(ExportCertificateRequest.class))).thenThrow(ResourceNotFoundException.builder().message("Resource not found.").build());
        assertThrows(ResourceNotFoundException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateInvalidArnException() {
        when(acmClient.exportCertificate(any(ExportCertificateRequest.class))).thenThrow(InvalidArnException.builder().message("Invalid certificate arn.").build());
        assertThrows(InvalidArnException.class, () -> acmCertificateProvider.getCertificate());
    }
}
