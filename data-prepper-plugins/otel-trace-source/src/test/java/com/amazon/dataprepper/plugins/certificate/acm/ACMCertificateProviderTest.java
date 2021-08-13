package com.amazon.dataprepper.plugins.certificate.acm;

import com.amazon.dataprepper.plugins.certificate.model.Certificate;
import com.amazonaws.services.certificatemanager.AWSCertificateManager;
import com.amazonaws.services.certificatemanager.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ACMCertificateProviderTest {
    private static final String acmCertificateArn = "arn:aws:acm:us-east-1:account:certificate/1234-567-856456";
    private static final long acmCertIssueTimeOutMillis = 2000L;
    private static final String acmPrivateKeyPassword = "password";
    @Mock
    private AWSCertificateManager  awsCertificateManager;

    @Mock
    private ExportCertificateResult exportCertificateResult;

    private ACMCertificateProvider acmCertificateProvider;

    @BeforeEach
    public void beforeEach() {
        acmCertificateProvider = new ACMCertificateProvider(awsCertificateManager, acmCertificateArn, acmCertIssueTimeOutMillis, acmPrivateKeyPassword);
    }

    @Test
    public void getACMCertificateWithEncryptedPrivateKeySuccess() throws IOException {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final Path encryptedKeyFilePath = Path.of("data/certificate/test_encrypted_key.key");
        final Path decryptedKeyFilePath = Path.of("data/certificate/test_decrypted_key.key");
        final String certAsString = Files.readString(certFilePath);
        final String encryptedKeyAsString = Files.readString(encryptedKeyFilePath);
        final String decryptedKeyAsString = Files.readString(decryptedKeyFilePath);
        when(exportCertificateResult.getCertificate()).thenReturn(certAsString);
        when(exportCertificateResult.getPrivateKey()).thenReturn(encryptedKeyAsString);
        when(awsCertificateManager.exportCertificate(any(ExportCertificateRequest.class))).thenReturn(exportCertificateResult);
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
        when(exportCertificateResult.getCertificate()).thenReturn(certAsString);
        when(exportCertificateResult.getPrivateKey()).thenReturn(decryptedKeyAsString);
        when(awsCertificateManager.exportCertificate(any(ExportCertificateRequest.class))).thenReturn(exportCertificateResult);
        final Certificate certificate = acmCertificateProvider.getCertificate();
        assertThat(certificate.getCertificate(), is(certAsString));
        assertThat(certificate.getPrivateKey(), is(decryptedKeyAsString));
    }

    @Test
    public void getACMCertificateWithInvalidPrivateKeyException() {
        when(exportCertificateResult.getPrivateKey()).thenReturn(UUID.randomUUID().toString());
        when(awsCertificateManager.exportCertificate(any(ExportCertificateRequest.class))).thenReturn(exportCertificateResult);
        assertThrows(RuntimeException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateRequestInProgressException() {
        when(awsCertificateManager.exportCertificate(any(ExportCertificateRequest.class))).thenThrow(new RequestInProgressException("Request in progress."));
        assertThrows(IllegalStateException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateResourceNotFoundException() {
        when(awsCertificateManager.exportCertificate(any(ExportCertificateRequest.class))).thenThrow(new ResourceNotFoundException("Resource not found."));
        assertThrows(ResourceNotFoundException.class, () -> acmCertificateProvider.getCertificate());
    }

    @Test
    public void getACMCertificateInvalidArnException() {
        when(awsCertificateManager.exportCertificate(any(ExportCertificateRequest.class))).thenThrow(new InvalidArnException("Invalid certificate arn."));
        assertThrows(InvalidArnException.class, () -> acmCertificateProvider.getCertificate());
    }
}
