package com.amazon.dataprepper.server;

import com.amazon.dataprepper.pipeline.server.SslUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;

import static com.amazon.dataprepper.pipeline.server.SslUtil.KEY_ALIAS;
import static org.junit.Assert.assertTrue;

public class SslUtilTest {
    private static final String RSA_KEY = "src/test/resources/tls/test_rsa.pkcs8";
    private static final String DSA_KEY = "src/test/resources/tls/test_dsa.pkcs8";
    private static final String CERT = "src/test/resources/tls/test_cert.cer";

    private static final String EMPTY_KEY = "src/test/resources/tls/test_empty_key.pkcs8";
    private static final String EMPTY_CERT = "src/test/resources/tls/test_empty_cert.cer";

    private SslUtil sut;

    @Before
    public void setup() {
        sut = new SslUtil();
    }

    @Test(expected = IOException.class)
    public void testPrivateKeyFileNotFound() throws Exception {
        sut.loadKeyStore(new File(""), new File(""));
    }

    @Test
    public void testLoadRsaKeySuccess() throws Exception {
        KeyStore result = sut.loadKeyStore(new File(CERT), new File(RSA_KEY));
        assertTrue(result.isKeyEntry(KEY_ALIAS));
    }

    @Test
    public void testLoadDsaKeySuccess() throws Exception {
        KeyStore result = sut.loadKeyStore(new File(CERT), new File(DSA_KEY));
        assertTrue(result.isKeyEntry(KEY_ALIAS));
    }

    @Test(expected = KeyStoreException.class)
    public void testLoadEmptyKey() throws Exception {
        KeyStore result = sut.loadKeyStore(new File(CERT), new File(EMPTY_KEY));
        assertTrue(result.isKeyEntry(KEY_ALIAS));
    }

    @Test(expected = CertificateException.class)
    public void testLoadEmptyCert() throws Exception {
        KeyStore result = sut.loadKeyStore(new File(EMPTY_CERT), new File(RSA_KEY));
        assertTrue(result.isKeyEntry(KEY_ALIAS));
    }
}
