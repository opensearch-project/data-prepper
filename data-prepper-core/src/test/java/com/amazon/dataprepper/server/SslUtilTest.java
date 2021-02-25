package com.amazon.dataprepper.server;

import com.amazon.dataprepper.pipeline.server.SslUtil;
import org.junit.Test;

import javax.net.ssl.SSLContext;

import static org.junit.Assert.assertEquals;

public class SslUtilTest {

    private static final String P12_KEYSTORE = "src/test/resources/tls/test_keystore.p12";
    private static final String JKS_KEYSTORE = "src/test/resources/tls/test_keystore.jks";

    private static final String KEYSTORE_WITH_PASSWORDS = "src/test/resources/tls/test_keystore_with_passwords.p12";
    private static final String KEYSTORE_PASSWORD = "password";
    private static final String PRIVATE_KEY_PASSWORD = "key";

    @Test
    public void testLoadP12KeyStore() {
        SSLContext result = SslUtil.createSslContext(P12_KEYSTORE, "", "");
        assertEquals("TLS", result.getProtocol());
    }

    @Test
    public void testLoadJksKeyStore() {
        SSLContext result = SslUtil.createSslContext(JKS_KEYSTORE, "", "");
        assertEquals("TLS", result.getProtocol());
    }

    @Test
    public void testLoadP12KeyStoreWithPasswords() {
        SSLContext result = SslUtil.createSslContext(KEYSTORE_WITH_PASSWORDS, KEYSTORE_PASSWORD, PRIVATE_KEY_PASSWORD);
        assertEquals("TLS", result.getProtocol());
    }
}
