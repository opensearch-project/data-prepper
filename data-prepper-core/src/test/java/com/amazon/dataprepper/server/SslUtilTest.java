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

package com.amazon.dataprepper.server;

import com.amazon.dataprepper.pipeline.server.SslUtil;
import org.junit.Test;

import javax.net.ssl.SSLContext;

import static org.junit.Assert.assertEquals;

public class SslUtilTest {

    private static final String P12_KEYSTORE = "src/test/resources/tls/test_keystore.p12";
    private static final String JKS_KEYSTORE = "src/test/resources/tls/test_keystore.jks";

    private static final String KEYSTORE_WITH_PASSWORDS = "src/test/resources/tls/test_keystore_with_different_passwords.p12";
    private static final String KEYSTORE_PASSWORD = "password";
    private static final String PRIVATE_KEY_PASSWORD = "key";

    private static final String KEYSTORE_WITH_IDENTICAL_PASSWORDS = "src/test/resources/tls/test_keystore_with_identical_passwords.p12";

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
    public void testLoadP12KeyStoreWithDifferentPasswords() {
        SSLContext result = SslUtil.createSslContext(KEYSTORE_WITH_PASSWORDS, KEYSTORE_PASSWORD, PRIVATE_KEY_PASSWORD);
        assertEquals("TLS", result.getProtocol());
    }

    @Test
    public void testLoadP12KeyStoreWithIdenticalPasswords() {
        SSLContext result = SslUtil.createSslContext(KEYSTORE_WITH_IDENTICAL_PASSWORDS, KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
        assertEquals("TLS", result.getProtocol());
    }
}
