/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.armeria.authentication;

import io.netty.handler.ssl.ClientAuth;

import javax.net.ssl.TrustManagerFactory;

/**
 * Configuration for client certificate authentication (mTLS).
 * This interface restricts what authentication plugins can configure
 * on the TLS context, preventing plugins from modifying server keys,
 * protocols, or ciphers.
 */
public interface ClientAuthConfiguration {

    /**
     * @return the client authentication mode (REQUIRE, OPTIONAL, or NONE)
     */
    ClientAuth getClientAuth();

    /**
     * @return the TrustManagerFactory used to verify client certificates
     */
    TrustManagerFactory getTrustManagerFactory();
}
