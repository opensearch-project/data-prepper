/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.office365.auth;

/**
 * Interface for Office 365 authentication provider.
 */
public interface Office365AuthenticationInterface {
    /**
     * Gets the tenant ID for the Office 365 application.
     *
     * @return The tenant ID
     */
    String getTenantId();

    /**
     * Gets the current access token.
     *
     * @return The access token
     */
    String getAccessToken();

    /**
     * Initializes the credentials.
     */
    void initCredentials();

    /**
     * Renews the credentials.
     */
    void renewCredentials();
}
