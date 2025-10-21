/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.auth;

import org.opensearch.dataprepper.plugins.source.source_crawler.auth.AuthenticationInterface;

/**
 * Interface for Office 365 authentication provider.
 * Extends the generic SaasAuthenticationProvider with Office 365-specific methods.
 */
public interface Office365AuthenticationInterface extends AuthenticationInterface {
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
}
