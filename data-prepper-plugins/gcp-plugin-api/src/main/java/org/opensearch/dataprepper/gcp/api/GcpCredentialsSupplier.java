/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

import com.google.auth.oauth2.GoogleCredentials;

import java.util.Optional;

/**
 * Provides Google credentials to Data Prepper plugins.
 */
public interface GcpCredentialsSupplier {
    /**
     * Gets credentials for the given options. When the options do not set an auth mode, the extension default's
     * credentials are used, then Application Default Credentials, keeping the options' scopes and project when set.
     *
     * @param options the plugin's credentials options; null means the extension default, then Application Default Credentials
     * @return the credentials
     * @throws GcpCredentialsException  if the credentials cannot be constructed
     * @throws IllegalArgumentException if a service-account key's token_uri is not the Google OAuth2 token endpoint
     */
    GoogleCredentials getCredentials(GcpCredentialsOptions options);

    /**
     * @return the default project from the extension configuration or Application Default Credentials, if any
     */
    Optional<String> getDefaultProjectId();
}
