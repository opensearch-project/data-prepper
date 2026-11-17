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
     * Gets credentials for the given options. When the options do not set an auth mode, the
     * extension default is used, and Application Default Credentials after that.
     *
     * @param options the plugin's credentials options
     * @return the credentials
     */
    GoogleCredentials getCredentials(GcpCredentialsOptions options);

    /**
     * @return the default project from the extension configuration or Application Default Credentials, if any
     */
    Optional<String> getDefaultProjectId();
}
