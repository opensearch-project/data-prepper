/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.opensearch.dataprepper.gcp.api.GcpAuthMode;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsException;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsOptions;

import java.io.IOException;

/**
 * Builds {@link GoogleCredentials} for fully resolved {@link GcpCredentialsOptions}.
 */
class GoogleCredentialsFactory {
    private final ApplicationDefaultCredentialsLoader applicationDefaultCredentialsLoader;
    private final TokenUriValidator tokenUriValidator;

    GoogleCredentialsFactory(final ApplicationDefaultCredentialsLoader applicationDefaultCredentialsLoader,
                             final TokenUriValidator tokenUriValidator) {
        this.applicationDefaultCredentialsLoader = applicationDefaultCredentialsLoader;
        this.tokenUriValidator = tokenUriValidator;
    }

    GoogleCredentials create(final GcpCredentialsOptions options) {
        final boolean serviceAccountKey = options.getAuthMode() == GcpAuthMode.SERVICE_ACCOUNT_KEY;
        try {
            if (!serviceAccountKey) {
                return applicationDefaultCredentialsLoader.load().createScoped(options.getScopes());
            }
            return ServiceAccountCredentials.newBuilder()
                    .setClientEmail(options.getClientEmail())
                    .setPrivateKeyString(options.getPrivateKey())
                    .setTokenServerUri(tokenUriValidator.validate(options.getTokenUri()))
                    .setScopes(options.getScopes())
                    .setProjectId(options.getProjectId())
                    .build();
        } catch (final IOException e) {
            throw new GcpCredentialsException(String.format("Failed to initialize Google credentials (%s): %s",
                    serviceAccountKey ? "service-account key" : "ADC/Workload Identity", e.getMessage()), e);
        }
    }
}
