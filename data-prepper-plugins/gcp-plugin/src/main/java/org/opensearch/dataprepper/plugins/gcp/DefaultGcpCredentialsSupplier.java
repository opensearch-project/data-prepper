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
import org.opensearch.dataprepper.gcp.api.GcpAuthMode;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsOptions;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * Resolves which credentials a plugin gets: its own options, then the extension default, then Application Default Credentials.
 * The plugin's scopes and project are kept whichever credentials identity is used.
 */
class DefaultGcpCredentialsSupplier implements GcpCredentialsSupplier {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultGcpCredentialsSupplier.class);
    private static final GcpCredentialsOptions APPLICATION_DEFAULT_IDENTITY =
            GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).build();

    private final GcpCredentialsOptions defaultOptions;
    private final GoogleCredentialsFactory googleCredentialsFactory;
    private final CredentialsCache credentialsCache;
    private final ApplicationDefaultCredentialsLoader applicationDefaultCredentialsLoader;
    private Optional<String> applicationDefaultProjectId;

    DefaultGcpCredentialsSupplier(final GcpCredentialsOptions defaultOptions,
                                  final GoogleCredentialsFactory googleCredentialsFactory,
                                  final CredentialsCache credentialsCache,
                                  final ApplicationDefaultCredentialsLoader applicationDefaultCredentialsLoader) {
        this.defaultOptions = defaultOptions;
        this.googleCredentialsFactory = googleCredentialsFactory;
        this.credentialsCache = credentialsCache;
        this.applicationDefaultCredentialsLoader = applicationDefaultCredentialsLoader;
    }

    @Override
    public GoogleCredentials getCredentials(final GcpCredentialsOptions options) {
        final GcpCredentialsOptions resolvedOptions = resolve(options);
        return credentialsCache.getOrCreate(resolvedOptions, () -> googleCredentialsFactory.create(resolvedOptions));
    }

    @Override
    public Optional<String> getDefaultProjectId() {
        if (defaultOptions != null && defaultOptions.getProjectId() != null) {
            return Optional.of(defaultOptions.getProjectId());
        }
        if (defaultOptions != null && defaultOptions.getAuthMode() == GcpAuthMode.SERVICE_ACCOUNT_KEY) {
            return Optional.empty();
        }
        return getApplicationDefaultProjectId();
    }

    private synchronized Optional<String> getApplicationDefaultProjectId() {
        if (applicationDefaultProjectId == null) {
            applicationDefaultProjectId = loadApplicationDefaultProjectId();
        }
        return applicationDefaultProjectId;
    }

    private Optional<String> loadApplicationDefaultProjectId() {
        try {
            return Optional.ofNullable(applicationDefaultCredentialsLoader.load().getProjectId());
        } catch (final IOException e) {
            LOG.debug("Application Default Credentials are not available to determine a default GCP project.", e);
            return Optional.empty();
        }
    }

    private GcpCredentialsOptions resolve(final GcpCredentialsOptions options) {
        if (options != null && options.getAuthMode() != null) {
            return options;
        }
        final GcpCredentialsOptions identity = defaultOptions != null && defaultOptions.getAuthMode() != null
                ? defaultOptions : APPLICATION_DEFAULT_IDENTITY;
        return options != null ? options.withIdentityFrom(identity) : identity;
    }
}
