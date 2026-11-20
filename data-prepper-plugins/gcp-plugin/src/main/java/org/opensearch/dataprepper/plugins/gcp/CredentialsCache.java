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
import org.opensearch.dataprepper.gcp.api.GcpCredentialsOptions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Caches {@link GoogleCredentials} by resolved options so that plugins sharing a configuration share credentials.
 */
class CredentialsCache {
    private final Map<GcpCredentialsOptions, GoogleCredentials> credentialsMap = new ConcurrentHashMap<>();

    GoogleCredentials getOrCreate(final GcpCredentialsOptions options, final Supplier<GoogleCredentials> credentialsSupplier) {
        return credentialsMap.computeIfAbsent(options, ignored -> credentialsSupplier.get());
    }
}
