/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

class CredentialsCache {
    private final Map<CredentialsIdentifier, AwsCredentialsProvider> credentialsProviderMap;

    CredentialsCache() {
        credentialsProviderMap = new ConcurrentHashMap<>();
    }

    AwsCredentialsProvider getOrCreate(final AwsCredentialsOptions awsCredentialsOptions, final Supplier<AwsCredentialsProvider> providerSupplier) {
        final CredentialsIdentifier identifier = CredentialsIdentifier.fromAwsCredentialsOption(awsCredentialsOptions);

        return credentialsProviderMap.computeIfAbsent(identifier, i -> providerSupplier.get());
    }
}
