/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.common;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class CredentialsCache {
    private final Map<CredentialsIdentifier, AwsCredentialsProvider> credentialsProviderMap;

    public CredentialsCache() {
        credentialsProviderMap = new ConcurrentHashMap<>();
    }

    public AwsCredentialsProvider getOrCreate(final AwsCredentialsOptions awsCredentialsOptions, final Supplier<AwsCredentialsProvider> providerSupplier) {
        final CredentialsIdentifier identifier = CredentialsIdentifier.fromAwsCredentialsOption(awsCredentialsOptions);

        return credentialsProviderMap.computeIfAbsent(identifier, i -> providerSupplier.get());
    }
}
