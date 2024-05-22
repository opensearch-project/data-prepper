/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;


class DefaultAwsCredentialsSupplier implements AwsCredentialsSupplier {
    private final CredentialsProviderFactory credentialsProviderFactory;
    private final CredentialsCache credentialsCache;

    DefaultAwsCredentialsSupplier(final CredentialsProviderFactory credentialsProviderFactory, final CredentialsCache credentialsCache) {
        this.credentialsProviderFactory = credentialsProviderFactory;
        this.credentialsCache = credentialsCache;
    }

    @Override
    public AwsCredentialsProvider getProvider(final AwsCredentialsOptions options) {
        return credentialsCache.getOrCreate(options, () -> credentialsProviderFactory.providerFromOptions(options));
    }
}
