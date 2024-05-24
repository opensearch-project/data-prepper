/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.api;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.Optional;

/**
 * An interface available to plugins via the AWS Plugin Extension which supplies
 * AWS credentials.
 */
public interface AwsCredentialsSupplier {
    /**
     * Gets an AWS SDK {@link AwsCredentialsProvider} which consumers can use within
     * AWS SDK clients.
     * @param options The {@link AwsCredentialsOptions} defining the credentials.
     * @return An {@link AwsCredentialsProvider} to use.
     */
    AwsCredentialsProvider getProvider(AwsCredentialsOptions options);

    /**
     * Gets the default region if it is configured. Otherwise returns null
     * @return Default {@link Region}
     */
    Optional<Region> getDefaultRegion();
}
