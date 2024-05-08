/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.ownership;

import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;
import software.amazon.awssdk.arns.Arn;

/**
 * Produces a {@link BucketOwnerProvider} from the S3 sink configuration as
 * provided in a {@link S3SinkConfig}.
 */
public class ConfigBucketOwnerProviderFactory {
    /**
     * Creates the {@link BucketOwnerProvider}
     * @param s3SinkConfig The input {@link S3SinkConfig}
     * @return The {@link BucketOwnerProvider}
     */
    public BucketOwnerProvider createBucketOwnerProvider(final S3SinkConfig s3SinkConfig) {
        if (s3SinkConfig.getDefaultBucketOwner() == null && s3SinkConfig.getBucketOwners() == null) {
            return new NoOwnershipBucketOwnerProvider();
        }

        final StaticBucketOwnerProvider staticBucketOwnerProvider = getStaticBucketOwnerProvider(s3SinkConfig);

        if(s3SinkConfig.getBucketOwners() != null && !s3SinkConfig.getBucketOwners().isEmpty()) {
            return new MappedBucketOwnerProvider(s3SinkConfig.getBucketOwners(), staticBucketOwnerProvider);
        } else {
            return staticBucketOwnerProvider;
        }
    }

    private StaticBucketOwnerProvider getStaticBucketOwnerProvider(final S3SinkConfig s3SinkConfig) {
        final String accountId;

        if(s3SinkConfig.getDefaultBucketOwner() != null)
            accountId = s3SinkConfig.getDefaultBucketOwner();
        else if(s3SinkConfig.getAwsAuthenticationOptions() != null && s3SinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn() != null)
            accountId = extractStsRoleArnAccountId(s3SinkConfig);
        else
            throw new InvalidPluginConfigurationException(
                    "The S3 sink is unable to determine a bucket owner. Configure the default_bucket_owner for the account Id that owns the bucket. You may also want to configure bucket_owners if you write to S3 buckets in different accounts.");

        return new StaticBucketOwnerProvider(accountId);
    }

    private String extractStsRoleArnAccountId(final S3SinkConfig s3SinkConfig) {
        final Arn roleArn = Arn.fromString(s3SinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn());

        if (roleArn.accountId().isPresent()) {
            return roleArn.accountId().get();
        }

        throw new RuntimeException(String.format("Unable to extract account id from sts_role_arn %s", s3SinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn()));
    }
}
