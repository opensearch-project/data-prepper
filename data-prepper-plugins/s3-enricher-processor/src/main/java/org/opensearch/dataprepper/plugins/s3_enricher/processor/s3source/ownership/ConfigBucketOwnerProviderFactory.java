/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source.ownership;

import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.S3EnricherProcessorConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Produces a {@link BucketOwnerProvider} from the S3 source configuration as
 * provided in a {@link S3EnricherProcessorConfig}.
 */
public class ConfigBucketOwnerProviderFactory {
    private final AwsCredentialsProvider defaultAwsCredentialsProvider;


    public ConfigBucketOwnerProviderFactory(final AwsCredentialsProvider defaultAwsCredentialsProvider) {
        this.defaultAwsCredentialsProvider = defaultAwsCredentialsProvider;
    }

    /**
     * Creates the {@link BucketOwnerProvider}
     * @param s3EnricherProcessorConfig The input {@link S3EnricherProcessorConfig}
     * @return The {@link BucketOwnerProvider}
     */
    public BucketOwnerProvider createBucketOwnerProvider(final S3EnricherProcessorConfig s3EnricherProcessorConfig) {
        if(s3EnricherProcessorConfig.isDisableBucketOwnershipValidation())
            return new NoOwnershipBucketOwnerProvider();
        final StaticBucketOwnerProvider staticBucketOwnerProvider = getStaticBucketOwnerProvider(s3EnricherProcessorConfig);

        if(s3EnricherProcessorConfig.getBucketOwners() != null && !s3EnricherProcessorConfig.getBucketOwners().isEmpty()) {
            return new MappedBucketOwnerProvider(s3EnricherProcessorConfig.getBucketOwners(), staticBucketOwnerProvider);
        } else {
            return staticBucketOwnerProvider;
        }
    }

    private StaticBucketOwnerProvider getStaticBucketOwnerProvider(final S3EnricherProcessorConfig s3EnricherProcessorConfig) {
        final String accountId;

        if(s3EnricherProcessorConfig.getDefaultBucketOwner() != null)
            accountId = s3EnricherProcessorConfig.getDefaultBucketOwner();
        else if(s3EnricherProcessorConfig.getAwsAuthenticationOptions() != null && s3EnricherProcessorConfig.getAwsAuthenticationOptions().getAwsStsRoleArn() != null)
            accountId = extractStsRoleArnAccountId(s3EnricherProcessorConfig);
        else {
            accountId = defaultAwsCredentialsProvider.resolveCredentials().accountId()
                    .orElseThrow(() -> new InvalidPluginConfigurationException(
                            "The S3 Enricher is unable to determine a bucket owner. Configure the default_bucket_owner for the account Id that owns the bucket. You may also want to configure bucket_owners if you read from S3 buckets in different accounts."
                    ));
        }

        return new StaticBucketOwnerProvider(accountId);
    }

    private String extractStsRoleArnAccountId(final S3EnricherProcessorConfig s3EnricherProcessorConfig) {
        return StsArnRole.parse(s3EnricherProcessorConfig.getAwsAuthenticationOptions().getAwsStsRoleArn())
                .getAccountId();
    }
}
