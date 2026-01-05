/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor.s3source.ownership;

import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.s3.common.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.ownership.MappedBucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.ownership.NoOwnershipBucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.ownership.StaticBucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.source.StsArnRole;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.S3EnrichProcessorConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Produces a {@link BucketOwnerProvider} from the S3 source configuration as
 * provided in a {@link S3EnrichProcessorConfig}.
 */
public class ConfigBucketOwnerProviderFactory {
    private final AwsCredentialsProvider defaultAwsCredentialsProvider;


    public ConfigBucketOwnerProviderFactory(final AwsCredentialsProvider defaultAwsCredentialsProvider) {
        this.defaultAwsCredentialsProvider = defaultAwsCredentialsProvider;
    }

    /**
     * Creates the {@link BucketOwnerProvider}
     * @param s3EnrichProcessorConfig The input {@link S3EnrichProcessorConfig}
     * @return The {@link BucketOwnerProvider}
     */
    public BucketOwnerProvider createBucketOwnerProvider(final S3EnrichProcessorConfig s3EnrichProcessorConfig) {
        if(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation())
            return new NoOwnershipBucketOwnerProvider();
        final StaticBucketOwnerProvider staticBucketOwnerProvider = getStaticBucketOwnerProvider(s3EnrichProcessorConfig);

        if(s3EnrichProcessorConfig.getBucketOwners() != null && !s3EnrichProcessorConfig.getBucketOwners().isEmpty()) {
            return new MappedBucketOwnerProvider(s3EnrichProcessorConfig.getBucketOwners(), staticBucketOwnerProvider);
        } else {
            return staticBucketOwnerProvider;
        }
    }

    private StaticBucketOwnerProvider getStaticBucketOwnerProvider(final S3EnrichProcessorConfig s3EnrichProcessorConfig) {
        final String accountId;

        if(s3EnrichProcessorConfig.getDefaultBucketOwner() != null)
            accountId = s3EnrichProcessorConfig.getDefaultBucketOwner();
        else if(s3EnrichProcessorConfig.getAwsAuthenticationOptions() != null && s3EnrichProcessorConfig.getAwsAuthenticationOptions().getAwsStsRoleArn() != null)
            accountId = extractStsRoleArnAccountId(s3EnrichProcessorConfig);
        else {
            accountId = defaultAwsCredentialsProvider.resolveCredentials().accountId()
                    .orElseThrow(() -> new InvalidPluginConfigurationException(
                            "The S3 Enricher is unable to determine a bucket owner. Configure the default_bucket_owner for the account Id that owns the bucket. You may also want to configure bucket_owners if you read from S3 buckets in different accounts."
                    ));
        }

        return new StaticBucketOwnerProvider(accountId);
    }

    private String extractStsRoleArnAccountId(final S3EnrichProcessorConfig s3EnrichProcessorConfig) {
        return StsArnRole.parse(s3EnrichProcessorConfig.getAwsAuthenticationOptions().getAwsStsRoleArn())
                .getAccountId();
    }
}
