/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.ownership;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;
import com.amazon.dataprepper.plugins.source.SqsQueueUrl;

import java.net.MalformedURLException;

/**
 * Produces a {@link BucketOwnerProvider} from the S3 source configuration as
 * provided in a {@link S3SourceConfig}.
 */
public class ConfigBucketOwnerProviderFactory {
    /**
     * Creates the {@link BucketOwnerProvider}
     * @param s3SourceConfig The input {@link S3SourceConfig}
     * @return The {@link BucketOwnerProvider}
     */
    public BucketOwnerProvider createBucketOwnerProvider(final S3SourceConfig s3SourceConfig) {
        if(s3SourceConfig.isDisableBucketOwnershipValidation())
            return new NoOwnershipBucketOwnerProvider();

        final String accountId = extractQueueAccountId(s3SourceConfig);

        return new StaticBucketOwnerProvider(accountId);
    }

    private String extractQueueAccountId(final S3SourceConfig s3SourceConfig) {
        final String queueUrl = s3SourceConfig.getSqsOptions().getSqsUrl();
        final String accountId;
        try {
            accountId = SqsQueueUrl.parse(queueUrl).getAccountId();
        } catch (final MalformedURLException e) {
            throw new RuntimeException(e);
        }
        return accountId;
    }
}
