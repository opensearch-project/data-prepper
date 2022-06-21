/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.ownership;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;
import com.amazon.dataprepper.plugins.source.SqsQueueUrl;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigBucketOwnerProviderFactoryTest {

    @Mock
    private S3SourceConfig s3SourceConfig;

    private ConfigBucketOwnerProviderFactory createObjectUnderTest() {
        return new ConfigBucketOwnerProviderFactory();
    }

    @Test
    void createBucketOwnerProvider_returns_NoOwnershipBucketOwnerProvider_when_disabled() {
        when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(true);

        final BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

        assertThat(bucketOwnerProvider, instanceOf(NoOwnershipBucketOwnerProvider.class));
    }

    @Test
    void createBucketOwnerProvider_returns_ownership_based_on_SQS_queueUrl() {
        final SqsOptions sqsOptions = mock(SqsOptions.class);
        final String accountId = UUID.randomUUID().toString();
        final String sqsUrl = UUID.randomUUID().toString();
        when(sqsOptions.getSqsUrl()).thenReturn(sqsUrl);
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);

        when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(false);

        final SqsQueueUrl sqsQueueUrl = mock(SqsQueueUrl.class);
        when(sqsQueueUrl.getAccountId()).thenReturn(accountId);

        final BucketOwnerProvider bucketOwnerProvider;
        try (final MockedStatic<SqsQueueUrl> sqsQueueUrlMockedStatic = mockStatic(SqsQueueUrl.class)) {
            sqsQueueUrlMockedStatic.when(() -> SqsQueueUrl.parse(sqsUrl))
                    .thenReturn(sqsQueueUrl);
            bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);
        }

        assertThat(bucketOwnerProvider, notNullValue());

        final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(UUID.randomUUID().toString());

        assertThat(optionalOwner, notNullValue());
        assertThat(optionalOwner.isPresent(), equalTo(true));
        assertThat(optionalOwner.get(), equalTo(accountId));
    }
}