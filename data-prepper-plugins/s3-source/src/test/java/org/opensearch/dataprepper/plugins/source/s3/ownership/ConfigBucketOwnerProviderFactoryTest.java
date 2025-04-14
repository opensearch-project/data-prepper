/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.ownership;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.source.s3.S3SourceConfig;
import org.opensearch.dataprepper.plugins.source.s3.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.SqsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigBucketOwnerProviderFactoryTest {

    @Mock
    private S3SourceConfig s3SourceConfig;
    @Mock
    private AwsCredentialsProvider defaultAwsCredentialsProvider;
    @Mock
    private AwsCredentials awsCredentials;
    private String accountId;

    @BeforeEach
    void setUp() {
        accountId = RandomStringUtils.randomNumeric(12);
    }

    private ConfigBucketOwnerProviderFactory createObjectUnderTest() {
        return new ConfigBucketOwnerProviderFactory(defaultAwsCredentialsProvider);
    }

    @Test
    void createBucketOwnerProvider_returns_NoOwnershipBucketOwnerProvider_when_disabled() {
        when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(true);

        final BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

        assertThat(bucketOwnerProvider, instanceOf(NoOwnershipBucketOwnerProvider.class));
    }

    @Test
    void createBucketOwnerProvider_returns_ownership_using_default_when_no_bucket_mapping() {
        when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3SourceConfig.getDefaultBucketOwner()).thenReturn(accountId);

        BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

        assertThat(bucketOwnerProvider, notNullValue());

        final String bucket = UUID.randomUUID().toString();
        final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(bucket);

        assertThat(optionalOwner, notNullValue());
        assertThat(optionalOwner.isPresent(), equalTo(true));
        assertThat(optionalOwner.get(), equalTo(accountId));
    }

    @Test
    void createBucketOwnerProvider_returns_ownership_using_default_when_bucket_mapping_does_not_match() {
        when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3SourceConfig.getBucketOwners()).thenReturn(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        when(s3SourceConfig.getDefaultBucketOwner()).thenReturn(accountId);

        BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

        assertThat(bucketOwnerProvider, notNullValue());

        final String bucket = UUID.randomUUID().toString();
        final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(bucket);

        assertThat(optionalOwner, notNullValue());
        assertThat(optionalOwner.isPresent(), equalTo(true));
        assertThat(optionalOwner.get(), equalTo(accountId));
    }

    @Test
    void createBucketOwnerProvider_throws_exception_when_ownership_cannot_be_determined() {
        when(defaultAwsCredentialsProvider.resolveCredentials()).thenReturn(awsCredentials);
        when(awsCredentials.accountId()).thenReturn(Optional.empty());
        final ConfigBucketOwnerProviderFactory objectUnderTest = createObjectUnderTest();
        final InvalidPluginConfigurationException actualException = assertThrows(InvalidPluginConfigurationException.class, () -> objectUnderTest.createBucketOwnerProvider(s3SourceConfig));

        assertThat(actualException.getMessage(), containsString("default_bucket_owner"));
    }

    @Test
    void createBucketOwnerProvider_with_ownership_extracted_from_default_aws_credentials_provider() {
        when(defaultAwsCredentialsProvider.resolveCredentials()).thenReturn(awsCredentials);
        when(awsCredentials.accountId()).thenReturn(Optional.of(accountId));

        BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

        assertThat(bucketOwnerProvider, notNullValue());

        final String bucket = UUID.randomUUID().toString();
        final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(bucket);

        assertThat(optionalOwner, notNullValue());
        assertThat(optionalOwner.isPresent(), equalTo(true));
        assertThat(optionalOwner.get(), equalTo(accountId));
    }

    @Nested
    class WithSqsQueueUrl {

        @BeforeEach
        void setUp() {
            final SqsOptions sqsOptions = mock(SqsOptions.class);
            final String sqsUrl = String.format("https://sqs.us-east-1.amazonaws.com/%s/MyQueue", accountId);
            lenient().when(sqsOptions.getSqsUrl()).thenReturn(sqsUrl);
            lenient().when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);
            lenient().when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        }

        @Test
        void createBucketOwnerProvider_returns_MappedBucketOwnerProvider_when_bucketOwners_defined() {
            when(s3SourceConfig.getBucketOwners()).thenReturn(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

            final BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

            assertThat(bucketOwnerProvider, instanceOf(MappedBucketOwnerProvider.class));
        }

        @Test
        void createBucketOwnerProvider_returns_ownership_based_on_bucket_owners_map() {
            final String bucket = UUID.randomUUID().toString();
            when(s3SourceConfig.getBucketOwners()).thenReturn(Map.of(bucket, accountId));

            BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

            assertThat(bucketOwnerProvider, notNullValue());

            final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(bucket);

            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(true));
            assertThat(optionalOwner.get(), equalTo(accountId));
        }

        @Test
        void createBucketOwnerProvider_returns_ownership_using_SQS_queue_URL_when_bucket_not_in_bucket_map() {
            when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
            when(s3SourceConfig.getBucketOwners()).thenReturn(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

            BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

            assertThat(bucketOwnerProvider, notNullValue());

            final String bucket = UUID.randomUUID().toString();
            final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(bucket);

            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(true));
            assertThat(optionalOwner.get(), equalTo(accountId));
        }

        @Test
        void createBucketOwnerProvider_returns_StaticBucketOwnerProvider_when_bucketOwners_not_defined() {
            final BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

            assertThat(bucketOwnerProvider, instanceOf(StaticBucketOwnerProvider.class));
        }


        @Test
        void createBucketOwnerProvider_returns_ownership_based_on_SQS_queueUrl() {
            final BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

            assertThat(bucketOwnerProvider, notNullValue());

            final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(UUID.randomUUID().toString());

            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(true));
            assertThat(optionalOwner.get(), equalTo(accountId));
        }

        @Test
        void createBucketOwnerProvider_returns_ownership_using_default_when_bucket_mapping_does_not_match() {
            accountId = RandomStringUtils.randomNumeric(12);
            when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
            when(s3SourceConfig.getBucketOwners()).thenReturn(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
            when(s3SourceConfig.getDefaultBucketOwner()).thenReturn(accountId);

            BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

            assertThat(bucketOwnerProvider, notNullValue());

            final String bucket = UUID.randomUUID().toString();
            final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(bucket);

            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(true));
            assertThat(optionalOwner.get(), equalTo(accountId));
        }
    }

    @Test
    void createBucketOwnerProvider_returns_ownership_based_on_STS_role_ARN_when_no_SQS_queue() {
        final String stsRoleArn = String.format("arn:aws:iam::%s:role/something", accountId);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(s3SourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);

        when(s3SourceConfig.isDisableBucketOwnershipValidation()).thenReturn(false);

        final BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SourceConfig);

        assertThat(bucketOwnerProvider, notNullValue());

        final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(UUID.randomUUID().toString());

        assertThat(optionalOwner, notNullValue());
        assertThat(optionalOwner.isPresent(), equalTo(true));
        assertThat(optionalOwner.get(), equalTo(accountId));
    }
}