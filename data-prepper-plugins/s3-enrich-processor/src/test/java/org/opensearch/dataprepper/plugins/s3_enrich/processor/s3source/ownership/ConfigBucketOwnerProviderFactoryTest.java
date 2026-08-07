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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.s3.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.s3.common.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.ownership.MappedBucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.ownership.NoOwnershipBucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.ownership.StaticBucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.S3EnrichProcessorConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigBucketOwnerProviderFactoryTest {

    @Mock
    private S3EnrichProcessorConfig s3EnrichProcessorConfig;

    @Mock
    private AwsCredentialsProvider defaultAwsCredentialsProvider;

    @Mock
    private AwsCredentials awsCredentials;

    private String accountId;

    @BeforeEach
    void setUp() {
        accountId = String.valueOf(100000000000L + new Random().nextInt(900000000));
    }

    private ConfigBucketOwnerProviderFactory createObjectUnderTest() {
        return new ConfigBucketOwnerProviderFactory(defaultAwsCredentialsProvider);
    }

    @Test
    void createBucketOwnerProvider_returns_NoOwnershipBucketOwnerProvider_when_validation_disabled() {
        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(true);

        final BucketOwnerProvider result = createObjectUnderTest().createBucketOwnerProvider(s3EnrichProcessorConfig);

        assertThat(result, instanceOf(NoOwnershipBucketOwnerProvider.class));
    }

    @Test
    void createBucketOwnerProvider_returns_StaticBucketOwnerProvider_for_defaultBucketOwner() {
        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3EnrichProcessorConfig.getDefaultBucketOwner()).thenReturn(accountId);

        final BucketOwnerProvider result = createObjectUnderTest().createBucketOwnerProvider(s3EnrichProcessorConfig);

        assertThat(result, instanceOf(StaticBucketOwnerProvider.class));
    }

    @Test
    void createBucketOwnerProvider_returns_correct_owner_for_defaultBucketOwner() {
        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3EnrichProcessorConfig.getDefaultBucketOwner()).thenReturn(accountId);

        final BucketOwnerProvider result = createObjectUnderTest().createBucketOwnerProvider(s3EnrichProcessorConfig);

        final Optional<String> owner = result.getBucketOwner(UUID.randomUUID().toString());
        assertThat(owner.isPresent(), equalTo(true));
        assertThat(owner.get(), equalTo(accountId));
    }

    @Test
    void createBucketOwnerProvider_returns_MappedBucketOwnerProvider_when_bucketOwners_are_defined() {
        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3EnrichProcessorConfig.getDefaultBucketOwner()).thenReturn(accountId);
        when(s3EnrichProcessorConfig.getBucketOwners()).thenReturn(
                Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        final BucketOwnerProvider result = createObjectUnderTest().createBucketOwnerProvider(s3EnrichProcessorConfig);

        assertThat(result, instanceOf(MappedBucketOwnerProvider.class));
    }

    @Test
    void createBucketOwnerProvider_returns_correct_mapped_owner_for_specific_bucket() {
        final String specificBucket = UUID.randomUUID().toString();
        final String specificOwner = String.valueOf(100000000000L + new Random().nextInt(900000000));
        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3EnrichProcessorConfig.getDefaultBucketOwner()).thenReturn(accountId);
        when(s3EnrichProcessorConfig.getBucketOwners()).thenReturn(Map.of(specificBucket, specificOwner));

        final BucketOwnerProvider result = createObjectUnderTest().createBucketOwnerProvider(s3EnrichProcessorConfig);

        final Optional<String> owner = result.getBucketOwner(specificBucket);
        assertThat(owner.isPresent(), equalTo(true));
        assertThat(owner.get(), equalTo(specificOwner));
    }

    @Test
    void createBucketOwnerProvider_falls_back_to_default_when_bucket_not_in_map() {
        final String unmappedBucket = UUID.randomUUID().toString();
        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3EnrichProcessorConfig.getDefaultBucketOwner()).thenReturn(accountId);
        when(s3EnrichProcessorConfig.getBucketOwners()).thenReturn(
                Map.of(UUID.randomUUID().toString(), String.valueOf(100000000000L + new Random().nextInt(900000000))));

        final BucketOwnerProvider result = createObjectUnderTest().createBucketOwnerProvider(s3EnrichProcessorConfig);

        final Optional<String> owner = result.getBucketOwner(unmappedBucket);
        assertThat(owner.isPresent(), equalTo(true));
        assertThat(owner.get(), equalTo(accountId));
    }

    @Test
    void createBucketOwnerProvider_extracts_account_from_sts_role_arn_when_no_defaultBucketOwner() {
        final String stsRoleArn = String.format("arn:aws:iam::%s:role/SomeRole", accountId);
        final AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);

        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3EnrichProcessorConfig.getDefaultBucketOwner()).thenReturn(null);
        when(s3EnrichProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);

        final BucketOwnerProvider result = createObjectUnderTest().createBucketOwnerProvider(s3EnrichProcessorConfig);

        assertThat(result, notNullValue());
        final Optional<String> owner = result.getBucketOwner(UUID.randomUUID().toString());
        assertThat(owner.isPresent(), equalTo(true));
        assertThat(owner.get(), equalTo(accountId));
    }

    @Test
    void createBucketOwnerProvider_uses_default_credentials_when_no_defaultBucketOwner_and_no_stsRoleArn() {
        when(defaultAwsCredentialsProvider.resolveCredentials()).thenReturn(awsCredentials);
        when(awsCredentials.accountId()).thenReturn(Optional.of(accountId));

        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3EnrichProcessorConfig.getDefaultBucketOwner()).thenReturn(null);
        when(s3EnrichProcessorConfig.getAwsAuthenticationOptions()).thenReturn(null);

        final BucketOwnerProvider result = createObjectUnderTest().createBucketOwnerProvider(s3EnrichProcessorConfig);

        assertThat(result, notNullValue());
        final Optional<String> owner = result.getBucketOwner(UUID.randomUUID().toString());
        assertThat(owner.isPresent(), equalTo(true));
        assertThat(owner.get(), equalTo(accountId));
    }

    @Test
    void createBucketOwnerProvider_throws_InvalidPluginConfigurationException_when_ownership_cannot_be_determined() {
        when(defaultAwsCredentialsProvider.resolveCredentials()).thenReturn(awsCredentials);
        when(awsCredentials.accountId()).thenReturn(Optional.empty());

        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(false);
        when(s3EnrichProcessorConfig.getDefaultBucketOwner()).thenReturn(null);
        when(s3EnrichProcessorConfig.getAwsAuthenticationOptions()).thenReturn(null);

        final ConfigBucketOwnerProviderFactory objectUnderTest = createObjectUnderTest();
        final InvalidPluginConfigurationException thrown = assertThrows(
                InvalidPluginConfigurationException.class,
                () -> objectUnderTest.createBucketOwnerProvider(s3EnrichProcessorConfig));

        assertThat(thrown.getMessage(), containsString("default_bucket_owner"));
    }
}
