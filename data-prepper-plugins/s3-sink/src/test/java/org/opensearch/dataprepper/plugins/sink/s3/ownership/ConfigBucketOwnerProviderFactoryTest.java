/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.ownership;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigBucketOwnerProviderFactoryTest {

    @Mock
    private S3SinkConfig s3SinkConfig;
    private String accountId;

    @BeforeEach
    void setUp() {
        accountId = RandomStringUtils.randomNumeric(12);
    }

    private ConfigBucketOwnerProviderFactory createObjectUnderTest() {
        return new ConfigBucketOwnerProviderFactory();
    }

    @Test
    void createBucketOwnerProvider_returns_NoOwnershipBucketOwnerProvider_when_bucketOwners_not_provided() {
        when(s3SinkConfig.getDefaultBucketOwner()).thenReturn(null);
        when(s3SinkConfig.getBucketOwners()).thenReturn(null);

        final BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SinkConfig);

        assertThat(bucketOwnerProvider, instanceOf(NoOwnershipBucketOwnerProvider.class));
    }

    @Test
    void createBucketOwnerProvider_returns_ownership_using_default_when_no_bucket_mapping() {
        when(s3SinkConfig.getDefaultBucketOwner()).thenReturn(accountId);

        BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SinkConfig);

        assertThat(bucketOwnerProvider, notNullValue());

        final String bucket = UUID.randomUUID().toString();
        final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(bucket);

        assertThat(optionalOwner, notNullValue());
        assertThat(optionalOwner.isPresent(), equalTo(true));
        assertThat(optionalOwner.get(), equalTo(accountId));
    }

    @Test
    void createBucketOwnerProvider_returns_ownership_using_default_when_bucket_mapping_does_not_match() {
        when(s3SinkConfig.getBucketOwners()).thenReturn(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        when(s3SinkConfig.getDefaultBucketOwner()).thenReturn(accountId);

        BucketOwnerProvider bucketOwnerProvider = createObjectUnderTest().createBucketOwnerProvider(s3SinkConfig);

        assertThat(bucketOwnerProvider, notNullValue());

        final String bucket = UUID.randomUUID().toString();
        final Optional<String> optionalOwner = bucketOwnerProvider.getBucketOwner(bucket);

        assertThat(optionalOwner, notNullValue());
        assertThat(optionalOwner.isPresent(), equalTo(true));
        assertThat(optionalOwner.get(), equalTo(accountId));
    }

    @Test
    void createBucketOwnerProvider_throws_exception_when_ownership_cannot_be_determined() {
        final ConfigBucketOwnerProviderFactory objectUnderTest = createObjectUnderTest();
        final InvalidPluginConfigurationException actualException = assertThrows(InvalidPluginConfigurationException.class, () -> objectUnderTest.createBucketOwnerProvider(s3SinkConfig));

        assertThat(actualException.getMessage(), containsString("default_bucket_owner"));
    }
}