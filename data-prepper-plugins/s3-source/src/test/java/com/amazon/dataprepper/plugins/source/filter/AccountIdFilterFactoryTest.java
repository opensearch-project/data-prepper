/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AccountIdFilterFactoryTest {
    @Mock
    private S3SourceConfig s3SourceConfig;

    @Mock
    private S3EventNotification.S3EventNotificationRecord notification;


    private AccountIdFilterFactory createObjectUnderTest() {
        return new AccountIdFilterFactory();
    }

    @Test
    void factory_produces_no_filter_if_isAllowAnyAccountId() {
        when(s3SourceConfig.isAllowAnyAccountId()).thenReturn(true);

        final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);

        assertThat(optionalFilter, notNullValue());
        assertThat(optionalFilter.isPresent(), equalTo(false));
    }

    @Nested
    class ForDefaultConfiguration {
        private String queueAccountId;
        @Mock
        private S3EventNotification.UserIdentityEntity ownerIdentity;

        @BeforeEach
        void setUp() {

            final SqsOptions sqsOptions = mock(SqsOptions.class);
            when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);

            queueAccountId = UUID.randomUUID().toString();
            final String sqsUrl = String.format("https://sqs.amazonaws.com/%s/%s", queueAccountId, UUID.randomUUID());
            when(sqsOptions.getSqsUrl()).thenReturn(sqsUrl);

            final S3EventNotification.S3BucketEntity s3BucketEntity = mock(S3EventNotification.S3BucketEntity.class);
            final S3EventNotification.S3Entity s3Entity = mock(S3EventNotification.S3Entity.class);
            when(s3BucketEntity.getOwnerIdentity()).thenReturn(ownerIdentity);
            when(s3Entity.getBucket()).thenReturn(s3BucketEntity);
            when(notification.getS3()).thenReturn(s3Entity);
        }


        @Test
        void filter_from_default_configuration_includes_if_owned_by_Sqs_accountId() {
            final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
            assertThat(optionalFilter, notNullValue());
            assertThat(optionalFilter.isPresent(), equalTo(true));

            when(ownerIdentity.getPrincipalId()).thenReturn(queueAccountId);

            final S3EventFilter objectUnderTest = optionalFilter.get();

            final Optional<S3EventNotification.S3EventNotificationRecord> optionalRecord = objectUnderTest.filter(notification);
            assertThat(optionalRecord, notNullValue());
            assertThat(optionalRecord.isPresent(), equalTo(true));
            assertThat(optionalRecord.get(), equalTo(notification));
        }

        @Test
        void filter_from_default_configuration_filters_out_if_not_owned_by_Sqs_accountId() {
            final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
            assertThat(optionalFilter, notNullValue());
            assertThat(optionalFilter.isPresent(), equalTo(true));

            when(ownerIdentity.getPrincipalId()).thenReturn(UUID.randomUUID().toString());

            final S3EventFilter objectUnderTest = optionalFilter.get();

            final Optional<S3EventNotification.S3EventNotificationRecord> optionalRecord = objectUnderTest.filter(notification);
            assertThat(optionalRecord, notNullValue());
            assertThat(optionalRecord.isPresent(), equalTo(false));
        }

    }

    @Nested
    class WithConfiguredAccountIds {

        private String knownAccountId;
        @Mock
        private S3EventNotification.UserIdentityEntity ownerIdentity;


        @BeforeEach
        void setUp() {

            knownAccountId = UUID.randomUUID().toString();

            when(s3SourceConfig.getAccountIds()).thenReturn(Arrays.asList(UUID.randomUUID().toString(), knownAccountId, UUID.randomUUID().toString()));

            final S3EventNotification.S3BucketEntity s3BucketEntity = mock(S3EventNotification.S3BucketEntity.class);
            final S3EventNotification.S3Entity s3Entity = mock(S3EventNotification.S3Entity.class);
            when(s3BucketEntity.getOwnerIdentity()).thenReturn(ownerIdentity);
            when(s3Entity.getBucket()).thenReturn(s3BucketEntity);
            when(notification.getS3()).thenReturn(s3Entity);
        }


        @Test
        void filter_from_default_configuration_includes_if_owned_by_Sqs_accountId() {
            final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
            assertThat(optionalFilter, notNullValue());
            assertThat(optionalFilter.isPresent(), equalTo(true));

            when(ownerIdentity.getPrincipalId()).thenReturn(knownAccountId);

            final S3EventFilter objectUnderTest = optionalFilter.get();

            final Optional<S3EventNotification.S3EventNotificationRecord> optionalRecord = objectUnderTest.filter(notification);
            assertThat(optionalRecord, notNullValue());
            assertThat(optionalRecord.isPresent(), equalTo(true));
            assertThat(optionalRecord.get(), equalTo(notification));
        }

        @Test
        void filter_from_default_configuration_filters_out_if_not_owned_by_Sqs_accountId() {
            final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
            assertThat(optionalFilter, notNullValue());
            assertThat(optionalFilter.isPresent(), equalTo(true));

            when(ownerIdentity.getPrincipalId()).thenReturn(UUID.randomUUID().toString());

            final S3EventFilter objectUnderTest = optionalFilter.get();

            final Optional<S3EventNotification.S3EventNotificationRecord> optionalRecord = objectUnderTest.filter(notification);
            assertThat(optionalRecord, notNullValue());
            assertThat(optionalRecord.isPresent(), equalTo(false));
        }

    }
}