/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BucketFilterFactoryTest {
    @Mock
    private S3SourceConfig s3SourceConfig;

    private BucketFilterFactory createObjectUnderTest() {
        return new BucketFilterFactory();
    }

    @Test
    void factory_with_null_buckets_returns_empty() {
        when(s3SourceConfig.getBuckets()).thenReturn(null);
        final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
        assertThat(optionalFilter, notNullValue());
        assertThat(optionalFilter.isPresent(), equalTo(false));
    }

    @Test
    void factory_without_buckets_returns_empty() {
        when(s3SourceConfig.getBuckets()).thenReturn(Collections.emptyList());
        final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
        assertThat(optionalFilter, notNullValue());
        assertThat(optionalFilter.isPresent(), equalTo(false));
    }

    @Nested
    class WithBucketRestriction {

        @Mock
        private S3EventNotification.S3EventNotificationRecord notification;
        private String updatedBucket;
        private List<String> restrictedBuckets;

        @BeforeEach
        void setUp() {

            updatedBucket = UUID.randomUUID().toString();

            final S3EventNotification.S3BucketEntity s3BucketEntity = mock(S3EventNotification.S3BucketEntity.class);
            when(s3BucketEntity.getName()).thenReturn(updatedBucket);
            final S3EventNotification.S3Entity s3Entity = mock(S3EventNotification.S3Entity.class);
            when(s3Entity.getBucket()).thenReturn(s3BucketEntity);
            when(notification.getS3()).thenReturn(s3Entity);
        }

        @Test
        void filter_returns_empty_if_the_bucket_is_not_present() {
            restrictedBuckets = Collections.singletonList(UUID.randomUUID().toString());
            when(s3SourceConfig.getBuckets()).thenReturn(restrictedBuckets);

            final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
            assertThat(optionalFilter, notNullValue());
            assertThat(optionalFilter.isPresent(), equalTo(true));
            assertThat(optionalFilter.get(), instanceOf(FieldContainsS3EventFilter.class));

            final Optional<S3EventNotification.S3EventNotificationRecord> result = optionalFilter.get().filter(notification);

            assertThat(result, notNullValue());
            assertThat(result.isPresent(), equalTo(false));
        }

        @Test
        void filter_returns_notification_if_the_bucket_is_present() {
            restrictedBuckets = Collections.singletonList(updatedBucket);
            when(s3SourceConfig.getBuckets()).thenReturn(restrictedBuckets);

            final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
            assertThat(optionalFilter, notNullValue());
            assertThat(optionalFilter.isPresent(), equalTo(true));
            assertThat(optionalFilter.get(), instanceOf(FieldContainsS3EventFilter.class));

            final Optional<S3EventNotification.S3EventNotificationRecord> result = optionalFilter.get().filter(notification);

            assertThat(result, notNullValue());
            assertThat(result.isPresent(), equalTo(true));
            assertThat(result.get(), equalTo(notification));
        }

        @Test
        void filter_returns_empty_if_the_bucket_is_not_present_when_multiple_buckets() {
            restrictedBuckets = Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
            when(s3SourceConfig.getBuckets()).thenReturn(restrictedBuckets);

            final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
            assertThat(optionalFilter, notNullValue());
            assertThat(optionalFilter.isPresent(), equalTo(true));
            assertThat(optionalFilter.get(), instanceOf(FieldContainsS3EventFilter.class));

            final Optional<S3EventNotification.S3EventNotificationRecord> result = optionalFilter.get().filter(notification);

            assertThat(result, notNullValue());
            assertThat(result.isPresent(), equalTo(false));
        }

        @Test
        void filter_returns_notification_if_the_bucket_is_present_when_multiple_buckets() {
            restrictedBuckets = Arrays.asList(UUID.randomUUID().toString(), updatedBucket, UUID.randomUUID().toString());
            when(s3SourceConfig.getBuckets()).thenReturn(restrictedBuckets);

            final Optional<S3EventFilter> optionalFilter = createObjectUnderTest().createFilter(s3SourceConfig);
            assertThat(optionalFilter, notNullValue());
            assertThat(optionalFilter.isPresent(), equalTo(true));
            assertThat(optionalFilter.get(), instanceOf(FieldContainsS3EventFilter.class));

            final Optional<S3EventNotification.S3EventNotificationRecord> result = optionalFilter.get().filter(notification);

            assertThat(result, notNullValue());
            assertThat(result.isPresent(), equalTo(true));
            assertThat(result.get(), equalTo(notification));
        }
    }
}