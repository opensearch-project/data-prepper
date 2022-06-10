/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ObjectCreatedFilterTest {

    private S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord;

    @BeforeEach
    void setUp() {
        s3EventNotificationRecord = mock(S3EventNotification.S3EventNotificationRecord.class);
    }

    private Optional<S3EventFilter> createObjectUnderTest() {
        return new ObjectCreatedFilter.Factory().createFilter(null);
    }

    @Test
    void filter_with_eventName_ObjectCreated_should_return_non_empty_instance_of_optional() {
        when(s3EventNotificationRecord.getEventName()).thenReturn("ObjectCreated:Put");
        final Optional<S3EventFilter> optionalFilter = createObjectUnderTest();
        assertThat(optionalFilter, notNullValue());
        assertThat(optionalFilter.isPresent(), equalTo(true));
        final S3EventFilter objectCreatedFilter = optionalFilter.get();
        final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = objectCreatedFilter.filter(s3EventNotificationRecord);

        assertThat(actualValue, instanceOf(Optional.class));
        assertTrue(actualValue.isPresent());
        assertThat(actualValue, equalTo(Optional.of(s3EventNotificationRecord)));
    }

    @Test
    void filter_with_eventName_ObjectRemoved_should_return_empty_instance_of_optional() {
        when(s3EventNotificationRecord.getEventName()).thenReturn("ObjectRemoved:Delete");
        final Optional<S3EventFilter> optionalFilter = createObjectUnderTest();
        assertThat(optionalFilter, notNullValue());
        assertThat(optionalFilter.isPresent(), equalTo(true));
        final S3EventFilter objectCreatedFilter = optionalFilter.get();
        final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = objectCreatedFilter.filter(s3EventNotificationRecord);

        assertThat(actualValue, instanceOf(Optional.class));
        assertFalse(actualValue.isPresent());
        assertThat(actualValue, equalTo(Optional.empty()));
    }

}