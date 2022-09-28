/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.dataprepper.plugins.source.S3EventNotification;

class ObjectCreatedFilterTest {

    private ObjectCreatedFilter objectCreatedFilter;
    private S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord;


    @BeforeEach
    void setUp() {
        objectCreatedFilter = new ObjectCreatedFilter();
        s3EventNotificationRecord = mock(S3EventNotification.S3EventNotificationRecord.class);
    }

    @Test
    void filter_with_eventName_ObjectCreated_should_return_non_empty_instance_of_optional() {
        when(s3EventNotificationRecord.getEventName()).thenReturn("ObjectCreated:Put");
        Optional<S3EventNotification.S3EventNotificationRecord> actualValue = objectCreatedFilter.filter(s3EventNotificationRecord);

        assertThat(actualValue, instanceOf(Optional.class));
        assertTrue(actualValue.isPresent());
        assertThat(actualValue, equalTo(Optional.of(s3EventNotificationRecord)));
    }

    @Test
    void filter_with_eventName_ObjectRemoved_should_return_empty_instance_of_optional() {
        when(s3EventNotificationRecord.getEventName()).thenReturn("ObjectRemoved:Delete");
        Optional<S3EventNotification.S3EventNotificationRecord> actualValue = objectCreatedFilter.filter(s3EventNotificationRecord);

        assertThat(actualValue, instanceOf(Optional.class));
        assertFalse(actualValue.isPresent());
        assertThat(actualValue, equalTo(Optional.empty()));
    }

}