/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FieldContainsS3EventFilterTest {
    @Mock
    private Function<S3EventNotification.S3EventNotificationRecord, String> fieldProvider;
    private Collection<String> validValues;

    private FieldContainsS3EventFilter<String> createObjectUnderTest() {
        return new FieldContainsS3EventFilter<>(fieldProvider, validValues);
    }

    @Test
    void constructor_throws_if_fieldProvider_is_null() {
        fieldProvider = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_validValues_is_null() {
        validValues = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_validValues_is_empty() {
        validValues = Collections.emptyList();
        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Nested
    class WithNotification {

        @Mock
        private S3EventNotification.S3EventNotificationRecord notification;
        private String actualValue;

        @BeforeEach
        void setUp() {
            validValues = Collections.emptyList();

            actualValue = UUID.randomUUID().toString();
            when(fieldProvider.apply(notification)).thenReturn(actualValue);
        }

        @Test
        void filter_returns_empty_if_the_field_is_not_present() {
            validValues = Collections.singletonList(UUID.randomUUID().toString());

            final Optional<S3EventNotification.S3EventNotificationRecord> result = createObjectUnderTest().filter(notification);

            assertThat(result, notNullValue());
            assertThat(result.isPresent(), equalTo(false));
        }

        @Test
        void filter_returns_notification_if_the_field_is_present() {
            validValues = Collections.singletonList(actualValue);

            final Optional<S3EventNotification.S3EventNotificationRecord> result = createObjectUnderTest().filter(notification);

            assertThat(result, notNullValue());
            assertThat(result.isPresent(), equalTo(true));
            assertThat(result.get(), equalTo(notification));
        }

        @Test
        void filter_returns_empty_if_the_field_is_not_present_when_multiple_validValues() {
            validValues = Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());

            final Optional<S3EventNotification.S3EventNotificationRecord> result = createObjectUnderTest().filter(notification);

            assertThat(result, notNullValue());
            assertThat(result.isPresent(), equalTo(false));
        }

        @Test
        void filter_returns_notification_if_the_field_is_present_when_multiple_validValues() {
            validValues = Arrays.asList(UUID.randomUUID().toString(), actualValue, UUID.randomUUID().toString());

            final Optional<S3EventNotification.S3EventNotificationRecord> result = createObjectUnderTest().filter(notification);

            assertThat(result, notNullValue());
            assertThat(result.isPresent(), equalTo(true));
            assertThat(result.get(), equalTo(notification));
        }
    }
}