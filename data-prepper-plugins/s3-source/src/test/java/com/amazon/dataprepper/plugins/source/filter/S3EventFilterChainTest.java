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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3EventFilterChainTest {

    @Mock
    private S3EventNotification.S3EventNotificationRecord notification;
    private List<S3EventFilter> innerFilters;

    @BeforeEach
    void setUp() {
        innerFilters = Collections.emptyList();
    }

    private S3EventFilterChain createObjectUnderTest() {
        return new S3EventFilterChain(innerFilters);
    }

    @Test
    void constructor_throws_if_filters_is_null() {
        innerFilters = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void filter_with_empty_filters_returns_empty_if_notification_is_null() {
        final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = createObjectUnderTest().filter(null);

        assertThat(actualValue, notNullValue());
        assertThat(actualValue.isPresent(), equalTo(false));
    }

    @Test
    void filter_with_empty_filters_returns_input_notification() {
        final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = createObjectUnderTest().filter(notification);

        assertThat(actualValue, notNullValue());
        assertThat(actualValue.isPresent(), equalTo(true));
        assertThat(actualValue.get(), equalTo(notification));
    }

    @Nested
    class WithOneFilter {

        private S3EventFilter innerFilter;

        @BeforeEach
        void setUp() {
            innerFilter = mock(S3EventFilter.class);
            innerFilters = Collections.singletonList(innerFilter);
        }


        @Test
        void filter_returns_empty_if_notification_is_null() {
            final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = createObjectUnderTest().filter(null);

            assertThat(actualValue, notNullValue());
            assertThat(actualValue.isPresent(), equalTo(false));
        }


        @Test
        void filter_returns_the_notification_when_inner_filter_returns_it() {
            when(innerFilter.filter(notification)).thenReturn(Optional.of(notification));

            final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = createObjectUnderTest().filter(notification);

            assertThat(actualValue, notNullValue());
            assertThat(actualValue.isPresent(), equalTo(true));
            assertThat(actualValue.get(), equalTo(notification));
        }

        @Test
        void filter_returns_empty_when_inner_filter_returns_empty() {
            when(innerFilter.filter(notification)).thenReturn(Optional.empty());

            final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = createObjectUnderTest().filter(notification);

            assertThat(actualValue, notNullValue());
            assertThat(actualValue.isPresent(), equalTo(false));
        }
    }

    @Nested
    class WithMultipleFilters {

        @BeforeEach
        void setUp() {
            innerFilters = IntStream.range(0, 5)
                    .mapToObj(i -> mock(S3EventFilter.class))
                    .collect(Collectors.toList());
        }

        @Test
        void filter_with_multiple_filters_returns_empty_when_first_filter_is_empty() {
            final S3EventFilter firstFilter = innerFilters.get(0);
            when(firstFilter.filter(notification)).thenReturn(Optional.empty());


            final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = createObjectUnderTest().filter(notification);

            assertThat(actualValue, notNullValue());
            assertThat(actualValue.isPresent(), equalTo(false));

            for (int i = 1; i < innerFilters.size(); i++) {
                final S3EventFilter otherFilters = innerFilters.get(i);
                verifyNoInteractions(otherFilters);
            }
        }

        @Test
        void filter_with_multiple_filters_returns_empty_when_last_filter_is_empty() {
            final S3EventFilter firstFilter = innerFilters.get(innerFilters.size() - 1);
            when(firstFilter.filter(notification)).thenReturn(Optional.empty());

            for (int i = 0; i < innerFilters.size() - 1; i++) {
                final S3EventFilter otherFilters = innerFilters.get(i);
                when(otherFilters.filter(notification))
                        .thenReturn(Optional.of(notification));
            }

            final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = createObjectUnderTest().filter(notification);

            assertThat(actualValue, notNullValue());
            assertThat(actualValue.isPresent(), equalTo(false));
        }

        @Test
        void filter_with_multiple_filters_returns_the_final_notification() {
            S3EventNotification.S3EventNotificationRecord previousNotification = notification;
            for (S3EventFilter innerFilter : innerFilters) {
                final S3EventNotification.S3EventNotificationRecord nextNotification = mock(S3EventNotification.S3EventNotificationRecord.class);
                when(innerFilter.filter(previousNotification))
                        .thenReturn(Optional.of(nextNotification));
                previousNotification = nextNotification;
            }

            final Optional<S3EventNotification.S3EventNotificationRecord> actualValue = createObjectUnderTest().filter(notification);

            assertThat(actualValue, notNullValue());
            assertThat(actualValue.isPresent(), equalTo(true));
            assertThat(actualValue.get(), equalTo(previousNotification));
        }
    }
}