/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigFilterConfigFactoryTest {
    private List<FilterConfigFactory> factories;

    @Mock
    private S3SourceConfig s3SourceConfig;

    private ConfigFilterConfigFactory createObjectUnderTest() {
        return new ConfigFilterConfigFactory(factories);
    }

    @Test
    void createFilter_creates_a_filter_which_calls_delegate_filters() {
        final S3EventFilter applicableFilter = mock(S3EventFilter.class);
        final FilterConfigFactory applicableFactory = mock(FilterConfigFactory.class);
        when(applicableFactory.createFilter(s3SourceConfig)).thenReturn(Optional.of(applicableFilter));

        final FilterConfigFactory notApplicableFactory = mock(FilterConfigFactory.class);
        when(notApplicableFactory.createFilter(s3SourceConfig)).thenReturn(Optional.empty());

        factories = Arrays.asList(notApplicableFactory, applicableFactory);

        final S3EventFilter filterCreated = createObjectUnderTest().createFilter(s3SourceConfig);

        assertThat(filterCreated, notNullValue());
        assertThat(filterCreated, instanceOf(S3EventFilterChain.class));

        final S3EventNotification.S3EventNotificationRecord record = mock(S3EventNotification.S3EventNotificationRecord.class);
        filterCreated.filter(record);

        verify(applicableFilter).filter(record);
    }

    @Test
    void sanity_check_on_public_constructor() {
        final SqsOptions sqsOptions = mock(SqsOptions.class);
        when(sqsOptions.getSqsUrl()).thenReturn("https://sqs.amazonaws.com/12345/MyQueue");
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);

        final S3EventFilter filterCreated = new ConfigFilterConfigFactory().createFilter(s3SourceConfig);

        assertThat(filterCreated, notNullValue());
        assertThat(filterCreated, instanceOf(S3EventFilterChain.class));
    }
}