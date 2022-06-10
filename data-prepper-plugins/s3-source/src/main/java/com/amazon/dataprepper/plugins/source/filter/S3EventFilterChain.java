/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazonaws.services.s3.event.S3EventNotification;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An {@link S3EventFilter} which filters through a set of other {@link S3EventFilter} objects.
 */
class S3EventFilterChain implements S3EventFilter {
    private final List<S3EventFilter> delegateFilters;

    public S3EventFilterChain(final List<S3EventFilter> delegateFilters) {
        this.delegateFilters = Objects.requireNonNull(delegateFilters);
    }

    @Override
    public Optional<S3EventNotification.S3EventNotificationRecord> filter(final S3EventNotification.S3EventNotificationRecord notification) {
        if (notification == null)
            return Optional.empty();

        Optional<S3EventNotification.S3EventNotificationRecord> lastResult = Optional.of(notification);
        for (S3EventFilter innerFilter : delegateFilters) {
            lastResult = innerFilter.filter(lastResult.get());

            if (!lastResult.isPresent())
                break;
        }

        return lastResult;
    }
}
