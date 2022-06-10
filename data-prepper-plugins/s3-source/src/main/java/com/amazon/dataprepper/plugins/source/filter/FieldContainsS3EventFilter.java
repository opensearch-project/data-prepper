/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazonaws.services.s3.event.S3EventNotification;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

class FieldContainsS3EventFilter<T> implements S3EventFilter {
    private final Function<S3EventNotification.S3EventNotificationRecord, T> fieldProvider;
    private final Collection<T> eligibleValues;

    public FieldContainsS3EventFilter(final Function<S3EventNotification.S3EventNotificationRecord, T> fieldProvider, final Collection<T> eligibleValues) {
        Objects.requireNonNull(eligibleValues);
        if (eligibleValues.isEmpty())
            throw new IllegalArgumentException("The eligibleValues must be non-empty.");

        this.eligibleValues = new HashSet<>(eligibleValues);
        this.fieldProvider = fieldProvider;
    }

    @Override
    public Optional<S3EventNotification.S3EventNotificationRecord> filter(final S3EventNotification.S3EventNotificationRecord notification) {
        final T fieldValue = fieldProvider.apply(notification);

        if (eligibleValues.contains(fieldValue))
            return Optional.of(notification);

        return Optional.empty();
    }
}
