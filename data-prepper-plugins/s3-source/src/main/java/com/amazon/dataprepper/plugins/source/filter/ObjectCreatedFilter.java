/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazonaws.services.s3.event.S3EventNotification;

import java.util.Optional;

public class ObjectCreatedFilter implements S3EventFilter {
    @Override
    public Optional<S3EventNotification.S3EventNotificationRecord> filter(final S3EventNotification.S3EventNotificationRecord notification) {
        if (notification.getEventName().startsWith("ObjectCreated"))
            return Optional.of(notification);
        else
            return Optional.empty();
    }
}
