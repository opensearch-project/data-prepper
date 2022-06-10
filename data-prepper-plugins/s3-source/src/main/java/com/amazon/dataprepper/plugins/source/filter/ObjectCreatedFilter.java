/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;
import com.amazonaws.services.s3.event.S3EventNotification;

import java.util.Optional;

class ObjectCreatedFilter implements S3EventFilter {
    private ObjectCreatedFilter() {}

    @Override
    public Optional<S3EventNotification.S3EventNotificationRecord> filter(final S3EventNotification.S3EventNotificationRecord notification) {
        if (notification.getEventName().startsWith("ObjectCreated"))
            return Optional.of(notification);
        else
            return Optional.empty();
    }

    static class Factory implements FilterConfigFactory {
        @Override
        public Optional<S3EventFilter> createFilter(final S3SourceConfig config) {
            return Optional.of(new ObjectCreatedFilter());
        }
    }
}
