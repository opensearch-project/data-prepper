/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazonaws.services.s3.event.S3EventNotification;

import java.util.Optional;

public interface S3EventFilter {
    Optional<S3EventNotification.S3EventNotificationRecord> filter(S3EventNotification.S3EventNotificationRecord notification);
}
