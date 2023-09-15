/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.parser;

import org.joda.time.DateTime;
import org.opensearch.dataprepper.plugins.source.s3.S3EventBridgeNotification;
import org.opensearch.dataprepper.plugins.source.s3.S3EventNotification;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public class ParsedMessage {
    private final Message message;
    private boolean failedParsing;
    private String bucketName;
    private String objectKey;
    private String eventName;
    private DateTime eventTime;
    private boolean emptyNotification;
    private String detailType;

    public ParsedMessage(final Message message, final boolean failedParsing) {
        this.message = message;
        this.failedParsing = failedParsing;
        this.emptyNotification = true;
    }

    // S3EventNotification contains only one S3EventNotificationRecord
     ParsedMessage(final Message message, final List<S3EventNotification.S3EventNotificationRecord> notificationRecords) {
        this.message = message;
        this.bucketName = notificationRecords.get(0).getS3().getBucket().getName();
        this.objectKey = notificationRecords.get(0).getS3().getObject().getUrlDecodedKey();
        this.eventName = notificationRecords.get(0).getEventName();
        this.eventTime = notificationRecords.get(0).getEventTime();
        this.failedParsing = false;
        this.emptyNotification = notificationRecords.isEmpty();
    }

    ParsedMessage(final Message message, final S3EventBridgeNotification eventBridgeNotification) {
        this.message = message;
        this.bucketName = eventBridgeNotification.getDetail().getBucket().getName();
        this.objectKey = eventBridgeNotification.getDetail().getObject().getUrlDecodedKey();
        this.detailType = eventBridgeNotification.getDetailType();
        this.eventTime = eventBridgeNotification.getTime();
    }

    public Message getMessage() {
        return message;
    }

    public boolean isFailedParsing() {
        return failedParsing;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public String getEventName() {
        return eventName;
    }

    public DateTime getEventTime() {
        return eventTime;
    }

    public boolean isEmptyNotification() {
        return emptyNotification;
    }

    public String getDetailType() {
        return detailType;
    }
}
