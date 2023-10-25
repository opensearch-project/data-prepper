/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.ConcurrentHashMap;

public class TopicEmptinessMetadata {
    private static final long IS_EMPTY_CHECK_INTERVAL_MS = 60000L;

    private long lastIsEmptyCheckTime;
    private Long topicEmptyCheckingOwnerThreadId;
    private ConcurrentHashMap<TopicPartition, Boolean> topicPartitionToIsEmpty;

    public TopicEmptinessMetadata() {
        this.lastIsEmptyCheckTime = 0;
        this.topicEmptyCheckingOwnerThreadId = null;
        this.topicPartitionToIsEmpty = new ConcurrentHashMap<>();
    }

    public void setLastIsEmptyCheckTime(final long timestamp) {
        this.lastIsEmptyCheckTime = timestamp;
    }

    public void setTopicEmptyCheckingOwnerThreadId(final Long threadId) {
        this.topicEmptyCheckingOwnerThreadId = threadId;
    }

    public void updateTopicEmptinessStatus(final TopicPartition topicPartition, final Boolean isEmpty) {
        topicPartitionToIsEmpty.put(topicPartition, isEmpty);
    }

    public long getLastIsEmptyCheckTime() {
        return this.lastIsEmptyCheckTime;
    }

    public Long getTopicEmptyCheckingOwnerThreadId() {
        return this.topicEmptyCheckingOwnerThreadId;
    }

    public boolean isTopicEmpty() {
        return topicPartitionToIsEmpty.values().stream().allMatch(isEmpty -> isEmpty);
    }

    public boolean isCheckDurationExceeded(final long epochTimestamp) {
        return epochTimestamp < lastIsEmptyCheckTime + IS_EMPTY_CHECK_INTERVAL_MS;
    }
}
