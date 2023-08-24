/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.commons.lang3.Range;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TopicPartitionCommitTracker {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceCustomConsumer.class);
    private long committedOffset;
    private long committedRecordCount;
    private long initialOffset;
    private final TopicPartition topicPartition;
    private final Map<Long, Range<Long>> offsetMaxMap;
    private final Map<Long, Range<Long>> offsetMinMap;

    public TopicPartitionCommitTracker(final TopicPartition topicPartition, final Long initialOffset) {
        this.topicPartition = topicPartition;
        this.initialOffset = initialOffset;
        LOG.info("Created commit tracker for partition: {}, initialOffset: {}", topicPartition, initialOffset);

        this.committedOffset = initialOffset-1L;
        this.committedRecordCount = 0;
        this.offsetMaxMap = new HashMap<>();
        this.offsetMinMap = new HashMap<>();
        this.offsetMaxMap.put(this.committedOffset, Range.between(this.committedOffset, this.committedOffset));
    }

    public long getInitialOffset() {
        return initialOffset;
    }

    public long getCommittedOffset() {
        return committedOffset;
    }

    public long getCommittedRecordCount() {
        long count = committedRecordCount;
        committedRecordCount = 0;
        return count;
    }

    public TopicPartitionCommitTracker(final String topic, final int partition, Long committedOffset) {
        this(new TopicPartition(topic, partition), committedOffset);
    }

    public OffsetAndMetadata addCompletedOffsets(final Range<Long> offsetRange) {
        Long min = offsetRange.getMinimum();
        Long max = offsetRange.getMaximum();
        boolean merged = false;
        if (offsetMaxMap.containsKey(min - 1)) {
            Range<Long> entry = offsetMaxMap.get(min - 1);
            offsetMaxMap.remove(min - 1);
            offsetMinMap.remove(entry.getMinimum());
            min = entry.getMinimum();
            Range<Long> newEntry = Range.between(min, max);
            offsetMaxMap.put(max, newEntry);
            offsetMinMap.put(min, newEntry);
            merged = true;
        }
        if (offsetMinMap.containsKey(max + 1)) {
            Range<Long> entry = offsetMinMap.get(max + 1);
            offsetMinMap.remove(max + 1);
            if (merged) {
                offsetMinMap.remove(min);   
                offsetMaxMap.remove(max);   
            }
            max = entry.getMaximum();
            offsetMaxMap.remove(max);
            Range<Long> newEntry = Range.between(min, max);
            offsetMaxMap.put(max, newEntry);
            offsetMinMap.put(min, newEntry);
            merged = true;
        }
        if (!merged) {
            offsetMaxMap.put(max, offsetRange);
            offsetMinMap.put(min, offsetRange);
            return null;
        }
        if (offsetMinMap.containsKey(committedOffset)) {
            Long maxValue = offsetMinMap.get(committedOffset).getMaximum();
            if (maxValue != committedOffset) {
                offsetMinMap.remove(committedOffset);
                committedRecordCount += (maxValue - committedOffset);
                committedOffset = maxValue;
                offsetMaxMap.put(committedOffset, Range.between(committedOffset, committedOffset));
                return new OffsetAndMetadata(committedOffset + 1);
            }
        }
        return null;
    }

}
