/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.Range;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class TopicPartitionCommitTracker {
    private long committedOffset;
    private final TopicPartition topicPartition;
    private final Map<Long, Range<Long>> offsetMaxMap;
    private final Map<Long, Range<Long>> offsetMinMap;

    public TopicPartitionCommitTracker(final TopicPartition topicPartition, Long committedOffset) {
        this.topicPartition = topicPartition;
        this.committedOffset = Objects.nonNull(committedOffset) ? committedOffset-1 : -1L;
        this.offsetMaxMap = new HashMap<>();
        this.offsetMinMap = new HashMap<>();
        this.offsetMaxMap.put(this.committedOffset, Range.between(this.committedOffset, this.committedOffset));
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
                committedOffset = maxValue;
                offsetMaxMap.put(committedOffset, Range.between(committedOffset, committedOffset));
                return new OffsetAndMetadata(committedOffset + 1);
            }
        }
        return null;
    }

}
