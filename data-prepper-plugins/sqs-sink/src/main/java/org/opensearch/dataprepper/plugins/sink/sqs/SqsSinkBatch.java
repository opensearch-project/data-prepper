/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.model.event.Event;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;

import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sqs.model.RequestThrottledException;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.SqsClient;


import java.time.Instant;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

public class SqsSinkBatch {
    public static final int MAX_MESSAGES_PER_BATCH = 10;
    public static final int MAX_BATCH_SIZE_BYTES = 256*1024;
    private static final String SQS_FIFO_SUFFIX = ".fifo";
    private long lastFlushedTime;
    private Map<String, SqsSinkBatchEntry> entries;
    private boolean flushReady;
    private boolean fifoQueue;
    private String queueUrl;
    private final long maxMessageSize;
    private final int maxEvents;
    private final OutputCodecContext codecContext;
    private final OutputCodec codec;
    private final SqsClient sqsClient;
    private final BufferFactory bufferFactory;
    private final SqsSinkMetrics sinkMetrics;
    private String currentId;
    private SqsSinkBatchEntry currentBatchEntry;
    private final BiConsumer<SqsSinkBatchEntry, String> addToDLQList;

    public SqsSinkBatch(final BufferFactory bufferFactory,
                        final SqsClient sqsClient,
                        final SqsSinkMetrics sinkMetrics,
                        final String queueUrl,
                        final OutputCodec codec,
                        final OutputCodecContext codecContext,
                        final SqsThresholdConfig thresholdConfig,
                        final BiConsumer<SqsSinkBatchEntry, String> addToDLQList) {
        this.bufferFactory = bufferFactory;
        this.maxMessageSize = thresholdConfig.getMaxMessageSizeBytes();
        this.maxEvents = thresholdConfig.getMaxEventsPerMessage();
        this.addToDLQList = addToDLQList;
        this.codec = codec;
        this.sinkMetrics = sinkMetrics;
        this.codecContext = codecContext;
        this.queueUrl = queueUrl;
        this.sqsClient = sqsClient;
        lastFlushedTime = Instant.now().toEpochMilli();
        flushReady = false;
        fifoQueue = queueUrl.endsWith(SQS_FIFO_SUFFIX);
        entries = new HashMap<>();
        currentBatchEntry = null;
        currentId = null;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    private boolean isFull() {
        return entries.size() == MAX_MESSAGES_PER_BATCH && currentBatchEntry.getEventCount() == maxEvents;
    }

    public boolean willExceedLimits(long estimatedSize) {
        if (getCurrentBatchSize() + estimatedSize > MAX_BATCH_SIZE_BYTES) {
            return true;
        }
        if (currentBatchEntry != null) {
            if (currentBatchEntry.getEventCount() < maxEvents &&
                currentBatchEntry.getSize() + estimatedSize <= maxMessageSize) {
                return false;
            }
        }
        if (entries.size() == MAX_MESSAGES_PER_BATCH) {
            return true;
        }
        return false;
    }

    public boolean addEntry(final Event event, String groupId, String deDupId, final long estimatedSize) throws Exception {
        if (currentBatchEntry != null) {
            if (currentBatchEntry.getEventCount() < maxEvents &&
                currentBatchEntry.getSize() + estimatedSize < maxMessageSize) {
                currentBatchEntry.addEvent(event);
                return isFull();
            } else {
                try {
                    currentBatchEntry.complete();
                } catch (IOException ex) {
                    addToDLQList.accept(currentBatchEntry, ex.getMessage());
                    entries.remove(currentId);
                }
            }
        }
        if (entries.size() == MAX_MESSAGES_PER_BATCH) {
            throw new RuntimeException("Exceeds max messages per batch");
        }
        if (groupId == null) {
            groupId = UUID.randomUUID().toString();
        }
        if (deDupId == null) {
            deDupId = UUID.randomUUID().toString();
        }
        currentBatchEntry = new SqsSinkBatchEntry(bufferFactory.getBuffer(), groupId, deDupId, codec, codecContext);

        currentBatchEntry.addEvent(event);
        final String id = UUID.randomUUID().toString();
        currentId = id;
        entries.put(id, currentBatchEntry);
        return isFull();
    }

    public long getLastFlushedTime() {
        return lastFlushedTime;
    }

    public long getCurrentBatchSize() {
        long sum = 0;
        for (Map.Entry<String, SqsSinkBatchEntry> entry : entries.entrySet()) {
            SqsSinkBatchEntry batchEntry = entry.getValue();
            sum += batchEntry.getSize();
            if (fifoQueue) {
                if (batchEntry.getGroupId() != null) {
                    sum += batchEntry.getGroupId().length();
                }
                if (batchEntry.getDedupId() != null) {
                    sum += batchEntry.getDedupId().length();
                }
            }
        }
        return sum;
    }

    public int getEventCount() {
        return entries.values().stream().mapToInt(SqsSinkBatchEntry::getEventCount).sum();
    }

    public void setFlushReady() {
        Iterator<Map.Entry<String, SqsSinkBatchEntry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, SqsSinkBatchEntry> entry = iterator.next();
            try {
                entry.getValue().complete();
            } catch (IOException ex) {
                addToDLQList.accept(entry.getValue(), ex.getMessage());
                iterator.remove();
            }
        }
        flushReady = true;
    }

    public boolean isReady() {
        return flushReady;
    }

    private SendMessageBatchRequestEntry getRequestEntry(final String id, final SqsSinkBatchEntry entry) {
        SendMessageBatchRequestEntry.Builder builder = SendMessageBatchRequestEntry.builder()
            .id(id)
            .messageBody(entry.getBody());
        if (fifoQueue) {
            builder = builder
                .messageGroupId(entry.getGroupId())
                .messageDeduplicationId(entry.getDedupId());
        }
        return builder.build();
    }

    private boolean isRetryableException(SqsException e) {
        return (e instanceof RequestThrottledException);
    }

    private long getEntrySize(SqsSinkBatchEntry entry) {
        long result = entry.getBody().getBytes(StandardCharsets.UTF_8).length;
        if (fifoQueue) {
            if (entry.getGroupId() != null) {
                result += entry.getGroupId().getBytes(StandardCharsets.UTF_8).length;
            }
            if (entry.getDedupId() != null) {
                result += entry.getDedupId().getBytes(StandardCharsets.UTF_8).length;
            }
        }
        return result;
    }

    public boolean flushOnce() {
        if (!isReady() || entries.size() == 0) {
            return true;
        }
        SendMessageBatchResponse flushResponse;
        List<SendMessageBatchRequestEntry> requestEntries = new ArrayList<>();
        long requestSize = 0;
        for (Map.Entry<String, SqsSinkBatchEntry> groupEntry: entries.entrySet()) {
            final String id = groupEntry.getKey();
            final SqsSinkBatchEntry entry = groupEntry.getValue();
            requestEntries.add(getRequestEntry(id, entry));
            requestSize += getEntrySize(entry);
        }
        SendMessageBatchRequest batchRequest =
            SendMessageBatchRequest.builder()
            .queueUrl(queueUrl)
            .entries(requestEntries)
            .build();
        try {
            flushResponse = sqsClient.sendMessageBatch(batchRequest);
            sinkMetrics.recordRequestSize((double)requestSize);

        } catch (SqsException e) {
            sinkMetrics.incrementRequestsFailedCounter(1);
            sinkMetrics.incrementEventsFailedCounter(entries.size());
            if (!isRetryableException(e)) {
                for (Map.Entry<String, SqsSinkBatchEntry> entry: entries.entrySet()) {
                    addToDLQList.accept(entry.getValue(), e.getMessage());
                }
                entries.clear();
                flushResponse = null;
                return true;
            }
            return false;
        }
        sinkMetrics.incrementRequestsSuccessCounter(1);

        boolean flushResult = false;
        if (!flushResponse.hasFailed()) {
            for (SendMessageBatchRequestEntry entry: requestEntries) {
                SqsSinkBatchEntry batchEntry = entries.get(entry.id());
                batchEntry.releaseEventHandles(true);
                sinkMetrics.incrementEventsSuccessCounter(batchEntry.getEventCount());
            }
            entries.clear();
        } else {
            Map<String, SqsSinkBatchEntry> newEntries = new HashMap<>();
            sinkMetrics.incrementEventsFailedCounter(flushResponse.failed().size());
            for (BatchResultErrorEntry errorEntry : flushResponse.failed()) {
                SqsSinkBatchEntry batchEntry = entries.get(errorEntry.id());
                if (!errorEntry.senderFault()) {
                    newEntries.put(errorEntry.id(), batchEntry);
                } else {
                    addToDLQList.accept(batchEntry, errorEntry.message());
                }
                entries.remove(errorEntry.id());
            }
            sinkMetrics.incrementEventsSuccessCounter(entries.size());
            for (Map.Entry<String, SqsSinkBatchEntry> entry: entries.entrySet()) {
                entry.getValue().releaseEventHandles(true);
            }
            entries.clear();
            entries = newEntries;
        }
        lastFlushedTime = Instant.now().toEpochMilli();
        return entries.size() == 0;
    }

    public Map<String, SqsSinkBatchEntry> getEntries() {
        return entries;
    }
}

