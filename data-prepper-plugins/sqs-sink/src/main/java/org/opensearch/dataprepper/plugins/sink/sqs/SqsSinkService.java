/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import software.amazon.awssdk.services.sqs.SqsClient;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.failures.DlqObject;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

public class SqsSinkService extends SqsSinkExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SqsSinkService.class);
    public static final int MAX_BYTES_IN_BATCH = 256*1024;
    public static final int MAX_EVENT_SIZE = 256*1024;

    private final Map<String, SqsSinkBatch> batchUrlMap;
    private final String queueUrl;
    private final String groupId;
    private final String deDupId;
    private final SqsClient sqsClient;
    private final boolean isDynamicGroupId;
    private final boolean isDynamicDeDupId;
    private final boolean isDynamicQueueUrl;
    private final ExpressionEvaluator expressionEvaluator;
    private final ReentrantLock reentrantLock;
    private final SqsThresholdConfig thresholdConfig;
    private final SqsSinkConfig sqsSinkConfig;
    private final SinkContext sinkContext;
    private final OutputCodec codec;
    private final BufferFactory inMemoryBufferFactory;
    private final SqsSinkMetrics sinkMetrics;
    private final DlqPushHandler dlqPushHandler;
    private final List<DlqObject> dlqObjects;

    public SqsSinkService(final SqsSinkConfig sqsSinkConfig,
                          final SqsClient sqsClient,
                          final ExpressionEvaluator expressionEvaluator,
                          final OutputCodec codec,
                          final SinkContext sinkContext,
                          final DlqPushHandler dlqPushHandler,
                          final PluginMetrics pluginMetrics) {
        batchUrlMap = new HashMap<>();
        dlqObjects = new ArrayList<>();
        inMemoryBufferFactory =new InMemoryBufferFactory();
        this.sqsClient = sqsClient;
        this.dlqPushHandler = dlqPushHandler;
        this.sinkContext = sinkContext;
        this.expressionEvaluator = expressionEvaluator;
        this.thresholdConfig = sqsSinkConfig.getThresholdConfig();
        this.codec = codec;
        this.sqsSinkConfig = sqsSinkConfig;
        reentrantLock = new ReentrantLock();
        this.sinkMetrics = new SqsSinkMetrics(pluginMetrics);

        queueUrl = sqsSinkConfig.getQueueUrl();
        isDynamicQueueUrl = queueUrl.contains("${");
        if (isDynamicQueueUrl) {
            if (!expressionEvaluator.isValidFormatExpression(queueUrl)) {
                throw new IllegalArgumentException("Invalid queue url expression");
            }
        }

        groupId = sqsSinkConfig.getGroupId();
        isDynamicGroupId = groupId != null && groupId.contains("${");
        if (isDynamicGroupId) {
            if (!expressionEvaluator.isValidFormatExpression(groupId)) {
                throw new IllegalArgumentException("Invalid groupId expression");
            }
        }

        deDupId = sqsSinkConfig.getDeDuplicationId();
        isDynamicDeDupId = deDupId != null && deDupId.contains("${");
        if (isDynamicDeDupId) {
            if (!expressionEvaluator.isValidFormatExpression(deDupId)) {
                throw new IllegalArgumentException("Invalid deduplicationId expression");
            }
        }

    }

    @Override
    public boolean exceedsMaxEventSizeThreshold(final long estimatedSize) {
        return estimatedSize > MAX_EVENT_SIZE;
    }

    @Override
    public void pushDLQList() {
        if (dlqObjects.size() == 0) {
            return;
        }
        boolean result = false;
        if (dlqPushHandler != null) {
            result = dlqPushHandler.perform(dlqObjects);
        }
        for (final DlqObject dlqObject : dlqObjects) {
            dlqObject.releaseEventHandles(result);
        }
        dlqObjects.clear();
    }

    @Override
    public void pushFailedObjectsToDlq(Object object) {
        List<SqsSinkBatch> failedBatches = (List<SqsSinkBatch>) object;
        for (SqsSinkBatch failedBatch: failedBatches) {
            for (Map.Entry<String, SqsSinkBatchEntry> entry: failedBatch.getEntries().entrySet()) {
                addBatchEntryToDLQ(entry.getValue(), "Failed to write to sink after maxRetries");
            }
            batchUrlMap.remove(failedBatch.getQueueUrl());
        }
    }

    @Override
    public long getEstimatedSize(final Event event) throws Exception {
        return codec.getEstimatedSize(event, new OutputCodecContext());
    }

    @Override
    public boolean willExceedMaxBatchSize(final Event event, final long estimatedSize) throws Exception {
        String qUrl = getQueueUrl(event, false);
        if (qUrl == null)
            return false;
        SqsSinkBatch batch = batchUrlMap.get(qUrl);
        if (batch == null)
            return false;
        boolean result = batch.willExceedLimits(estimatedSize);
        if (result) {
            setFlushReady(qUrl, batch);
        }
        return result;
    }

    private boolean doFlushBatch(SqsSinkBatch batch) {
        boolean flushSuccess = batch.flushOnce(
            (batchEntry, exceptionMessage ) -> {
                addBatchEntryToDLQ(batchEntry, exceptionMessage);
             });

        // Sending to DLQ is also considered success (because no
        // retry needed)
        return flushSuccess;
    }

    @Override
    public Object doFlushOnce(Object previousFailedBatches) {
        List<SqsSinkBatch> failedBatches = new ArrayList<>();
        List<String> successQueueUrls = new ArrayList<>();
        if (previousFailedBatches != null) {
            List<SqsSinkBatch> pFailedBatches = (List<SqsSinkBatch>) previousFailedBatches;
            for (SqsSinkBatch failedBatch: pFailedBatches) {
                if (!doFlushBatch(failedBatch)) {
                    failedBatches.add(failedBatch);
                } else {
                    successQueueUrls.add(failedBatch.getQueueUrl());
                }
            }
        } else {
            Iterator<Map.Entry<String, SqsSinkBatch>> iterator = batchUrlMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, SqsSinkBatch> qUrlEntry = iterator.next();
                SqsSinkBatch batch = qUrlEntry.getValue();
                if (batch.isReady()) {
                    if (!doFlushBatch(batch)) {
                        failedBatches.add(batch);
                    } else {
                        successQueueUrls.add(batch.getQueueUrl());
                    }
                }
            }
        }
        for (final String qUrl : successQueueUrls) {
            batchUrlMap.remove(qUrl);
        }
        if (failedBatches.size() == 0)
            return null;
        return failedBatches;
    }

    private String getQueueUrl(final Event event, boolean logError) {
        String qUrl = queueUrl;
        if (isDynamicQueueUrl) {
            try {
                Object obj = event.formatString(queueUrl, expressionEvaluator);
                if (obj instanceof String) {
                    qUrl = (String) obj;
                } else {
                    throw new RuntimeException("Evaluated queue url is not a string");
                }
            } catch (Exception e) {
                qUrl = null;
                if (logError) {
                    LOG.error(NOISY, "Invalid queueURL expression {} ", e.getMessage());
                    addEventToDLQList(event, e);
                }
            }
        }
        return qUrl;
    }

    private String getGroupId(final Event event) {
        String gId = groupId;
        if (isDynamicGroupId) {
            try {
                Object obj = event.formatString(groupId, expressionEvaluator);
                if (obj instanceof String) {
                    gId = (String) obj;
                } else {
                    throw new RuntimeException("Evaluated group id is not a string");
                }
            } catch (Exception e) {
                LOG.error(NOISY, "Invalid groupId expression {}, using random groupId ", e.getMessage());
            }
        }
        return gId;
    }

    private String getDeDupId(final Event event) {
        String ddId = deDupId;
        if (isDynamicDeDupId) {
            try {
                Object obj = event.formatString(deDupId, expressionEvaluator);
                if (obj instanceof String) {
                    ddId = (String) obj;
                } else {
                    throw new RuntimeException("Evaluated deduplicate id is not a string");
                }
            } catch (Exception e) {
                LOG.error(NOISY, "Invalid deDupId expression {}, using random deDupId ", e.getMessage());
            }
        }
        return ddId;
    }


    @Override
    public int getMaxRetries() {
        return sqsSinkConfig.getMaxRetries();
    }

    Map<String, SqsSinkBatch> getBatchUrlMap() {
        return batchUrlMap;
    }

    @Override
    public boolean addToBuffer(final Event event, final long estimatedSize) throws Exception {
        String qUrl = getQueueUrl(event, true);
        if (qUrl == null) {
            return false;
        }
        SqsSinkBatch batch = batchUrlMap.get(qUrl);
        if (batch == null) {
            final OutputCodecContext codecContext = OutputCodecContext.fromSinkContext(sinkContext);
            batch = new SqsSinkBatch(inMemoryBufferFactory, sqsClient, sinkMetrics, qUrl, codec, codecContext, thresholdConfig.getMaxMessageSizeBytes(), thresholdConfig.getMaxEventsPerMessage());

            batchUrlMap.put(qUrl, batch);
        }
        String gId = getGroupId(event);
        String ddId = getDeDupId(event);
        boolean isFull = batch.addEntry(event, gId, ddId, estimatedSize);
        if (isFull) {
            setFlushReady(qUrl, batch);
        }
        return isFull;
    }

    private boolean setFlushReady(final String queueUrl, final SqsSinkBatch batch) {
        try {
            batch.setFlushReady();
            return true;
        } catch (Exception e) {
            for (Map.Entry<String, SqsSinkBatchEntry> entry: batch.getEntries().entrySet()) {
                addBatchEntryToDLQ(entry.getValue(), "Failed to setFlushReady for the batch");
            }
            batchUrlMap.remove(queueUrl);
            return false;
        }
    }

    @Override
    public boolean exceedsFlushTimeInterval() {
        long now = Instant.now().getEpochSecond();
        boolean result = false;

        Iterator<Map.Entry<String, SqsSinkBatch>> iterator = batchUrlMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, SqsSinkBatch> qUrlEntry = iterator.next();
            String qUrl = qUrlEntry.getKey();
            SqsSinkBatch batch = qUrlEntry.getValue();
            if (now - batch.getLastFlushedTime() > thresholdConfig.getFlushInterval()) {
                result = result || setFlushReady(qUrl, batch);
            }
        }
        return result;
    }

    private void addBatchEntryToDLQ(final SqsSinkBatchEntry batchEntry, final String errorMessage) {
        addMessageToDLQ(batchEntry.getBody(), batchEntry.getEventHandles(), errorMessage);
    }

    private void addMessageToDLQ(final String message, final List<EventHandle> eventHandles, final String errorMessage) {
        if (dlqPushHandler != null) {
            SqsSinkDlqData sqsSinkDlqData = SqsSinkDlqData.createDlqData(message, errorMessage);
            DlqObject dlqObject = DlqObject.createDlqObject(dlqPushHandler.getPluginSetting(), eventHandles, sqsSinkDlqData);
            dlqObjects.add(dlqObject);
        } else {
            for (final EventHandle handle: eventHandles) {
                handle.release(false);
            }
        }
    }

    @Override
    public void addEventToDLQList(final Event event, Throwable ex) {
        List<EventHandle> eventHandles = new ArrayList<>();
        eventHandles.add(event.getEventHandle());
        addMessageToDLQ(event.toJsonString(), eventHandles, ex.getMessage());
    }

    @Override
    public void lock() {
        reentrantLock.lock();
    }

    @Override
    public void unlock() {
        reentrantLock.unlock();
    }

    void output(Collection<Record<Event>> records) {
        execute(records);
    }
}
