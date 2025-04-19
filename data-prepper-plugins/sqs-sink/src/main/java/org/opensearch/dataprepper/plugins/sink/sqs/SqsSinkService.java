/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.sink.SinkExecutor;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;
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
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

public class SqsSinkService extends SinkExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SqsSinkService.class);
    public static final int MAX_BYTES_IN_BATCH = 256*1024;
    public static final int MAX_EVENT_SIZE = 256*1024;

    final Map<String, SqsSinkBatch> batchUrlMap;
    final String queueUrl;
    String groupId;
    final SqsClient sqsClient;
    final boolean isDynamicGroupId;
    final boolean isDynamicQueueUrl;
    final ExpressionEvaluator expressionEvaluator;
    final ReentrantLock reentrantLock;
    final SqsThresholdConfig thresholdConfig;
    final SqsSinkConfig sqsSinkConfig;
    final String defaultGroupId;
    final OutputCodecContext codecContext;
    final OutputCodec codec;
    final BufferFactory inMemoryBufferFactory;
    private final SqsSinkMetrics sinkMetrics;
    private DlqPushHandler dlqPushHandler;
    private List<DlqObject> dlqObjects;

    public SqsSinkService(final SqsSinkConfig sqsSinkConfig,
                          final SqsClient sqsClient,
                          final ExpressionEvaluator expressionEvaluator,
                          final OutputCodec codec,
                          final OutputCodecContext codecContext,
                          final DlqPushHandler dlqPushHandler,
                          final PluginMetrics pluginMetrics) {
        batchUrlMap = new HashMap<>();
        dlqObjects = new ArrayList<>();
        inMemoryBufferFactory =new InMemoryBufferFactory();
        this.sqsClient = sqsClient;
        this.dlqPushHandler = dlqPushHandler;
        this.codecContext = codecContext;
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

        defaultGroupId = UUID.randomUUID().toString();
        groupId = sqsSinkConfig.getGroupId();
        isDynamicGroupId = groupId != null && groupId.contains("${");
        if (isDynamicGroupId) {
            if (!expressionEvaluator.isValidFormatExpression(groupId)) {
                throw new IllegalArgumentException("Invalid groupId expression");
            }
        } else if (groupId == null) {
            groupId = defaultGroupId;
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
    }

    @Override
    public void pushFailedObjectsToDlq(Object object) {
        List<SqsSinkBatch> failedBatches = (List<SqsSinkBatch>) object;
        for (SqsSinkBatch failedBatch: failedBatches) {
            for (Map.Entry<String, SqsSinkBatchEntry> entry: failedBatch.getEntries().entrySet()) {
                addBatchEntryToDLQ(entry.getValue(), "Failed to write to sink after maxRetries");
            }
        }
        pushDLQList();
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
        //boolean result = (batch.getCurrentBatchSize() + estimatedSize) > MAX_BYTES_IN_BATCH;
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
        
        // Sending DLQ is also considered success (because no
        // retry needed)
        if (flushSuccess) {
            batchUrlMap.remove(batch.getQueueUrl());
        }
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
                    }
                }
            }
        }
        if (failedBatches.size() == 0)
            return null;
        return failedBatches;
    }

    private String getQueueUrl(final Event event, boolean logError) {
        String qUrl = queueUrl;
        if (isDynamicQueueUrl) {
            try {
                Object obj = expressionEvaluator.evaluate(queueUrl, event);
                if (obj instanceof String) {
                    qUrl = (String) obj;
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

    private String getGroupId(final Event event, final boolean logError) {
        String gId = groupId;
        if (isDynamicGroupId) {
            try {
                Object obj = expressionEvaluator.evaluate(groupId, event);
                if (obj instanceof String) {
                    gId = (String) obj;
                }
            } catch (Exception e) {
                if (logError) {
                    LOG.error(NOISY, "Invalid groupId expression {}, using default groupId ", e.getMessage());
                }
                gId = defaultGroupId;
            }
        }
        return gId;
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
            batch = new SqsSinkBatch(inMemoryBufferFactory, sqsClient, sinkMetrics, qUrl, codec, codecContext, thresholdConfig.getMaxMessageSizeBytes(), thresholdConfig.getMaxEventsPerMessage());
            
            batchUrlMap.put(qUrl, batch);
        }
        String gId = getGroupId(event, true);
        boolean isFull = batch.addEntry(event, gId, estimatedSize);
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
