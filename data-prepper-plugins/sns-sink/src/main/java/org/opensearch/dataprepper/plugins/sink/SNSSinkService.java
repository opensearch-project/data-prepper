/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import io.micrometer.core.instrument.Counter;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class responsible for create {@link SnsClient} object, check thresholds,
 * get new buffer and write records into buffer.
 */
public class SNSSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(SNSSinkService.class);

    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_SNS_SUCCESS = "snsSinkObjectsEventsSucceeded";

    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_SNS_FAILED = "snsSinkObjectsEventsFailed";

    private final SNSSinkConfig snsSinkConfig;

    private final Lock reentrantLock;

    private final BufferFactory bufferFactory;

    private final Collection<EventHandle> bufferedEventHandles;

    private final SnsClient snsClient;

    private Buffer currentBuffer;

    private final int maxEvents;

    private final ByteCount maxBytes;

    private final long maxCollectionDuration;

    private final String topicName;

    private final int maxRetries;

    private final Counter numberOfRecordsSuccessCounter;

    private final Counter numberOfRecordsFailedCounter;

    /**
     * @param snsSinkConfig  sns sink related configuration.
     * @param bufferFactory factory of buffer.
     * @param snsClient
     * @param pluginMetrics metrics.
     */
    public SNSSinkService(final SNSSinkConfig snsSinkConfig,
                          final BufferFactory bufferFactory,
                          final SnsClient snsClient,
                          final PluginMetrics pluginMetrics) {
        this.snsSinkConfig = snsSinkConfig;
        this.bufferFactory = bufferFactory;
        this.snsClient = snsClient;
        this.reentrantLock = new ReentrantLock();
        this.bufferedEventHandles = new LinkedList<>();
        this.maxEvents = snsSinkConfig.getThresholdOptions().getEventCount();
        this.maxBytes = snsSinkConfig.getThresholdOptions().getMaximumSize();
        this.maxCollectionDuration = snsSinkConfig.getThresholdOptions().getEventCollectTimeOut().getSeconds();
        this.topicName = snsSinkConfig.getTopicArn();
        this.maxRetries = snsSinkConfig.getMaxUploadRetries();

        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_SNS_SUCCESS);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_SNS_FAILED);
    }


    /**
     * @param records received records and add into buffer.
     */
    void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        if (currentBuffer == null) {
            currentBuffer = bufferFactory.getBuffer();
        }
        try {
            for (Record<Event> record : records) {
                final Event event = record.getData();
                final byte[] encodedBytes = event.toJsonString().getBytes();
                currentBuffer.writeEvent(encodedBytes);
                if (event.getEventHandle() != null) {
                    bufferedEventHandles.add(event.getEventHandle());
                }
                if (checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration)) {
                    LOG.info("Writing {} to SNS with {} events and size of {} bytes.",
                            currentBuffer.getEventCount(), currentBuffer.getSize());
                    final boolean isFlushToSNS = retryFlushToSNS(currentBuffer, topicName);
                    if (isFlushToSNS) {
                        numberOfRecordsSuccessCounter.increment(currentBuffer.getEventCount());
                    } else {
                        numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
                    }
                    releaseEventHandles(true);
                    currentBuffer = bufferFactory.getBuffer();
                }
            }
        } catch (IOException | InterruptedException e) {
            LOG.error("Exception while write event into buffer :", e);
        }
        reentrantLock.unlock();
    }

    /**
     * Check threshold exceeds.
     * @param currentBuffer current buffer.
     * @param maxEvents maximum event provided by user as threshold.
     * @param maxBytes maximum bytes provided by user as threshold.
     * @param maxCollectionDuration maximum event collection duration provided by user as threshold.
     * @return boolean value whether the threshold are met.
     */
    public static boolean checkThresholdExceed(final Buffer currentBuffer, final int maxEvents, final ByteCount maxBytes, final long maxCollectionDuration) {
        if (maxEvents > 0) {
            return currentBuffer.getEventCount() + 1 > maxEvents ||
                    currentBuffer.getDuration() > maxCollectionDuration ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        } else {
            return currentBuffer.getDuration() > maxCollectionDuration ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        }
    }

    private void releaseEventHandles(final boolean result) {
        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }

        bufferedEventHandles.clear();
    }

    /**
     * perform retry in-case any issue occurred, based on max_upload_retries configuration.
     *
     * @param currentBuffer current buffer.
     * @return boolean based on object upload status.
     * @throws InterruptedException interruption during sleep.
     */
    protected boolean retryFlushToSNS(final Buffer currentBuffer, final String topicName) throws InterruptedException {
        boolean isUploadedToSNS = Boolean.FALSE;
        int retryCount = maxRetries;
        do {
            try {
                LOG.info("Trying to Push Msg to SNS");
                final byte[] sinkBufferData = currentBuffer.getSinkBufferData();
                publishToTopic(snsClient, topicName,sinkBufferData);
                isUploadedToSNS = Boolean.TRUE;
            } catch (AwsServiceException | SdkClientException | IOException e) {
                LOG.error("Exception occurred while uploading records to sns. Retry countdown  : {} | exception:",
                        retryCount, e);
                LOG.info("Error Massage {}", e.getMessage());
                --retryCount;
                if (retryCount == 0) {
                    return isUploadedToSNS;
                }
                Thread.sleep(5000);
            }
        } while (!isUploadedToSNS);
        return isUploadedToSNS;
    }

    public void publishToTopic(SnsClient snsClient, String topicName,final byte[] sinkBufferData) {
        LOG.info("Trying to Push Msg to SNS: {}",topicName);
        try {
            PublishRequest request = PublishRequest.builder()
                    .message(new String(sinkBufferData))
                    .topicArn(topicName)
                    .subject(snsSinkConfig.getId())
                    .build();
            PublishResponse result = snsClient.publish(request);
            LOG.info(result.messageId() + " Message sent. Status is " + result.sdkHttpResponse().statusCode());
        }catch (Exception e) {
            throw e;
        }
    }

}
