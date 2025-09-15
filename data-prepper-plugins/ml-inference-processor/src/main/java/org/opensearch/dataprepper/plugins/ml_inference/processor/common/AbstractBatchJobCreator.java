/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.ml_inference.processor.dlq.MLBatchJobFailedDlqData;
import org.opensearch.dataprepper.plugins.ml_inference.processor.util.MlCommonRequester;
import software.amazon.awssdk.auth.signer.Aws4Signer;

import java.util.List;
import java.util.Optional;

import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.LOG;

public abstract class AbstractBatchJobCreator implements MLBatchJobCreator {
    public static final String NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION = "batchJobsCreationSucceeded";
    public static final String NUMBER_OF_FAILED_BATCH_JOBS_CREATION = "batchJobsCreationFailed";
    public static final String NUMBER_OF_RECORDS_FAILED_IN_BATCH_JOB = "recordsFailedInBatchJobCreation";
    public static final String NUMBER_OF_RECORDS_SUCCEEDED_IN_BATCH_JOB = "recordsSucceededInBatchJobCreation";
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final long MAX_RETRY_WINDOW_MS = 600_000; // 10 minutes
    protected static final int TOO_MANY_REQUESTS = 429;
    protected final MLProcessorConfig mlProcessorConfig;
    protected final AwsCredentialsSupplier awsCredentialsSupplier;
    protected final Counter numberOfBatchJobsSuccessCounter;
    protected final Counter numberOfBatchJobsFailedCounter;
    protected final Counter numberOfRecordsFailedCounter;
    protected final Counter numberOfRecordsSuccessCounter;
    protected final List<String> tagsOnFailure;
    protected final MlCommonRequester mlCommonRequester;
    protected DlqPushHandler dlqPushHandler = null;
    private static final Aws4Signer signer;
    static {
        signer = Aws4Signer.create();
    }
    // Constructor
    public AbstractBatchJobCreator(MLProcessorConfig mlProcessorConfig,
                                   AwsCredentialsSupplier awsCredentialsSupplier,
                                   final PluginMetrics pluginMetrics, final DlqPushHandler dlqPushHandler) {
        this.mlProcessorConfig = mlProcessorConfig;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.numberOfBatchJobsSuccessCounter = pluginMetrics.counter(NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION);
        this.numberOfBatchJobsFailedCounter = pluginMetrics.counter(NUMBER_OF_FAILED_BATCH_JOBS_CREATION);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(
                NUMBER_OF_RECORDS_FAILED_IN_BATCH_JOB);
        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(
                NUMBER_OF_RECORDS_SUCCEEDED_IN_BATCH_JOB);
        this.tagsOnFailure = mlProcessorConfig.getTagsOnFailure();
        this.mlCommonRequester = new MlCommonRequester(signer, mlProcessorConfig, awsCredentialsSupplier);
        this.dlqPushHandler = dlqPushHandler;
    }

    // Add common logic here that both subclasses can share
    public void incrementSuccessCounter() {
        numberOfBatchJobsSuccessCounter.increment();
    }

    public void incrementFailureCounter() {
        numberOfBatchJobsFailedCounter.increment();
    }

    // Abstract methods for batch job creation, specific to the implementations
    public abstract void createMLBatchJob(List<Record<Event>> inputRecords, List<Record<Event>> resultRecords);

    /*
     * Add the failure tags to the records that aren't processed
     */
    protected List<Record<Event>> addFailureTags(List<Record<Event>> records) {
        if (tagsOnFailure == null || tagsOnFailure.isEmpty()) {
            return records;
        }
        // Add failure tags to each event in the batch
        for (Record<Event> record : records) {
            Event event = record.getData();
            EventMetadata metadata = event.getMetadata();
            if (metadata != null) {
                metadata.addTags(tagsOnFailure);
            } else {
                LOG.warn("Event metadata is null, cannot add failure tags.");
            }
        }
        return records;
    }

    protected DlqObject createDlqObjectFromEvent(final Event event,
                                               final int status,
                                               final String message) {
        String bucket = Optional.ofNullable(event.getJsonNode().get("bucket"))
                .map(JsonNode::asText)
                .orElse("");

        String key = Optional.ofNullable(event.getJsonNode().get("key"))
                .map(JsonNode::asText)
                .orElse("");

        return DlqObject.builder()
                .withEventHandle(event.getEventHandle())
                .withFailedData(MLBatchJobFailedDlqData.builder()
                        .withS3Bucket(bucket)
                        .withS3Key(key)
                        .withData(event.toJsonString())
                        .withStatus(status)
                        .withMessage(message)
                        .build())
                .withPluginName(dlqPushHandler.getDlqPluginSetting().getName())
                .withPipelineName(dlqPushHandler.getDlqPluginSetting().getPipelineName())
                .withPluginId(dlqPushHandler.getDlqPluginSetting().getName())
                .build();
    }

    class RetryRecord {
        private final Record<Event> record;
        private final long createdTime;
        private int retryCount;

        RetryRecord(Record<Event> record) {
            this.record = record;
            this.createdTime = System.currentTimeMillis();
            this.retryCount = 0;
        }

        boolean isExpired() {
            return System.currentTimeMillis() - createdTime > MAX_RETRY_WINDOW_MS;
        }

        void incrementRetryCount() {
            retryCount++;
        }

        Record<Event> getRecord() {
            return record;
        }

        int getRetryCount() {
            return retryCount;
        }
    }
}
