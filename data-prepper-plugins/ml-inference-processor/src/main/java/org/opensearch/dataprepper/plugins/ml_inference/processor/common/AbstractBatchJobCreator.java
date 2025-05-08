/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.util.MlCommonRequester;
import software.amazon.awssdk.auth.signer.Aws4Signer;

import java.util.List;

import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.LOG;

public abstract class AbstractBatchJobCreator implements MLBatchJobCreator {
    public static final String NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION = "batchJobsCreationSucceeded";
    public static final String NUMBER_OF_FAILED_BATCH_JOBS_CREATION = "batchJobsCreationFailed";

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected final MLProcessorConfig mlProcessorConfig;
    protected final AwsCredentialsSupplier awsCredentialsSupplier;
    protected final Counter numberOfBatchJobsSuccessCounter;
    protected final Counter numberOfBatchJobsFailedCounter;
    protected final List<String> tagsOnFailure;
    protected final MlCommonRequester mlCommonRequester;

    private static final Aws4Signer signer;
    static {
        signer = Aws4Signer.create();
    }
    // Constructor
    public AbstractBatchJobCreator(MLProcessorConfig mlProcessorConfig,
                                   AwsCredentialsSupplier awsCredentialsSupplier,
                                   final PluginMetrics pluginMetrics) {
        this.mlProcessorConfig = mlProcessorConfig;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.numberOfBatchJobsSuccessCounter = pluginMetrics.counter(NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION);
        this.numberOfBatchJobsFailedCounter = pluginMetrics.counter(NUMBER_OF_FAILED_BATCH_JOBS_CREATION);
        this.tagsOnFailure = mlProcessorConfig.getTagsOnFailure();
        this.mlCommonRequester = new MlCommonRequester(signer, mlProcessorConfig, awsCredentialsSupplier);
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
}
