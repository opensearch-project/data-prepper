/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml.processor.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml.processor.MLProcessorConfig;

import java.util.Collection;

public abstract class AbstractBatchJobCreator implements MLBatchJobCreator {
    public static final String NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION = "numberOfBatchJobsCreationSucceeded";
    public static final String NUMBER_OF_FAILED_BATCH_JOBS_CREATION = "numberOfBatchJobsCreationFailed";

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected final MLProcessorConfig mlProcessorConfig;
    protected final AwsCredentialsSupplier awsCredentialsSupplier;
    protected final Counter numberOfBatchJobsSuccessCounter;
    protected final Counter numberOfBatchJobsFailedCounter;

    // Constructor
    public AbstractBatchJobCreator(MLProcessorConfig mlProcessorConfig,
                                   AwsCredentialsSupplier awsCredentialsSupplier,
                                   final PluginMetrics pluginMetrics) {
        this.mlProcessorConfig = mlProcessorConfig;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        System.out.println("AbstractBatchJobCreator constructor called");
        this.numberOfBatchJobsSuccessCounter = pluginMetrics.counter(NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION);
        this.numberOfBatchJobsFailedCounter = pluginMetrics.counter(NUMBER_OF_FAILED_BATCH_JOBS_CREATION);
        System.out.println("numberOfBatchJobsSuccessCounter is " + numberOfBatchJobsSuccessCounter.count());
    }

    // Add common logic here that both subclasses can share
    public void incrementSuccessCounter() {
        numberOfBatchJobsSuccessCounter.increment();
    }

    public void incrementFailureCounter() {
        numberOfBatchJobsFailedCounter.increment();
    }

    // Abstract methods for batch job creation, specific to the implementations
    public abstract void createMLBatchJob(Collection<Record<Event>> records);

}
