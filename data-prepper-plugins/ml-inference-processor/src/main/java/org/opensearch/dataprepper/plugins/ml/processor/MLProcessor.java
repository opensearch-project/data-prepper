/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml.processor;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml.processor.common.MLBatchJobCreator;
import org.opensearch.dataprepper.plugins.ml.processor.common.MLBatchJobCreatorFactory;
import org.opensearch.dataprepper.plugins.ml.processor.configuration.ServiceName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

@Experimental
@DataPrepperPlugin(name = "ml_inference", pluginType = Processor.class, pluginConfigurationType = MLProcessorConfig.class)
public class MLProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    public static final Logger LOG = LoggerFactory.getLogger(MLProcessor.class);
    public static final String NUMBER_OF_ML_PROCESSOR_SUCCESS = "mlProcessorSuccessfullyCreated";
    public static final String NUMBER_OF_ML_PROCESSOR_FAILED = "mlProcessorFailedToCreated";

    private final String whenCondition;
    private MLBatchJobCreator mlBatchJobCreator;
    private final Counter numberOfMLProcessorSuccessCounter;
    private final Counter numberOfMLProcessorFailedCounter;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public MLProcessor(final MLProcessorConfig mlProcessorConfig, final PluginMetrics pluginMetrics, final AwsCredentialsSupplier awsCredentialsSupplier, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.whenCondition = mlProcessorConfig.getWhenCondition();
        ServiceName serviceName = mlProcessorConfig.getServiceName();
        this.numberOfMLProcessorSuccessCounter = pluginMetrics.counter(
                NUMBER_OF_ML_PROCESSOR_SUCCESS);
        this.numberOfMLProcessorFailedCounter = pluginMetrics.counter(
                NUMBER_OF_ML_PROCESSOR_FAILED);
        this.expressionEvaluator = expressionEvaluator;

        // Use factory to get the appropriate job creator
        mlBatchJobCreator = MLBatchJobCreatorFactory.getJobCreator(serviceName, mlProcessorConfig, awsCredentialsSupplier, pluginMetrics);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        // reads from input - S3 input
        if (records.size() == 0)
            return records;

        List<Record<Event>> recordsToMlCommons = records.stream()
            .filter(record -> {
                try {
                    return whenCondition == null || expressionEvaluator.evaluateConditional(whenCondition, record.getData());
                } catch (Exception e) {
                    LOG.error("Failed to evaluate conditional expression for record: {}", record, e);
                    return false; // Skip the record if evaluation fails
                }
            })
            .collect(Collectors.toList());

        if (recordsToMlCommons.isEmpty()) {
            return records;
        }

        try {
            mlBatchJobCreator.createMLBatchJob(recordsToMlCommons);
            numberOfMLProcessorSuccessCounter.increment();
        } catch (Exception e) {
            LOG.error(NOISY, e.getMessage(), e);
            numberOfMLProcessorFailedCounter.increment();
        }
        return records;
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}
