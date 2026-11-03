/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.expression.ExpressionParsingException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.BatchActionExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.MLActionExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.MLBatchJobCreatorFactory;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.ModelSyncInferenceExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.PredictActionExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ServiceName;
import org.opensearch.dataprepper.plugins.ml_inference.processor.dlq.DlqPushHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

@DataPrepperPlugin(name = "ml_inference", pluginType = Processor.class, pluginConfigurationType = MLProcessorConfig.class)
public class MLProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    public static final Logger LOG = LoggerFactory.getLogger(MLProcessor.class);
    public static final String NUMBER_OF_ML_PROCESSOR_SUCCESS = "BatchJobRequestsSucceeded";
    public static final String NUMBER_OF_ML_PROCESSOR_FAILED = "BatchJobRequestsFailed";

    private final String whenCondition;
    private final MLActionExecutor actionExecutor;
    private final Counter numberOfMLProcessorSuccessCounter;
    private final Counter numberOfMLProcessorFailedCounter;
    private final ExpressionEvaluator expressionEvaluator;
    private final PluginSetting pluginSetting;

    private DlqPushHandler dlqPushHandler = null;

    @DataPrepperPluginConstructor
    public MLProcessor(final MLProcessorConfig mlProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final PluginSetting pluginSetting, final AwsCredentialsSupplier awsCredentialsSupplier, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.whenCondition = mlProcessorConfig.getWhenCondition();
        this.numberOfMLProcessorSuccessCounter = pluginMetrics.counter(NUMBER_OF_ML_PROCESSOR_SUCCESS);
        this.numberOfMLProcessorFailedCounter = pluginMetrics.counter(NUMBER_OF_ML_PROCESSOR_FAILED);
        this.expressionEvaluator = expressionEvaluator;
        this.pluginSetting = pluginSetting;

        if (mlProcessorConfig.getDlqPluginSetting() != null) {
            this.dlqPushHandler = new DlqPushHandler(pluginFactory, pluginSetting, mlProcessorConfig.getDlq(), mlProcessorConfig.getAwsAuthenticationOptions());
        }

        if (ActionType.PREDICT.equals(mlProcessorConfig.getActionType())) {
            this.actionExecutor = new PredictActionExecutor(new ModelSyncInferenceExecutor(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics));
        } else {
            final ServiceName serviceName = mlProcessorConfig.getServiceName();
            this.actionExecutor = new BatchActionExecutor(MLBatchJobCreatorFactory.getJobCreator(serviceName, mlProcessorConfig, awsCredentialsSupplier, pluginMetrics, dlqPushHandler));
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final List<Record<Event>> resultRecords = new ArrayList<>();

        actionExecutor.prepareExecution(resultRecords);

        if (records.isEmpty()) {
            return resultRecords;
        }

        final List<Record<Event>> filteredRecords = filterByCondition(records, resultRecords);
        if (filteredRecords.isEmpty()) {
            return resultRecords;
        }

        try {
            actionExecutor.execute(filteredRecords, resultRecords);
            numberOfMLProcessorSuccessCounter.increment();
        } catch (final Exception e) {
            LOG.error(NOISY, "Unexpected error during ML processing: {}", e.getMessage(), e);
            numberOfMLProcessorFailedCounter.increment();
        }

        return resultRecords;
    }

    private List<Record<Event>> filterByCondition(final Collection<Record<Event>> records,
                                                   final List<Record<Event>> resultRecords) {
        return records.stream()
                .filter(record -> {
                    try {
                        final boolean meetCondition = whenCondition == null
                                || expressionEvaluator.evaluateConditional(whenCondition, record.getData());
                        if (!meetCondition) {
                            resultRecords.add(record);
                        }
                        return meetCondition;
                    } catch (ExpressionParsingException e) {
                        LOG.warn("Expression parsing failed for record: {}. Error: {}", record, e.getMessage());
                        resultRecords.add(record);
                        return false;
                    } catch (ClassCastException e) {
                        LOG.warn("Unexpected return type when evaluating condition for record: {}. Error: {}", record, e.getMessage());
                        resultRecords.add(record);
                        return false;
                    } catch (Exception e) {
                        LOG.error("Failed to evaluate conditional expression for record: {}", record, e);
                        resultRecords.add(record);
                        return false;
                    }
                })
                .collect(Collectors.toList());
    }

    @Override
    public void prepareForShutdown() {
        actionExecutor.prepareForShutdown();
    }

    @Override
    public boolean isReadyForShutdown() {
        return actionExecutor.isReadyForShutdown();
    }

    @Override
    public void shutdown() {
        actionExecutor.shutdown();
    }
}
