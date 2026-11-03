/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.AbstractConnector;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.BuiltInConnectors;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.Connector;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.ConnectorActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.ConnectorExecutorFactory;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.RemoteConnectorExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles synchronous PREDICT invocations.
 *
 * <p>For each record:
 * <ol>
 *   <li>Reads model input fields from the event using {@code input_map}.</li>
 *   <li>Invokes the remote model via the built-in connector's PREDICT action.</li>
 *   <li>Extracts model output fields from the response using {@code output_map} and writes
 *       them back into the event.</li>
 * </ol>
 */
public class ModelSyncInferenceExecutor {

    public static final String NUMBER_OF_SYNC_INFERENCE_RECORDS_SUCCESS = "syncInferenceRecordsSucceeded";
    public static final String NUMBER_OF_SYNC_INFERENCE_RECORDS_FAILED = "syncInferenceRecordsFailed";

    private static final Logger LOG = LoggerFactory.getLogger(ModelSyncInferenceExecutor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final MLProcessorConfig config;
    private final RemoteConnectorExecutor connectorExecutor;
    private final List<Map<String, String>> inputMap;
    private final List<Map<String, String>> outputMap;
    private final List<String> tagsOnFailure;
    private final Counter numberOfSyncInferenceRecordsSuccessCounter;
    private final Counter numberOfSyncInferenceRecordsFailedCounter;

    public ModelSyncInferenceExecutor(final MLProcessorConfig config,
                                      final AwsCredentialsSupplier awsCredentialsSupplier,
                                      final PluginMetrics pluginMetrics) {
        this.config = config;
        this.inputMap = config.getInputMap() != null ? config.getInputMap() : Collections.emptyList();
        this.outputMap = config.getOutputMap() != null ? config.getOutputMap() : Collections.emptyList();
        this.tagsOnFailure = config.getTagsOnFailure() != null ? config.getTagsOnFailure() : Collections.emptyList();
        this.connectorExecutor = buildConnectorExecutor(config, awsCredentialsSupplier);
        this.numberOfSyncInferenceRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_SYNC_INFERENCE_RECORDS_SUCCESS);
        this.numberOfSyncInferenceRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_SYNC_INFERENCE_RECORDS_FAILED);
    }

    /**
     * Processes all records synchronously. For each record, every entry in {@code input_map}
     * triggers one PREDICT invocation; the corresponding {@code output_map} entry determines
     * where the result is written back into the event.
     */
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        final List<Record<Event>> resultRecords = new ArrayList<>();
        for (final Record<Event> record : records) {
            try {
                processRecord(record);
                numberOfSyncInferenceRecordsSuccessCounter.increment();
                resultRecords.add(record);
            } catch (final Exception e) {
                LOG.error("Failed to run PREDICT for record: {}", e.getMessage(), e);
                numberOfSyncInferenceRecordsFailedCounter.increment();
                addFailureTags(record);
                resultRecords.add(record);
            }
        }
        return resultRecords;
    }

    private void processRecord(final Record<Event> record) {
        final Event event = record.getData();

        for (int i = 0; i < inputMap.size(); i++) {
            final Map<String, String> inputEntry = inputMap.get(i);
            final Map<String, String> outputEntry = i < outputMap.size() ? outputMap.get(i) : Collections.emptyMap();

            final Map<String, String> runtimeParameters = buildRuntimeParameters(event, inputEntry);
            LOG.debug("Invoking PREDICT with parameters: {}", runtimeParameters.keySet());

            final String responseBody = connectorExecutor.executeActionAndGetResponse(
                    ConnectorActionType.PREDICT, runtimeParameters);

            writeOutputsToEvent(event, responseBody, outputEntry);
        }
    }

    /**
     * Reads each model input field value from the event using the document field specified
     * in the input map entry, and adds the region from config.
     */
    private Map<String, String> buildRuntimeParameters(final Event event,
                                                        final Map<String, String> inputEntry) {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("region", config.getAwsAuthenticationOptions().getAwsRegion().id());

        for (final Map.Entry<String, String> mapping : inputEntry.entrySet()) {
            final String modelInputField = mapping.getKey();
            final String documentField = mapping.getValue();
            final String fieldValue = event.get(documentField, String.class);
            if (fieldValue == null) {
                throw new MLBatchJobException(HttpURLConnection.HTTP_BAD_REQUEST,
                        "input_map field '" + documentField + "' not found in event");
            }
            parameters.put(modelInputField, fieldValue);
        }
        return parameters;
    }

    /**
     * Extracts values from the JSON response body using the model output JSON paths defined
     * in the output map and writes them into the event under the corresponding document field names.
     */
    private void writeOutputsToEvent(final Event event,
                                      final String responseBody,
                                      final Map<String, String> outputEntry) {
        if (outputEntry.isEmpty()) {
            return;
        }
        try {
            final JsonNode responseNode = OBJECT_MAPPER.readTree(responseBody);
            for (final Map.Entry<String, String> mapping : outputEntry.entrySet()) {
                final String documentField = mapping.getKey();
                final String modelOutputPath = mapping.getValue();
                final Object value = extractFromResponse(responseNode, modelOutputPath);
                event.put(documentField, value);
                LOG.debug("Wrote output field '{}' from model path '{}'", documentField, modelOutputPath);
            }
        } catch (final MLBatchJobException e) {
            throw e;
        } catch (final Exception e) {
            throw new MLBatchJobException(HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Failed to parse model response: " + e.getMessage());
        }
    }

    private Object extractFromResponse(final JsonNode responseNode, final String modelOutputPath) {
        try {
            final String pointer = modelOutputPath.startsWith("/") ? modelOutputPath : "/" + modelOutputPath;
            final JsonNode result = responseNode.at(pointer);
            if (result.isMissingNode()) {
                throw new IllegalArgumentException("Path '" + modelOutputPath + "' not found in response");
            }
            return OBJECT_MAPPER.convertValue(result, Object.class);
        } catch (final Exception e) {
            throw new MLBatchJobException(HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "output_map path '" + modelOutputPath + "' not found in model response: " + e.getMessage());
        }
    }

    private void addFailureTags(final Record<Event> record) {
        if (tagsOnFailure.isEmpty()) {
            return;
        }
        final Event event = record.getData();
        if (event.getMetadata() != null) {
            event.getMetadata().addTags(tagsOnFailure);
        }
    }

    private static RemoteConnectorExecutor buildConnectorExecutor(final MLProcessorConfig config,
                                                                    final AwsCredentialsSupplier supplier) {
        return BuiltInConnectors.findConnectorJson(config.getModelId())
                .map(json -> {
                    try {
                        final Connector connector = AbstractConnector.fromJson(json);
                        final RemoteConnectorExecutor executor = ConnectorExecutorFactory.create(connector, config, supplier);
                        LOG.info("ModelSyncInferenceExecutor using built-in connector for model: {}", config.getModelId());
                        return executor;
                    } catch (final Exception e) {
                        throw new RuntimeException(
                                "Failed to initialize connector for model: " + config.getModelId(), e);
                    }
                })
                .orElseThrow(() -> new IllegalArgumentException(
                        "No built-in connector found for model_id '" + config.getModelId()
                                + "'. The predict action_type requires a supported model_id."));
    }
}
