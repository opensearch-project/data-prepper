/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.ocsf;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.OPERATION_SCHEMA_VALIDATON;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.WORKLOAD;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.COMPANY_NAME;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.SOFTWARE_TYPE;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.CREATION_TIME;

@DataPrepperPlugin(name = "ocsf_transform", pluginType = Processor.class, pluginConfigurationType = OcsfProcessorConfig.class)
public class OcsfProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(OcsfProcessor.class);

    private static final String METRIC_RECORDS_TRANSFORMED = "recordsTransformed";
    private static final String METRIC_RECORDS_FAILED = "recordsFailed";
    private static final String METRIC_PROCESSING_TIME = "processingTime";
    private static final String MAPPING_SCHEMA_PATH_FORMAT = "./data-prepper-plugins/ocsf-processor/src/main/resources/schemas/%s-ocsf-mapping.json";
    private static final Set<String> SUPPORTED_SCHEMA_TYPES = Set.of(
            "office365",
            "crowdstrike"
    );
    private static final Set<String> OCSF_TRANSFORMATION_REQUIRED_SCHEMA_TYPES = Set.of(
            "office365"
    );

    private final OcsfProcessorConfig config;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Timer processingTimer;
    private final String schemaType;
    private final Boolean isOCSFTransformationRequired;
    private final DateTimeFormatter formatter;

    @DataPrepperPluginConstructor
    public OcsfProcessor(final PluginMetrics pluginMetrics,
                         final OcsfProcessorConfig config) {
        super(pluginMetrics);
        this.config = config;
        this.schemaType = config.getSchemaType();
        validateSchemaTypeSupportStatus(schemaType);
        this.isOCSFTransformationRequired = isOcsfTransformationRequired(schemaType);
        this.formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        this.successCounter = pluginMetrics.counter(METRIC_RECORDS_TRANSFORMED);
        this.failureCounter = pluginMetrics.counter(METRIC_RECORDS_FAILED);
        this.processingTimer = pluginMetrics.timer(METRIC_PROCESSING_TIME);
    }

    protected String getMappingSchemaPath(String schemaType) {
        return String.format(MAPPING_SCHEMA_PATH_FORMAT, schemaType);
    }

    /**
     * Check if the given schema type requires OCSF transformation.
     * @param schemaType the schema type to check
     * @return true if the schema type requires OCSF transformation
     */
    private boolean isOcsfTransformationRequired(String schemaType) {
        return OCSF_TRANSFORMATION_REQUIRED_SCHEMA_TYPES.contains(schemaType);
    }

    /**
     * Validate if the schema type is a supported type.
     * @param schemaType the schema type to validate
     * @throws InvalidPluginConfigurationException if the schema type is not supported
     */
    private void validateSchemaTypeSupportStatus(String schemaType) throws InvalidPluginConfigurationException {
        if (!SUPPORTED_SCHEMA_TYPES.contains(schemaType)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Unsupported schema_type: %s. Supported types are: %s",
                            schemaType, SUPPORTED_SCHEMA_TYPES));
        }
    }

    /**
     * Validate if the source data contains required fields based on @schemaType.
     * @param sourceData the map of input source data
     * @throws InvalidPluginConfigurationException if the source data does not contain required fields
     */
    private void validateSchemaContent(Map<String, Object> sourceData) throws InvalidPluginConfigurationException {
        try {
            if (OCSF_TRANSFORMATION_REQUIRED_SCHEMA_TYPES.contains(schemaType)) {
                validateStandardSchemaContent(sourceData);
            } else {
                LOG.info("Schema type {} does not require OCSF transformation.", schemaType);
                validateOcsfSchemaContent(sourceData);
            }
        } catch (InvalidPluginConfigurationException e) {
            LOG.warn("Failed to validate schema: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Validate if the source data contains required fields for OCSF format schemas.
     * @param sourceData the map of input source data
     * @throws InvalidPluginConfigurationException if the source data does not contain required fields
     */
    private void validateOcsfSchemaContent(Map<String, Object> sourceData) throws InvalidPluginConfigurationException {
        if (!sourceData.containsKey(COMPANY_NAME)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", COMPANY_NAME));
        }

        if (!sourceData.containsKey(SOFTWARE_TYPE)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", SOFTWARE_TYPE));
        }
    }

    /**
     * Validate if the source data contains required fields for standard schemas.
     * @param sourceData the map of input source data
     * @throws InvalidPluginConfigurationException if the source data does not contain required fields
     */
    private void validateStandardSchemaContent(Map<String, Object> sourceData) throws InvalidPluginConfigurationException {
        if (!sourceData.containsKey(OPERATION_SCHEMA_VALIDATON)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", OPERATION_SCHEMA_VALIDATON));
        }

        if (!sourceData.containsKey(WORKLOAD)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", WORKLOAD));
        }

        if (!sourceData.containsKey(CREATION_TIME)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", CREATION_TIME));
        } else {
            String creationTime = sourceData.get(CREATION_TIME).toString();
            try {
                formatter.parse(creationTime);
            } catch (DateTimeParseException e) {
                throw new InvalidPluginConfigurationException(
                        String.format("Schema %s section has invalid input.", CREATION_TIME));
            }
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        Timer.Sample sample = Timer.start();
        try {
            for (Record<Event> record : records) {
                Event event = record.getData();
                Map<String, Object> sourceData = event.toMap();
                Map<String, Object> outputData = sourceData;
                try {
                    validateSchemaContent(sourceData);
                    // Skip OCSF transformation for schema types that are already in OCSF format
                    if (isOCSFTransformationRequired) {
                        final String mappingSchemaPath = getMappingSchemaPath(schemaType);
                        final OcsfSchemaMapper schemaMapper = new OcsfSchemaMapper(mappingSchemaPath);
                        outputData = schemaMapper.mapToOcsf(sourceData);
                    }
                    event.clear();
                    outputData.forEach(event::put);
                    successCounter.increment();
                } catch (Exception e) {
                    failureCounter.increment();
                    LOG.error("Failed to transform record to OCSF: {}", e.getMessage());
                    record.getData().getMetadata().addTags(config.getTagsOnFailure());
                }
            }
        } finally {
            sample.stop(processingTimer);
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