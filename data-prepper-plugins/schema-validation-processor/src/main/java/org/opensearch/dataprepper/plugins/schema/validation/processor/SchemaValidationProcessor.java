package org.opensearch.dataprepper.plugins.schema.validation.processor;

import org.json.JSONObject;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.opensearch.dataprepper.plugins.schema.validation.processor.utils.Constants.ACTIVITY_ID;
import static org.opensearch.dataprepper.plugins.schema.validation.processor.utils.Constants.CATEGORY_UID;
import static org.opensearch.dataprepper.plugins.schema.validation.processor.utils.Constants.CREATION_TIME;
import static org.opensearch.dataprepper.plugins.schema.validation.processor.utils.Constants.LOG_EVENT_ID;

@DataPrepperPlugin(name = "schema_validation", pluginType = Processor.class, pluginConfigurationType = SchemaValidationConfig.class)
public class SchemaValidationProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidationProcessor.class);

    private static final String METRIC_RECORDS_VALIDATED = "recordsValidated";
    private static final String METRIC_RECORDS_FAILED = "recordsFailed";
    private static final String METRIC_PROCESSING_TIME = "processingTime";
    private static final String SCHEMA_PATH_FORMAT = "./data-prepper-plugins/schema-validation-processor/src/main/resources/schemas/%s-log.json";
    private static final Set<String> OCSF_SCHEMA_TYPES = Set.of(
            "crowdstrike"
    );
    private static final Set<String> STANDARD_SCHEMA_TYPES = Set.of(
            "office365"
    );

    private final Counter successCounter;
    private final Counter failureCounter;
    private final Timer processingTimer;
    private final String schemaType;
    private final JSONObject schema;

    protected String getSchemaPath() {
        return String.format(SCHEMA_PATH_FORMAT, schemaType);
    }

    private JSONObject getSchema() throws InvalidPluginConfigurationException {
        String schemaPath = getSchemaPath();
        try {
            String schemaContent = new String(Files.readAllBytes(Paths.get(schemaPath)));
            return new JSONObject(schemaContent);
        } catch (Exception e) {
            throw new InvalidPluginConfigurationException(
                    String.format("Failed to read schema file: %s", schemaPath));
        }
    }

    private void validateSchemaType(Map<String, Object> sourceData) throws InvalidPluginConfigurationException {
        boolean validationResult = false;

        try {
            if (!OCSF_SCHEMA_TYPES.contains(schemaType) && !STANDARD_SCHEMA_TYPES.contains(schemaType)) {
                throw new InvalidPluginConfigurationException(
                        String.format("Unsupported schema_type: %s. Supported OCSF schemas types: %s, supported standard schema types: %s",
                                schemaType, OCSF_SCHEMA_TYPES, STANDARD_SCHEMA_TYPES));
            }

            if (OCSF_SCHEMA_TYPES.contains(schemaType)) {
                validateOcsfSchema();
            } else {
                validateStandardSchema();
            }
            validationResult = true;
        } catch (InvalidPluginConfigurationException e) {
            LOG.warn("Failed to validate schema: {}", e.getMessage());
            throw e;
        } finally {
            sourceData.putIfAbsent("validation", validationResult);
        }
    }

    private void validateOcsfSchema() throws InvalidPluginConfigurationException {
        if (!schema.has(ACTIVITY_ID)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", ACTIVITY_ID));
        }

        if (!schema.has(CATEGORY_UID)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", CATEGORY_UID));
        }
    }

    private void validateStandardSchema() throws InvalidPluginConfigurationException {
        if (!schema.has(CREATION_TIME)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", CREATION_TIME));
        }

        if (!schema.has(LOG_EVENT_ID)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain %s section.", LOG_EVENT_ID));
        }
    }

    @DataPrepperPluginConstructor
    public SchemaValidationProcessor(final PluginMetrics pluginMetrics,
                                     final SchemaValidationConfig config) {
        super(pluginMetrics);
        this.schemaType = config.getSchemaType();
        this.schema = getSchema();
        this.successCounter = pluginMetrics.counter(METRIC_RECORDS_VALIDATED);
        this.failureCounter = pluginMetrics.counter(METRIC_RECORDS_FAILED);
        this.processingTimer = pluginMetrics.timer(METRIC_PROCESSING_TIME);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        Timer.Sample sample = Timer.start();
        try {
            for (Record<Event> record : records) {
                Event event = record.getData();
                Map<String, Object> sourceData = event.toMap();
                try {
                    validateSchemaType(sourceData);
                    successCounter.increment();
                } catch (Exception e) {
                    failureCounter.increment();
                    LOG.error("Failed to validate schema: {}", e.getMessage());
                } finally {
                    event.clear();
                    sourceData.forEach(event::put);
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
