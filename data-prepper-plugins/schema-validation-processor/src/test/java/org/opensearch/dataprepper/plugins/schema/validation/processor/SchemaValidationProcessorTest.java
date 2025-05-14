package org.opensearch.dataprepper.plugins.schema.validation.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SchemaValidationProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter successCounter;
    @Mock
    private Counter failureCounter;
    @Mock
    private Timer processingTimer;

    private SchemaValidationConfig config_standard;
    private SchemaValidationConfig config_ocsf;

    @BeforeEach
    void setUp() {
        lenient().when(pluginMetrics.counter(anyString())).thenReturn(successCounter);
        lenient().when(pluginMetrics.timer(anyString())).thenReturn(processingTimer);
        when(pluginMetrics.counter("recordsFailed")).thenReturn(failureCounter);

        config_standard = new SchemaValidationConfig() {
            @Override
            public String getSchemaType() {
                return "office365";
            }
        };
        config_ocsf = new SchemaValidationConfig() {
            @Override
            public String getSchemaType() {
                return "crowdstrike";
            }
        };
    }

    @Test
    void testValidateStandardSchema() {
        SchemaValidationProcessor schemaValidationProcessor_standard = new SchemaValidationProcessor(pluginMetrics, config_standard) {
            @Override
            protected String getSchemaPath() {
                return getClass().getClassLoader().getResource("schemas/test-standard-schema.json").getPath();
            }
        };

        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("CreationTime", "creation_time");
        sourceData.put("Id", "id");

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = schemaValidationProcessor_standard.doExecute(
                Collections.singletonList(new Record<>(event)));

        verify(successCounter, times(1)).increment();
        verify(failureCounter, times(0)).increment();
        assertEquals(Boolean.TRUE, processedRecords.iterator().next().getData().get("validation", Boolean.class));
    }

    @Test
    void testValidateInvalidStandardSchema() {
        SchemaValidationProcessor schemaValidationProcessor_standard = new SchemaValidationProcessor(pluginMetrics, config_standard) {
            @Override
            protected String getSchemaPath() {
                return getClass().getClassLoader().getResource("schemas/test-invalid-standard-schema.json").getPath();
            }
        };

        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("CreationTime", "creation_time");
        sourceData.put("Id", "id");

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = schemaValidationProcessor_standard.doExecute(
                Collections.singletonList(new Record<>(event)));

        verify(successCounter, times(0)).increment();
        verify(failureCounter, times(1)).increment();
        assertEquals(Boolean.FALSE, processedRecords.iterator().next().getData().get("validation", Boolean.class));
    }

    @Test
    void testValidateOCSFSchema() {
        SchemaValidationProcessor schemaValidationProcessor_ocsf = new SchemaValidationProcessor(pluginMetrics, config_ocsf){
            @Override
            protected String getSchemaPath() {
                return getClass().getClassLoader().getResource("schemas/test-ocsf-schema.json").getPath();
            }
        };

        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("activity_id", "activity_id");
        sourceData.put("category_uid", "category_uid");

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = schemaValidationProcessor_ocsf.doExecute(
                Collections.singletonList(new Record<>(event)));

        verify(successCounter, times(1)).increment();
        verify(failureCounter, times(0)).increment();
        assertEquals(Boolean.TRUE, processedRecords.iterator().next().getData().get("validation", Boolean.class));
    }

    @Test
    void testValidateInvalidOCSFSchema() {
        SchemaValidationProcessor schemaValidationProcessor_ocsf = new SchemaValidationProcessor(pluginMetrics, config_ocsf){
            @Override
            protected String getSchemaPath() {
                return getClass().getClassLoader().getResource("schemas/test-invalid-ocsf-schema.json").getPath();
            }
        };

        Map<String, Object> sourceData = new HashMap<>();

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = schemaValidationProcessor_ocsf.doExecute(
                Collections.singletonList(new Record<>(event)));

        verify(successCounter, times(0)).increment();
        verify(failureCounter, times(1)).increment();
        assertEquals(Boolean.FALSE, processedRecords.iterator().next().getData().get("validation", Boolean.class));
    }

    @Test
    void testValidateInvalidSchemaType() {
        SchemaValidationConfig config_withInvalidSchemaType = new SchemaValidationConfig() {
            @Override
            public String getSchemaType() {
                return "invalid";
            }
        };
        SchemaValidationProcessor schemaValidationProcessor_withInvalidSchemaType = new SchemaValidationProcessor(pluginMetrics, config_withInvalidSchemaType) {
            @Override
            protected String getSchemaPath() {
                return getClass().getClassLoader().getResource("schemas/test-invalid-schema.json").getPath();
            }
        };
        Map<String, Object> sourceData = new HashMap<>();

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = schemaValidationProcessor_withInvalidSchemaType.doExecute(
                Collections.singletonList(new Record<>(event)));

        verify(successCounter, times(0)).increment();
        verify(failureCounter, times(1)).increment();
        assertEquals(Boolean.FALSE, processedRecords.iterator().next().getData().get("validation", Boolean.class));
    }
}
