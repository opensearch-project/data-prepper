/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.configuration.EntryConfig;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.EnrichFailedException;
import org.opensearch.dataprepper.plugins.processor.extension.GeoIPProcessorService;
import org.opensearch.dataprepper.plugins.processor.extension.GeoIpConfigSupplier;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.GeoIPProcessor.GEO_IP_EVENTS_PROCESSED;
import static org.opensearch.dataprepper.plugins.processor.GeoIPProcessor.GEO_IP_EVENTS_FAILED_DB_LOOKUP;

@ExtendWith(MockitoExtension.class)
class GeoIPProcessorTest {
    public static final String SOURCE = "/peer/ip";
    public static final String TARGET1 = "geolocation";
    public static final String TARGET2 = "geodata";
    @Mock
    private GeoIPProcessorService geoIPProcessorService;
    @Mock
    private GeoIPProcessorConfig geoIPProcessorConfig;
    @Mock
    private GeoIpConfigSupplier geoIpConfigSupplier;
    @Mock
    private EntryConfig entry;
    @Mock
    private EntryConfig anotherEntry;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter geoIpEventsProcessed;
    @Mock
    private Counter geoIpEventsFailedDBLookup;

    @BeforeEach
    void setUp() {
        when(geoIpConfigSupplier.getGeoIPProcessorService()).thenReturn(geoIPProcessorService);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_PROCESSED)).thenReturn(geoIpEventsProcessed);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_FAILED_DB_LOOKUP)).thenReturn(geoIpEventsFailedDBLookup);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(geoIpEventsProcessed);
        verifyNoMoreInteractions(geoIpEventsFailedDBLookup);
    }

    private GeoIPProcessor createObjectUnderTest() {
        return new GeoIPProcessor(pluginMetrics, geoIPProcessorConfig, geoIpConfigSupplier, expressionEvaluator);
    }

    @Test
    void doExecuteTest_with_when_condition_should_only_enrich_events_that_match_when_condition() throws NoSuchFieldException, IllegalAccessException {
        final String whenCondition = "/peer/status == success";

        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry, anotherEntry));
        when(entry.getWhenCondition()).thenReturn(whenCondition);
        when(entry.getSource()).thenReturn("/peer/source_ip");
        when(entry.getTarget()).thenReturn(TARGET1);
        when(entry.getFields()).thenReturn(setFields());

        when(anotherEntry.getSource()).thenReturn("/peer/target_ip");
        when(anotherEntry.getTarget()).thenReturn(TARGET2);
        when(anotherEntry.getFields()).thenReturn(setFields());

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        when(geoIPProcessorService.getGeoData(any(), any())).thenReturn(prepareGeoData());

        ReflectivelySetField.setField(GeoIPProcessor.class, geoIPProcessor, "geoIPProcessorService", geoIPProcessorService);

        final Record<Event> record1 = createCustomRecord("success");
        final Record<Event> record2 = createCustomRecord("failed");
        List<Record<Event>> recordsIn = new ArrayList<>();
        recordsIn.add(record1);
        recordsIn.add(record2);

        when(expressionEvaluator.evaluateConditional(whenCondition, record1.getData())).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(whenCondition, record2.getData())).thenReturn(false);

        final Collection<Record<Event>> records = geoIPProcessor.doExecute(recordsIn);

        assertThat(records.size(), equalTo(2));

        final Collection<Record<Event>> recordsWithLocation = records.stream().filter(record -> record.getData().containsKey(TARGET1))
                .collect(Collectors.toList());

        final Collection<Record<Event>> recordsWithSecondEntry = records.stream().filter(record -> record.getData().containsKey(TARGET2))
                .collect(Collectors.toList());

        assertThat(recordsWithLocation.size(), equalTo(1));
        assertThat(recordsWithSecondEntry.size(), equalTo(2));

        for (final Record<Event> record : recordsWithLocation) {
            final Event event = record.getData();
            assertThat(event.get("/peer/status", String.class), equalTo("success"));
        }
        verify(geoIpEventsProcessed, times(3)).increment();
    }

    @Test
    void doExecuteTest() throws NoSuchFieldException, IllegalAccessException {
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
        when(entry.getSource()).thenReturn(SOURCE);
        when(entry.getTarget()).thenReturn(TARGET1);
        when(entry.getFields()).thenReturn(setFields());

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        when(geoIPProcessorService.getGeoData(any(), any())).thenReturn(prepareGeoData());
        ReflectivelySetField.setField(GeoIPProcessor.class, geoIPProcessor,
                "geoIPProcessorService", geoIPProcessorService);
        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
        for (final Record<Event> record : records) {
            final Event event = record.getData();
            assertThat(event.get("/peer/ip", String.class), equalTo("136.226.242.205"));
            assertThat(event.containsKey("geolocation"), equalTo(true));
            verify(geoIpEventsProcessed).increment();
        }
    }

    @Test
    void test_tags_when_enrich_fails() {
        when(entry.getSource()).thenReturn(SOURCE);
        when(entry.getFields()).thenReturn(setFields());

        List<String> testTags = List.of("tag1", "tag2");
        when(geoIPProcessorConfig.getTagsOnFailure()).thenReturn(testTags);
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));

        when(geoIpConfigSupplier.getGeoIPProcessorService()).thenReturn(geoIPProcessorService);

        GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        doThrow(EnrichFailedException.class).when(geoIPProcessorService).getGeoData(any(), any());

        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());

        for (final Record<Event> record : records) {
            Event event = record.getData();
            assertTrue(event.getMetadata().hasTags(testTags));
            verify(geoIpEventsFailedDBLookup).increment();
        }
    }

    @Test
    void isReadyForShutdownTest() {
        GeoIPProcessor geoIPProcessor = createObjectUnderTest();
        assertTrue(geoIPProcessor.isReadyForShutdown());
    }

    @Test
    void shutdownTest() {
        GeoIPProcessor geoIPProcessor = createObjectUnderTest();
        assertDoesNotThrow(geoIPProcessor::shutdown);
    }

    private Map<String, Object> prepareGeoData() {
        Map<String, Object> geoDataMap = new HashMap<>();
        geoDataMap.put("country_IsoCode", "US");
        geoDataMap.put("continent_name", "North America");
        geoDataMap.put("timezone", "America/Chicago");
        geoDataMap.put("country_name", "United States");
        return geoDataMap;
    }

    private List<String> setFields() {
        final List<String> attributes = new ArrayList<>();
        attributes.add("city_name");
        attributes.add("country_name");
        return attributes;
    }


    private Collection<Record<Event>> setEventQueue() {
        final Collection<Record<Event>> jsonObjects = new LinkedList<>();
        jsonObjects.add(createRecord());
        return jsonObjects;
    }

    private static Record<Event> createRecord() {
        String json = "{\"peer\": {\"ip\": \"136.226.242.205\", \"host\": \"example.org\" }, \"status\": \"success\"}";
        final JacksonEvent event = JacksonLog.builder().withData(json).build();
        return new Record<>(event);
    }

    private Record<Event> createCustomRecord(final String customFieldValue) {
        Map<String, String> innerMap = new HashMap<>();
        innerMap.put("source_ip", "136.226.242.205");
        innerMap.put("target_ip", "136.226.242.201");
        innerMap.put("host", "example.org");
        innerMap.put("status", customFieldValue);

        final Map<String, Object> eventMap1 = new HashMap<>();
        eventMap1.put("peer", innerMap);

        final Event firstEvent = JacksonEvent.builder()
                .withData(eventMap1)
                .withEventType("event")
                .build();

        return new Record<>(firstEvent);
    }
}
