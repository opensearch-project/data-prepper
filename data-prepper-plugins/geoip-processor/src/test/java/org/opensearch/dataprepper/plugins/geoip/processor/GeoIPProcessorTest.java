/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.exception.EngineFailureException;
import org.opensearch.dataprepper.plugins.geoip.exception.EnrichFailedException;
import org.opensearch.dataprepper.plugins.geoip.extension.GeoIPProcessorService;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIpConfigSupplier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.CITY_CONFIDENCE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.CONTINENT_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.CONTINENT_NAME;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.COUNTRY_CONFIDENCE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.IS_COUNTRY_IN_EUROPEAN_UNION;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LATITUDE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LEAST_SPECIFIED_SUBDIVISION_CONFIDENCE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LEAST_SPECIFIED_SUBDIVISION_ISO_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LEAST_SPECIFIED_SUBDIVISION_NAME;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LOCATION;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LOCATION_ACCURACY_RADIUS;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LONGITUDE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.METRO_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.MOST_SPECIFIED_SUBDIVISION_CONFIDENCE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.MOST_SPECIFIED_SUBDIVISION_ISO_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.MOST_SPECIFIED_SUBDIVISION_NAME;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.POSTAL_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.POSTAL_CODE_CONFIDENCE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.REGISTERED_COUNTRY_ISO_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.REGISTERED_COUNTRY_NAME;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.REPRESENTED_COUNTRY_ISO_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.REPRESENTED_COUNTRY_NAME;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.REPRESENTED_COUNTRY_TYPE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.TIME_ZONE;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_FAILED;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_FAILED_ENGINE_EXCEPTION;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_FAILED_IP_NOT_FOUND;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_PROCESSED;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_SUCCEEDED;

@ExtendWith(MockitoExtension.class)
class GeoIPProcessorTest {
    public static final String SOURCE = "/peer/ip";
    public static final String TARGET = "geolocation";
    @Mock
    private GeoIPProcessorService geoIPProcessorService;
    @Mock
    private GeoIPProcessorConfig geoIPProcessorConfig;
    @Mock
    private GeoIpConfigSupplier geoIpConfigSupplier;
    @Mock
    private EntryConfig entry;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter geoIpEventsProcessed;
    @Mock
    private Counter geoIpEventsFailed;
    @Mock
    private Counter geoIpEventsSucceeded;
    @Mock
    private Counter geoIpEventsFailedEngineException;
    @Mock
    private Counter geoIpEventsFailedIPNotFound;
    @Mock
    private GeoIPDatabaseReader geoIPDatabaseReader;
    @Captor
    private ArgumentCaptor<List<GeoIPField>> geoIPFieldCaptor;

    @BeforeEach
    void setUp() {
        when(geoIpConfigSupplier.getGeoIPProcessorService()).thenReturn(Optional.of(geoIPProcessorService));
        lenient().when(geoIPProcessorService.getGeoIPDatabaseReader()).thenReturn(geoIPDatabaseReader);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_PROCESSED)).thenReturn(geoIpEventsProcessed);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_SUCCEEDED)).thenReturn(geoIpEventsSucceeded);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_FAILED)).thenReturn(geoIpEventsFailed);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_FAILED_ENGINE_EXCEPTION)).thenReturn(geoIpEventsFailedEngineException);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_FAILED_IP_NOT_FOUND)).thenReturn(geoIpEventsFailedIPNotFound);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(geoIpEventsProcessed);
        verifyNoMoreInteractions(geoIpEventsSucceeded);
        verifyNoMoreInteractions(geoIpEventsFailed);
        verifyNoMoreInteractions(geoIpEventsFailedEngineException);
        verifyNoMoreInteractions(geoIpEventsFailedIPNotFound);
    }

    private GeoIPProcessor createObjectUnderTest() {
        return new GeoIPProcessor(pluginMetrics, geoIPProcessorConfig, geoIpConfigSupplier, expressionEvaluator);
    }

    @Test
    void doExecuteTest_with_when_condition_should_enrich_events_that_match_when_condition() {
        final String whenCondition = "/peer/status == success";

        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
        when(geoIPProcessorConfig.getWhenCondition()).thenReturn(whenCondition);
        when(entry.getSource()).thenReturn("/peer/ip");
        when(entry.getTarget()).thenReturn(TARGET);
        when(entry.getIncludeFields()).thenReturn(setFields());

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        when(geoIPDatabaseReader.getGeoData(any(), any(), any())).thenReturn(prepareGeoData());

        final Record<Event> record1 = createCustomRecord("success");
        List<Record<Event>> recordsIn = List.of(record1);

        when(expressionEvaluator.evaluateConditional(whenCondition, record1.getData())).thenReturn(true);

        final Collection<Record<Event>> records = geoIPProcessor.doExecute(recordsIn);

        assertThat(records.size(), equalTo(1));

        final Collection<Record<Event>> recordsWithLocation = records.stream().filter(record -> record.getData().containsKey(TARGET))
                .collect(Collectors.toList());

        assertThat(recordsWithLocation.size(), equalTo(1));
        verify(geoIpEventsSucceeded).increment();
        verify(geoIpEventsProcessed).increment();
    }

    @Test
    void doExecuteTest_with_when_condition_should_not_enrich_if_when_condition_is_false() {
        final String whenCondition = "/peer/status == success";

        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
        when(geoIPProcessorConfig.getWhenCondition()).thenReturn(whenCondition);

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        final Record<Event> record1 = createCustomRecord("success");
        List<Record<Event>> recordsIn = new ArrayList<>();
        recordsIn.add(record1);

        when(expressionEvaluator.evaluateConditional(whenCondition, record1.getData())).thenReturn(false);

        final Collection<Record<Event>> records = geoIPProcessor.doExecute(recordsIn);

        assertThat(records.size(), equalTo(1));

        final Collection<Record<Event>> recordsWithLocation = records.stream().filter(record -> record.getData().containsKey(TARGET))
                .collect(Collectors.toList());

        assertThat(recordsWithLocation.size(), equalTo(0));
    }

    @Test
    void doExecuteTest_should_add_geo_data_to_event_if_source_is_non_null() {
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
        when(entry.getSource()).thenReturn(SOURCE);
        when(entry.getTarget()).thenReturn(TARGET);
        when(entry.getIncludeFields()).thenReturn(setFields());

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        when(geoIPDatabaseReader.getGeoData(any(), any(), any())).thenReturn(prepareGeoData());
        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
        for (final Record<Event> record : records) {
            final Event event = record.getData();
            assertThat(event.get("/peer/ip", String.class), equalTo("136.226.242.205"));
            assertThat(event.containsKey(TARGET), equalTo(true));
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsSucceeded).increment();
        }
    }

    @Test
    void doExecuteTest_should_add_geo_data_with_expected_fields_to_event_when_include_fields_is_configured() {
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
        when(entry.getSource()).thenReturn(SOURCE);
        when(entry.getTarget()).thenReturn(TARGET);

        final List<String> includeFields = List.of("city_name", "asn");
        final List<GeoIPField> includeFieldsResult = List.of(GeoIPField.CITY_NAME, GeoIPField.ASN);
        when(entry.getIncludeFields()).thenReturn(includeFields);

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        when(geoIPDatabaseReader.getGeoData(any(), any(), any())).thenReturn(prepareGeoData());
        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
        verify(geoIPDatabaseReader).getGeoData(any(), geoIPFieldCaptor.capture(), any());

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            assertThat(event.get("/peer/ip", String.class), equalTo("136.226.242.205"));
            assertThat(event.containsKey(TARGET), equalTo(true));
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsSucceeded).increment();
        }

        final List<GeoIPField> value = geoIPFieldCaptor.getValue();
        assertThat(value, containsInAnyOrder(includeFieldsResult.toArray()));
    }

    @Test
    void doExecuteTest_should_add_geo_data_with_expected_fields_to_event_when_exclude_fields_is_configured() {
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
        when(entry.getSource()).thenReturn(SOURCE);
        when(entry.getTarget()).thenReturn(TARGET);

        final List<String> excludeFields = List.of("country_name", "country_iso_code", "city_name", "asn", "asn_organization", "network", "ip");
        final List<GeoIPField> excludeFieldsResult = List.of(CONTINENT_NAME, CONTINENT_CODE, IS_COUNTRY_IN_EUROPEAN_UNION,
                REPRESENTED_COUNTRY_NAME, REPRESENTED_COUNTRY_ISO_CODE, REPRESENTED_COUNTRY_TYPE, REGISTERED_COUNTRY_NAME,
                REGISTERED_COUNTRY_ISO_CODE, LOCATION, LOCATION_ACCURACY_RADIUS, LATITUDE, LONGITUDE, METRO_CODE, TIME_ZONE, POSTAL_CODE,
                MOST_SPECIFIED_SUBDIVISION_NAME, MOST_SPECIFIED_SUBDIVISION_ISO_CODE, LEAST_SPECIFIED_SUBDIVISION_NAME,
                LEAST_SPECIFIED_SUBDIVISION_ISO_CODE, COUNTRY_CONFIDENCE, CITY_CONFIDENCE, MOST_SPECIFIED_SUBDIVISION_CONFIDENCE,
                LEAST_SPECIFIED_SUBDIVISION_CONFIDENCE, POSTAL_CODE_CONFIDENCE);
        when(entry.getExcludeFields()).thenReturn(excludeFields);

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        when(geoIPDatabaseReader.getGeoData(any(), any(), any())).thenReturn(prepareGeoData());
        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
        verify(geoIPDatabaseReader).getGeoData(any(), geoIPFieldCaptor.capture(), any());

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            assertThat(event.get("/peer/ip", String.class), equalTo("136.226.242.205"));
            assertThat(event.containsKey(TARGET), equalTo(true));
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsSucceeded).increment();
        }

        final List<GeoIPField> value = geoIPFieldCaptor.getValue();
        assertThat(value, containsInAnyOrder(excludeFieldsResult.toArray()));
    }

    @Test
    void doExecuteTest_should_not_add_geo_data_to_event_if_source_is_null() {
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
        when(entry.getSource()).thenReturn("ip");
        when(entry.getIncludeFields()).thenReturn(setFields());
        List<String> testTags = List.of("tag1", "tag2");
        when(geoIPProcessorConfig.getTagsOnIPNotFound()).thenReturn(testTags);

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            assertThat(!event.containsKey("geo"), equalTo(true));
            assertTrue(event.getMetadata().hasTags(testTags));
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsFailed).increment();
            verify(geoIpEventsFailedIPNotFound).increment();
        }
    }

    @Test
    void doExecuteTest_should_not_add_geo_data_to_event_if_returned_data_is_empty() {
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
        when(entry.getSource()).thenReturn(SOURCE);
        when(entry.getIncludeFields()).thenReturn(setFields());

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        when(geoIPDatabaseReader.getGeoData(any(), any(), any())).thenReturn(Collections.EMPTY_MAP);
        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
        for (final Record<Event> record : records) {
            final Event event = record.getData();
            assertThat(!event.containsKey("geo"), equalTo(true));
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsFailed).increment();
            verify(geoIpEventsFailedIPNotFound).increment();
        }
    }

    @Test
    void doExecuteTest_should_not_add_geodata_if_database_is_expired() {
        when(geoIPDatabaseReader.isExpired()).thenReturn(true);

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        final Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
        for (final Record<Event> record : records) {
            final Event event = record.getData();
            assertThat(!event.containsKey("geo"), equalTo(true));
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsFailed).increment();
        }
    }

    @Test
    void doExecuteTest_should_not_add_geodata_if_database_reader_is_null() {
        when(geoIPProcessorService.getGeoIPDatabaseReader()).thenReturn(null);

        final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        final Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
        for (final Record<Event> record : records) {
            final Event event = record.getData();
            assertThat(!event.containsKey("geo"), equalTo(true));
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsFailed).increment();
        }
    }

    @Test
    void doExecuteTest_should_not_add_geodata_if_ip_address_is_not_public() {
        try (final MockedStatic<GeoInetAddress> ipValidationCheckMockedStatic = mockStatic(GeoInetAddress.class)) {
            ipValidationCheckMockedStatic.when(() -> GeoInetAddress.usableInetFromString(any())).thenReturn(Optional.empty());

            when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));
            when(entry.getSource()).thenReturn(SOURCE);
            when(entry.getIncludeFields()).thenReturn(setFields());

            final GeoIPProcessor geoIPProcessor = createObjectUnderTest();

            final Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
            for (final Record<Event> record : records) {
                final Event event = record.getData();
                assertThat(!event.containsKey("geo"), equalTo(true));
                verify(geoIpEventsProcessed).increment();
                verify(geoIpEventsFailed).increment();
                verify(geoIpEventsFailedIPNotFound).increment();
            }
        }
    }

    @Test
    void test_ip_not_found_tags_when_EnrichFailedException_is_thrown() {
        when(entry.getSource()).thenReturn(SOURCE);
        when(entry.getIncludeFields()).thenReturn(setFields());

        List<String> testTags = List.of("tag1", "tag2");
        when(geoIPProcessorConfig.getTagsOnIPNotFound()).thenReturn(testTags);
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));

        GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        doThrow(EnrichFailedException.class).when(geoIPDatabaseReader).getGeoData(any(), any(), any());

        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());

        for (final Record<Event> record : records) {
            Event event = record.getData();
            assertTrue(event.getMetadata().hasTags(testTags));
            verify(geoIpEventsFailed).increment();
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsFailedIPNotFound).increment();
        }
    }

    @Test
    void test_ip_not_found_tags_when_EngineFailureException_is_thrown() {
        when(entry.getSource()).thenReturn(SOURCE);
        when(entry.getIncludeFields()).thenReturn(setFields());

        List<String> testTags = List.of("tag1", "tag2");
        when(geoIPProcessorConfig.getTagsOnEngineFailure()).thenReturn(testTags);
        when(geoIPProcessorConfig.getEntries()).thenReturn(List.of(entry));

        GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        doThrow(EngineFailureException.class).when(geoIPDatabaseReader).getGeoData(any(), any(), any());

        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());

        for (final Record<Event> record : records) {
            Event event = record.getData();
            assertTrue(event.getMetadata().hasTags(testTags));
            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsFailed).increment();
            verify(geoIpEventsFailedEngineException).increment();
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
        innerMap.put("ip", "136.226.242.205");
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
