/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.geoip.extension.GeoIpConfigExtension;
import org.opensearch.dataprepper.plugins.geoip.extension.GeoIpServiceConfig;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIpConfigSupplier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_FAILED;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_FAILED_ENGINE_EXCEPTION;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_FAILED_IP_NOT_FOUND;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_PROCESSED;
import static org.opensearch.dataprepper.plugins.geoip.processor.GeoIPProcessor.GEO_IP_EVENTS_SUCCEEDED;

@ExtendWith(MockitoExtension.class)
public class GeoIPProcessorIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS))
            .registerModule(new JavaTimeModule());
    private static final String COUNTRY_URL ="https://geoip.maps.opensearch.org/v1/mmdb/geolite2-country/manifest.json";
    private static final String CITY_URL = "https://geoip.maps.opensearch.org/v1/mmdb/geolite2-city/manifest.json";
    private static final String ASN_URL = "https://geoip.maps.opensearch.org/v1/mmdb/geolite2-asn/manifest.json";
    private static GeoIPProcessorConfig geoipProcessorConfig;
    private static GeoIpConfigSupplier defaultGeoIpConfigSupplier;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
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

    @BeforeAll
    public static void downloadDatabases() throws JsonProcessingException {
        String geoipProcessorConfigYaml = "          entries:\n" +
                "            - source: \"peer/ip\"\n" +
                "              target: \"geo\"\n" +
                "              include_fields: [\"country_name\", \"continent_name\", \"location\", \"asn\"]\n" +
                "          geoip_when: '/peer/status == \"success\"'\n" +
                "          tags_on_no_valid_ip: [\"private_ip\", \"invalid_ip\"]";
        String geoipServiceConfigYaml = "        maxmind:\n" +
                "            databases:\n" +
                "              country: " + COUNTRY_URL + "\n" +
                "              city: " + CITY_URL + "\n" +
                "              asn: " + ASN_URL + "\n" +
                "            cache_count: 1024\n" +
                "            database_refresh_interval: P3D";

        geoipProcessorConfig = OBJECT_MAPPER.readValue(geoipProcessorConfigYaml, GeoIPProcessorConfig.class);
        GeoIpServiceConfig geoipServiceConfig = OBJECT_MAPPER.readValue(geoipServiceConfigYaml, GeoIpServiceConfig.class);

        System.setProperty("data-prepper.dir", "build");
        final ExtensionPoints extensionPoints = mock(ExtensionPoints.class);
        final GeoIpConfigExtension geoIpConfigExtension = new GeoIpConfigExtension(geoipServiceConfig);
        geoIpConfigExtension.apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider<GeoIpConfigSupplier>> extensionProviderArgumentCaptor = ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final ExtensionProvider<GeoIpConfigSupplier> extensionProvider = extensionProviderArgumentCaptor.getValue();
        defaultGeoIpConfigSupplier = extensionProvider.provideInstance(null).get();
    }

    @BeforeEach
    public void setUp() {
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_PROCESSED)).thenReturn(geoIpEventsProcessed);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_SUCCEEDED)).thenReturn(geoIpEventsSucceeded);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_FAILED)).thenReturn(geoIpEventsFailed);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_FAILED_ENGINE_EXCEPTION)).thenReturn(geoIpEventsFailedEngineException);
        lenient().when(pluginMetrics.counter(GEO_IP_EVENTS_FAILED_IP_NOT_FOUND)).thenReturn(geoIpEventsFailedIPNotFound);
    }

    public GeoIPProcessor createObjectUnderTest() {
        return new GeoIPProcessor(pluginMetrics, geoipProcessorConfig, defaultGeoIpConfigSupplier,  expressionEvaluator);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(geoIpEventsProcessed);
        verifyNoMoreInteractions(geoIpEventsSucceeded);
        verifyNoMoreInteractions(geoIpEventsFailed);
        verifyNoMoreInteractions(geoIpEventsFailedEngineException);
        verifyNoMoreInteractions(geoIpEventsFailedIPNotFound);
    }


    @Test
    void test_public_ipv4() {
        final Collection<Record<Event>> inputRecords = generateEventData("8.8.8.8");
        for (Record<Event> record: inputRecords) {
            when(expressionEvaluator.evaluateConditional("/peer/status == \"success\"", record.getData())).thenReturn(true);
        }
        final GeoIPProcessor objectUnderTest = createObjectUnderTest();

        final Collection<Record<Event>> records = objectUnderTest.doExecute(inputRecords);
        verifyGeoDataAndMetricsOnSuccess(records);
    }

    @Test
    void test_public_ipv6() {
        final Collection<Record<Event>> inputRecords = generateEventData("2001:4860:4860::8888");
        for (Record<Event> record: inputRecords) {
            when(expressionEvaluator.evaluateConditional("/peer/status == \"success\"", record.getData())).thenReturn(true);
        }
        final GeoIPProcessor objectUnderTest = createObjectUnderTest();

        final Collection<Record<Event>> records = objectUnderTest.doExecute(inputRecords);
        verifyGeoDataAndMetricsOnSuccess(records);
    }

    @Test
    void test_private_ipv4_should_not_add_geodata_but_adds_tags() {
        final List<String> tags = List.of("private_ip", "invalid_ip");
        final Collection<Record<Event>> inputRecords = generateEventData("192.168.255.255");
        for (Record<Event> record: inputRecords) {
            when(expressionEvaluator.evaluateConditional("/peer/status == \"success\"", record.getData())).thenReturn(true);
        }
        final GeoIPProcessor objectUnderTest = createObjectUnderTest();

        final Collection<Record<Event>> records = objectUnderTest.doExecute(inputRecords);
        for (Record<Event> record: records) {
            final Event event = record.getData();
            assertFalse(event.containsKey("geo"));
            assertTrue(event.getMetadata().hasTags(tags));

            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsFailed).increment();
        }
    }

    @Test
    void test_private_ipv6_should_not_add_geodata_but_adds_tags() {
        final List<String> tags = List.of("private_ip", "invalid_ip");
        final Collection<Record<Event>> inputRecords = generateEventData("FD00::BE2:54:34:2/7");
        for (Record<Event> record: inputRecords) {
            when(expressionEvaluator.evaluateConditional("/peer/status == \"success\"", record.getData())).thenReturn(true);
        }
        final GeoIPProcessor objectUnderTest = createObjectUnderTest();

        final Collection<Record<Event>> records = objectUnderTest.doExecute(inputRecords);
        for (Record<Event> record: records) {
            final Event event = record.getData();
            assertFalse(event.containsKey("geo"));
            assertTrue(event.getMetadata().hasTags(tags));

            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsFailed).increment();
        }
    }

    @Test
    void test_geoip_doExecute_should_add_geodata_if_when_condition_is_true() {
        final GeoIPProcessor objectUnderTest = createObjectUnderTest();
        final Collection<Record<Event>> inputRecords = generateEventData("8.8.8.8");
        for (Record<Event> record: inputRecords) {
            when(expressionEvaluator.evaluateConditional("/peer/status == \"success\"", record.getData())).thenReturn(true);
        }

        final Collection<Record<Event>> records = objectUnderTest.doExecute(inputRecords);
        verifyGeoDataAndMetricsOnSuccess(records);
    }

    @Test
    void test_geoip_doExecute_should_not_add_geodata_if_when_condition_is_false() {
        final GeoIPProcessor objectUnderTest = createObjectUnderTest();
        final Collection<Record<Event>> inputRecords = generateEventData("8.8.8.8");
        for (Record<Event> record: inputRecords) {
            when(expressionEvaluator.evaluateConditional("/peer/status == \"success\"", record.getData())).thenReturn(false);
        }

        final Collection<Record<Event>> records = objectUnderTest.doExecute(inputRecords);
        for (Record<Event> record: records) {
            final Event event = record.getData();
            assertFalse(event.containsKey("geo"));
        }
    }

    private void verifyGeoDataAndMetricsOnSuccess(Collection<Record<Event>> records) {
        for (Record<Event> record: records) {
            final Event event = record.getData();
            assertTrue(event.containsKey("geo"));
            assertTrue(event.containsKey("geo/continent_name"));
            assertTrue(event.containsKey("geo/country_name"));
            assertTrue(event.containsKey("geo/location"));
            assertTrue(event.containsKey("geo/asn"));

            verify(geoIpEventsProcessed).increment();
            verify(geoIpEventsSucceeded).increment();
        }
    }

    private Collection<Record<Event>> generateEventData(final String ipAddress) {
        final List<Record<Event>> records = new ArrayList<>();
        final List<String> ips = List.of(ipAddress);
        for (final String ip: ips) {
            Map<String, String> innerMap = new HashMap<>();
            innerMap.put("ip", ip);
            innerMap.put("host", "example.org");
            innerMap.put("status", "success");

            final Map<String, Object> eventMap1 = new HashMap<>();
            eventMap1.put("peer", innerMap);

            final Event firstEvent = JacksonEvent.builder()
                    .withData(eventMap1)
                    .withEventType("event")
                    .build();

            records.add(new Record<>(firstEvent));
        }
        return records;
    }
}
