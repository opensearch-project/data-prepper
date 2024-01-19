/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;
import org.opensearch.dataprepper.plugins.processor.configuration.KeyConfig;
import org.opensearch.dataprepper.plugins.processor.configuration.KeysConfig;
import org.opensearch.dataprepper.plugins.processor.configuration.MaxMindServiceConfig;
import org.opensearch.dataprepper.plugins.processor.configuration.ServiceTypeOptions;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.EnrichFailedException;
import org.opensearch.dataprepper.plugins.processor.extension.GeoIpConfigSupplier;
import org.opensearch.dataprepper.plugins.processor.loadtype.LoadTypeOptions;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.net.MalformedURLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeoIPProcessorTest {

    public static final int REFRESH_SCHEDULE = 10;
    public static final String SOURCE = "/peer/ip";
    public static final String TARGET = "location";
    public static final String PROCESSOR_PLUGIN_NAME = "geoip";
    public static final String PROCESSOR_PIPELINE_NAME = "geoIP-processor-pipeline";
    @Mock
    private GeoIPProcessorService geoIPProcessorService;
    @Mock
    private GeoIPProcessorConfig geoCodingProcessorConfig;
    @Mock
    private GeoIpConfigSupplier geoIpConfigSupplier;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private ServiceTypeOptions serviceTypeOptions;
    @Mock
    private MaxMindServiceConfig maxMindServiceConfig;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {

        when(pluginSetting.getName()).thenReturn(PROCESSOR_PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PROCESSOR_PIPELINE_NAME);
        when(geoCodingProcessorConfig.getServiceType()).thenReturn(serviceTypeOptions);
        when(geoCodingProcessorConfig.getServiceType().getMaxMindService()).thenReturn(maxMindServiceConfig);
        when(geoCodingProcessorConfig.getServiceType().getMaxMindService().getLoadType())
                .thenReturn(LoadTypeOptions.INMEMORY);
        when(geoCodingProcessorConfig.getServiceType().getMaxMindService().getCacheRefreshSchedule())
                .thenReturn(Duration.ofSeconds(REFRESH_SCHEDULE));

        DatabasePathURLConfig databasePathURLConfig1 = new DatabasePathURLConfig();
        ReflectivelySetField.setField(DatabasePathURLConfig.class,
                databasePathURLConfig1, "url", "./src/test/resources/mmdb-file/geo-lite2");
        List<DatabasePathURLConfig> urlList = List.of(databasePathURLConfig1);
        when(geoCodingProcessorConfig.getServiceType().getMaxMindService().getDatabasePath()).thenReturn(urlList);
    }

    @Test
    void doExecuteTest() throws MalformedURLException, NoSuchFieldException, IllegalAccessException {
        KeyConfig keyConfig = new KeyConfig();
        final List<String> attributes = setAttributes();
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "attributes", attributes);
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "source", SOURCE);
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "target", TARGET);

        KeysConfig keysConfig1 = new KeysConfig();
        ReflectivelySetField.setField(KeysConfig.class,
                keysConfig1, "keyConfig", keyConfig);

        List<KeysConfig> configs = new ArrayList<>();
        configs.add(keysConfig1);
        when(geoCodingProcessorConfig.getKeysConfig()).thenReturn(configs);
        GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        when(geoIPProcessorService.getGeoData(any(), any())).thenReturn(prepareGeoData());
        ReflectivelySetField.setField(GeoIPProcessor.class, geoIPProcessor,
                "geoIPProcessorService", geoIPProcessorService);
        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());
        for (final Record<Event> record : records) {
            Event event = record.getData();
            assertThat(event.get("/peer/ip", String.class), equalTo("136.226.242.205"));
        }
    }


    @Test
    void test_tags_when_enrich_fails() throws MalformedURLException, NoSuchFieldException, IllegalAccessException {

        KeyConfig keyConfig = new KeyConfig();
        final List<String> attributes = setAttributes();
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "attributes", attributes);
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "source", SOURCE);
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "target", TARGET);

        List<String> testTags = List.of("tag1", "tag2");
        when(geoCodingProcessorConfig.getTagsOnSourceNotFoundFailure()).thenReturn(testTags);

        KeysConfig keysConfig1 = new KeysConfig();
        ReflectivelySetField.setField(KeysConfig.class,
                keysConfig1, "keyConfig", keyConfig);

        List<KeysConfig> configs = new ArrayList<>();
        configs.add(keysConfig1);
        when(geoCodingProcessorConfig.getKeysConfig()).thenReturn(configs);
        GeoIPProcessor geoIPProcessor = createObjectUnderTest();

        doThrow(EnrichFailedException.class).when(geoIPProcessorService).getGeoData(any(), any());

        ReflectivelySetField.setField(GeoIPProcessor.class, geoIPProcessor,
                "geoIPProcessorService", geoIPProcessorService);
        Collection<Record<Event>> records = geoIPProcessor.doExecute(setEventQueue());

        for (final Record<Event> record : records) {
            Event event = record.getData();
            assertTrue(event.getMetadata().hasTags(testTags));
        }
    }

    @Test
    void isReadyForShutdownTest() throws MalformedURLException {
        GeoIPProcessor geoIPProcessor = createObjectUnderTest();
        Assertions.assertFalse(geoIPProcessor.isReadyForShutdown());
    }

    @Test
    void shutdownTest() throws MalformedURLException {
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

    private GeoIPProcessor createObjectUnderTest() throws MalformedURLException {
        return new GeoIPProcessor(pluginSetting, geoCodingProcessorConfig, geoIpConfigSupplier);
    }

    private List<String> setAttributes() {
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
}
