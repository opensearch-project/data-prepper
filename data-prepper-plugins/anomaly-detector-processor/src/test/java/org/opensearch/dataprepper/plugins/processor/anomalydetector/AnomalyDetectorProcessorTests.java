/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestMode;
import org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig;

@ExtendWith(MockitoExtension.class)
public class AnomalyDetectorProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter numberRCFInstances;
    @Mock
    private Counter recordsIn;

    @Mock
    private Counter recordsOut;

    @Mock
    private Timer timeElapsed;

    @Mock
    private AnomalyDetectorProcessorConfig mockConfig;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginModel modeConfiguration;

    private AnomalyDetectorProcessor anomalyDetectorProcessor;


    @BeforeEach
    void setUp() {
        when(mockConfig.getKeys()).thenReturn(new ArrayList<String>(Collections.singleton("latency")));
        RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();

        when(mockConfig.getDetectorMode()).thenReturn(modeConfiguration);
        when(modeConfiguration.getPluginName()).thenReturn(UUID.randomUUID().toString());
        when(modeConfiguration.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(pluginFactory.loadPlugin(eq(AnomalyDetectorMode.class), any(PluginSetting.class)))
            .thenAnswer(invocation -> new RandomCutForestMode(randomCutForestModeConfig));

        when(pluginMetrics.counter(AnomalyDetectorProcessor.NUMBER_RCF_INSTANCES)).thenReturn(numberRCFInstances);
        when(pluginMetrics.counter(MetricNames.RECORDS_IN)).thenReturn(recordsIn);
        when(pluginMetrics.counter(MetricNames.RECORDS_OUT)).thenReturn(recordsOut);
        when(pluginMetrics.timer(MetricNames.TIME_ELAPSED)).thenReturn(timeElapsed);

    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6})
    void testAnomalyDetectorProcessor(int type) {

        anomalyDetectorProcessor = new AnomalyDetectorProcessor(mockConfig, pluginMetrics, pluginFactory);
        final int numSamples = 1024;
        final List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            Object value;
            switch (type) {
                case 1: value = (byte)ThreadLocalRandom.current().nextInt(1, 3);
                        break;
                case 2: value = (short)ThreadLocalRandom.current().nextInt(1, 3);
                        break;
                case 3: value = ThreadLocalRandom.current().nextInt(1, 3);
                        break;
                case 4: value = ThreadLocalRandom.current().nextLong(1, 3);
                        break;
                case 5: value = (float)ThreadLocalRandom.current().nextDouble(0.5, 0.6);
                        break;
                default: value = ThreadLocalRandom.current().nextDouble(0.5, 0.6);
                        break;
            }
            records.add(getLatencyMessage(UUID.randomUUID().toString(), value));
        }
        anomalyDetectorProcessor.doExecute(records);
        
        Object anomalyValue;
        switch (type) {
            case 1: anomalyValue = (byte)(int)ThreadLocalRandom.current().nextInt(115, 120);
                    break;
            case 2: anomalyValue = (short)(int)ThreadLocalRandom.current().nextInt(15000, 16000);
                    break;
            case 3: anomalyValue = ThreadLocalRandom.current().nextInt(15000, 16000);
                    break;
            case 4: anomalyValue = ThreadLocalRandom.current().nextLong(15000, 16000);
                    break;
            case 5: anomalyValue = (float)(double)ThreadLocalRandom.current().nextDouble(10.5, 10.8);
                    break;
            default: anomalyValue = ThreadLocalRandom.current().nextDouble(10.5, 10.8);
                    break;
        }
        final List<Record<Event>> recordsWithAnomaly = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyMessage(UUID.randomUUID().toString(), anomalyValue)));
        assertThat(recordsWithAnomaly.size(), equalTo(1));
        Event event = recordsWithAnomaly.get(0).getData();
        List<Double> deviation = event.get(AnomalyDetectorProcessor.DEVIATION_KEY, List.class);
        assertThat((double)deviation.get(0), greaterThan(9.0));
        double grade = (double)event.get(AnomalyDetectorProcessor.GRADE_KEY, Double.class);
        assertThat(grade, equalTo(1.0));
    }

    @Test
    void testAnomalyDetectorProcessorTwoKeys() {

        anomalyDetectorProcessor = new AnomalyDetectorProcessor(mockConfig, pluginMetrics, pluginFactory);
        final int numSamples = 1024;
        final List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            records.add(getLatencyBytesMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6), ThreadLocalRandom.current().nextLong(100, 110)));
        }
        anomalyDetectorProcessor.doExecute(records);
        final List<Record<Event>> recordsWithAnomaly = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyBytesMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(15.4, 15.8), ThreadLocalRandom.current().nextLong(1000, 1110))));
        assertThat(recordsWithAnomaly.size(), equalTo(1));
        Event event = recordsWithAnomaly.get(0).getData();
        List<Double> deviation = event.get(AnomalyDetectorProcessor.DEVIATION_KEY, List.class);
        for (int i = 0; i < mockConfig.getKeys().size(); i++) {
            assertThat((double)deviation.get(i), greaterThan(9.0));
        }
        double grade = (double)event.get(AnomalyDetectorProcessor.GRADE_KEY, Double.class);
        assertThat(grade, equalTo(1.0));
    }

    @Test
    void testAnomalyDetectorProcessorNoMatchingKeys() {
        when(mockConfig.getKeys()).thenReturn(new ArrayList<String>(Collections.singleton("bytes")));
        anomalyDetectorProcessor = new AnomalyDetectorProcessor(mockConfig, pluginMetrics, pluginFactory);
        final int numSamples = 1024;
        final List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            records.add(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6)));
        }
        anomalyDetectorProcessor.doExecute(records);
        
        final List<Record<Event>> recordsWithAnomaly = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(10.4, 10.8))));
        assertThat(recordsWithAnomaly.size(), equalTo(0));
    }

    @Test
    void testAnomalyDetectorProcessorInvalidTypeKeys() {
        when(mockConfig.getKeys()).thenReturn(new ArrayList<String>(Collections.singleton("bytes")));
        anomalyDetectorProcessor = new AnomalyDetectorProcessor(mockConfig, pluginMetrics, pluginFactory);
        final int numSamples = 1024;
        final List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            records.add(getBytesStringMessage(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        assertThrows(RuntimeException.class, () -> anomalyDetectorProcessor.doExecute(records));
    }

    @Test
    void testAnomalyDetectorCardinality() {
        List<String> identificationKeyList = new ArrayList<String>();
        identificationKeyList.add("ip");
        when(mockConfig.getIdentificationKeys()).thenReturn(identificationKeyList);

        anomalyDetectorProcessor = new AnomalyDetectorProcessor(mockConfig, pluginMetrics, pluginFactory);
        final int numSamples = 1024;
        final List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            if (i % 2 == 0) {
                records.add(getLatencyBytesMessageWithIp(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6), ThreadLocalRandom.current().nextLong(100, 110), "1.1.1.1"));
            } else {
                records.add(getLatencyBytesMessageWithIp(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(15.5, 15.6), ThreadLocalRandom.current().nextLong(1000, 1110), "255.255.255.255"));
            }
        }

        anomalyDetectorProcessor.doExecute(records);

        final List<Record<Event>> slowRecordFromFastIp = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyBytesMessageWithIp(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(15.5, 15.8), ThreadLocalRandom.current().nextLong(1000, 1110), "1.1.1.1")));
        assertThat(slowRecordFromFastIp.size(), equalTo(1));

        final List<Record<Event>> slowRecordFromSlowIp = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyBytesMessageWithIp(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(15.5, 15.6), ThreadLocalRandom.current().nextLong(1000, 1110), "255.255.255.255")));
        assertThat(slowRecordFromSlowIp.size(), equalTo(0));

    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
            .withData(data)
            .withEventType("event")
            .build());
    }

    private Record<Event> getBytesStringMessage(String message, String bytes) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put("bytes", bytes);
        return buildRecordWithEvent(testData);
    }

    private Record<Event> getLatencyMessage(String message, Object latency) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put("latency", latency);
        return buildRecordWithEvent(testData);
    }

    private Record<Event> getLatencyBytesMessage(String message, double latency, long bytes) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put("latency", latency);
        testData.put("bytes", bytes);
        return buildRecordWithEvent(testData);
    }

    private Record<Event> getLatencyBytesMessageWithIp(String message, double latency, long bytes, String ip) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put("latency", latency);
        testData.put("bytes", bytes);
        testData.put("ip", ip);

        return buildRecordWithEvent(testData);
    }
}
