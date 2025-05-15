/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.ocsf;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class OcsfProcessorTest {

    public class TestOcsfTransformer implements OcsfTransformer {
        public void transform(Event event, final String version) {
            if (event.get("shouldThrow", Boolean.class) != null) {
                throw new RuntimeException("Exception");
            }
            event.put("transformed", true);
        }
    }

    @Mock
    PluginMetrics pluginMetrics;
    @Mock
    PluginFactory pluginFactory;
    @Mock
    PluginModel testPlugin;
    @Mock
    OcsfProcessorConfig ocsfProcessorConfig;
    @Mock
    Counter errorCounter;

    AtomicInteger errorCount;

    OcsfProcessor ocsfProcessor;
    TestOcsfTransformer testOcsfTransformer;
    
    @BeforeEach
    void setup() {
        errorCount = new AtomicInteger(0);
        testOcsfTransformer = new TestOcsfTransformer();
        ocsfProcessorConfig = mock(OcsfProcessorConfig.class);
        testPlugin = mock(PluginModel.class);
        when(testPlugin.getPluginName()).thenReturn("test-plugin");
        when(testPlugin.getPluginSettings()).thenReturn(Map.of());
        when(ocsfProcessorConfig.getSchemaType()).thenReturn(testPlugin);
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);
        when(pluginFactory.loadPlugin(eq(OcsfTransformer.class), any())).thenReturn(testOcsfTransformer);
        errorCounter = mock(Counter.class);
        lenient().doAnswer(a -> {
            errorCount.getAndAdd(1);
            return null;
        }).when(errorCounter).increment();
        lenient().doAnswer(a -> {
            return errorCounter;
        }).when(pluginMetrics).counter(anyString());
    }

    OcsfProcessor createObjectUnderTest() {
        return new OcsfProcessor(ocsfProcessorConfig, pluginMetrics, pluginFactory);
    }

    @Test
    public void testSuccessfulTransformations() {
        ocsfProcessor = createObjectUnderTest();
        int numRecords = 10;
        Collection<Record<Event>> records = getRecordList(numRecords);
        List<Record<Event>> recordsOut = (List<Record<Event>>)ocsfProcessor.doExecute(records);
        assertThat(recordsOut.size(), equalTo(numRecords));
        for (final Record<Event> record: recordsOut) {
            assertThat(record.getData().get("transformed", Boolean.class), equalTo(true));
        }
        
    }

    @Test
    public void testFailedTransformations() {
        ocsfProcessor = createObjectUnderTest();
        int numRecords = 10;
        Collection<Record<Event>> records = getRecordList(numRecords);
        for (Record<Event> record: records) {
            record.getData().put("shouldThrow", true);
        }
        List<Record<Event>> recordsOut = (List<Record<Event>>)ocsfProcessor.doExecute(records);
        assertThat(recordsOut.size(), equalTo(numRecords));
        for (final Record<Event> record: recordsOut) {
            assertThat(record.getData().get("transformed", Boolean.class), equalTo(null));
        }
        assertThat(errorCount.get(), equalTo(numRecords));
        
    }

    private Collection<Record<Event>> getRecordList(int numberOfRecords) {
        final Collection<Record<Event>> recordList = new ArrayList<>();
        List<HashMap> records = generateRecords(numberOfRecords);
        for (int i = 0; i < numberOfRecords; i++) {
            final Event event = JacksonLog.builder()
                                .withData(records.get(i))
                                .build();
            recordList.add(new Record<>(event));
        }
        return recordList;
    }

    private List<HashMap> generateRecords(int numberOfRecords) {
        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {
            HashMap<String, String> eventData = new HashMap<>();
            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add(eventData);

        }
        return recordList;
    }
}

