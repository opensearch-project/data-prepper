/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.loggenerator.logtypes.VpcFlowLogTypeGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CsvProcessorIT {
    private static final String PLUGIN_NAME = "csv";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";

    private CsvProcessorConfig csvProcessorConfig;
    private CsvProcessor csvProcessor;
    private VpcFlowLogTypeGenerator vpcFlowLogTypeGenerator;

    @BeforeEach
    void setup() {
        csvProcessorConfig = new CsvProcessorConfig();
        try {
            reflectivelySetField(csvProcessorConfig, "delimiter", " ");
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        csvProcessor = new CsvProcessor(pluginMetrics, csvProcessorConfig);
        vpcFlowLogTypeGenerator = new VpcFlowLogTypeGenerator();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 25, 50, 500})
    void when_processSpaceSeparatedVpcFlowLogs_then_parsesCorrectly(final int numberOfRecords) {
        final List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            final Event thisEvent = vpcFlowLogTypeGenerator.generateEvent();
            final Record asRecord = new Record<Event>(thisEvent);
            records.add(asRecord);
        }

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) csvProcessor.doExecute(records);

        assertThat(records.size(), equalTo(parsedRecords.size()));

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            final String originalString = parsedEvent.get("message", String.class);
            assertThat(eventHasKnownLogSnippet(parsedEvent, originalString), equalTo(true));
        }
    }

    /**
     * Helper method that asserts that an Event matches the parsed form of a space-delimited log snippet.
     * @param event
     * @param knownLogSnippet
     * @return
     */
    private boolean eventHasKnownLogSnippet(final Event event, final String knownLogSnippet) {
        final String[] logSplitOnSpace = knownLogSnippet.split(" ");
        for (int columnIndex = 0; columnIndex < logSplitOnSpace.length; columnIndex++) {
            final String field = logSplitOnSpace[columnIndex];
            final String expectedColumnName = "column" + (columnIndex+1);
            if (!event.containsKey(expectedColumnName)) {
                return false;
            }
            if (!event.get(expectedColumnName, String.class).equals(field)) {
                return false;
            }
        }
        return true;
    }

    private void reflectivelySetField(final CsvProcessorConfig csvProcessorConfig, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = CsvProcessorConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(CsvProcessorIT.this.csvProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
