/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.csv;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.source.loggenerator.logtypes.VpcFlowLogTypeGenerator;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

@ExtendWith(MockitoExtension.class)
public class CsvProcessorIT {
    private static final String PLUGIN_NAME = "csv";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final String DELIMITER = " ";
    private CsvProcessorConfig csvProcessorConfig;
    private CsvProcessor csvProcessor;
    private VpcFlowLogTypeGenerator vpcFlowLogTypeGenerator;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setup() {
        csvProcessorConfig = new CsvProcessorConfig();
        try {
            setField(CsvProcessorConfig.class, CsvProcessorIT.this.csvProcessorConfig, "delimiter", DELIMITER);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        csvProcessor = new CsvProcessor(pluginMetrics, csvProcessorConfig, expressionEvaluator);
        vpcFlowLogTypeGenerator = new VpcFlowLogTypeGenerator();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 25, 50, 500})
    void when_processSpaceSeparatedVpcFlowLogs_then_parsesCorrectly(final int numberOfRecords) {
        final List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            final Event thisEvent = vpcFlowLogTypeGenerator.generateEvent();
            final Record<Event> asRecord = new Record<>(thisEvent);
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
        final String[] logSplitOnSpace = knownLogSnippet.split(DELIMITER);
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
}
