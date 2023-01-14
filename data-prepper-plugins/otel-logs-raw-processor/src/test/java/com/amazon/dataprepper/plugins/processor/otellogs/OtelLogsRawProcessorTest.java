/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otellogs;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.JacksonOtelLog;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;


public class OtelLogsRawProcessorTest {


    private static final List<Record<OpenTelemetryLog>> EMPTY_LOGS = Stream.of(
            new Record<OpenTelemetryLog>(new JacksonOtelLog.Builder().build()),
            new Record<OpenTelemetryLog>(new JacksonOtelLog.Builder().build())).collect(Collectors.toList());


    private OTelLogsRawProcessor oTelLogsRawProcessor;


    @Before
    public void init() {
        PluginSetting testsettings = new PluginSetting("testsettings", Collections.emptyMap());
        testsettings.setPipelineName("testpipeline");
        oTelLogsRawProcessor = new OTelLogsRawProcessor(testsettings);
    }

    @Test
    public void testEmptyCollection() {
       assertThat(oTelLogsRawProcessor.doExecute(Collections.emptyList())).isEmpty();
    }

    @Test
    public void testNonemptyCollection()  {
        Collection<Record<OpenTelemetryLog>> processedRecords = oTelLogsRawProcessor.doExecute(EMPTY_LOGS);
        assertThat(processedRecords).hasSize(2);
    }

    @Test
    public void testPrepareForShutdown() {
        // Assert can be shut down
        assertTrue(oTelLogsRawProcessor.isReadyForShutdown());

        // Add records to memory/queue
        oTelLogsRawProcessor.doExecute(EMPTY_LOGS);

        // Can always be shut down because state is local
        assertTrue(oTelLogsRawProcessor.isReadyForShutdown());

    }

}
