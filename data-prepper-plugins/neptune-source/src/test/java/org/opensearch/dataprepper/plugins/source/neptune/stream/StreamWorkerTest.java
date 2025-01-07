/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.stream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.neptune.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;

import java.util.Random;

import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.BYTES_PROCESSED;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.BYTES_RECEIVED;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.FAILURE_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.SUCCESS_ITEM_COUNTER_NAME;

@ExtendWith(MockitoExtension.class)
public class StreamWorkerTest {
    @Mock
    private RecordBufferWriter mockRecordBufferWriter;
    @Mock
    private StreamRecordConverter mockRecordConverter;
    @Mock
    private NeptuneSourceConfig mockSourceConfig;
    @Mock
    private StreamAcknowledgementManager mockStreamAcknowledgementManager;
    @Mock
    private DataStreamPartitionCheckpoint mockPartitionCheckpoint;
    @Mock
    private PluginMetrics mockPluginMetrics;

    @Mock
    private AwsConfig mockAwsConfig;
    @Mock
    private StreamPartition streamPartition;
    @Mock
    private StreamProgressState streamProgressState;
    @Mock
    private Counter successItemsCounter;
    @Mock
    private DistributionSummary bytesReceivedSummary;
    @Mock
    private DistributionSummary bytesProcessedSummary;
    @Mock
    private Counter failureItemsCounter;

    @Mock
    private Counter streamApiInvocations;

    @Mock
    private Counter stream4xxErrors;

    @Mock
    private Counter stream5xxErrors;

    private StreamWorker streamWorker;

    private static final Random random = new Random();

    @BeforeEach
    public void setup() {
        when(mockPluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME)).thenReturn(successItemsCounter);
        when(mockPluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME)).thenReturn(failureItemsCounter);
        when(mockPluginMetrics.summary(BYTES_RECEIVED)).thenReturn(bytesReceivedSummary);
        when(mockPluginMetrics.summary(BYTES_PROCESSED)).thenReturn(bytesProcessedSummary);
        when(mockSourceConfig.isAcknowledgments()).thenReturn(false);
        when(mockSourceConfig.getRegion()).thenReturn("us-east-1");
        streamWorker = new StreamWorker(mockRecordBufferWriter, mockRecordConverter, mockSourceConfig, mockStreamAcknowledgementManager,
                mockPartitionCheckpoint, mockPluginMetrics, 2, 1000, 10_000, 1_000);
    }
}
