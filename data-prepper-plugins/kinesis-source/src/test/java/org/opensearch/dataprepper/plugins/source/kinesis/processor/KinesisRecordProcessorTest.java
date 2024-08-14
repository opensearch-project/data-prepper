package org.opensearch.dataprepper.plugins.source.kinesis.processor;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.KinesisStreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.kinesis.processor.KinesisRecordProcessor.KINESIS_CHECKPOINT_FAILURES;
import static org.opensearch.dataprepper.plugins.source.kinesis.processor.KinesisRecordProcessor.KINESIS_RECORD_PROCESSING_ERRORS;
import static org.opensearch.dataprepper.plugins.source.kinesis.processor.KinesisRecordProcessor.KINESIS_STREAM_TAG_KEY;

public class KinesisRecordProcessorTest {
    private KinesisRecordProcessor kinesisRecordProcessor;
    private static final String shardId = "123";
    private static final String streamId = "stream-1";
    private static final String codec_plugin_name = "json";

    private static final int CHECKPOINT_INTERVAL_MS = 1000;
    private static final int NUMBER_OF_RECORDS_TO_ACCUMULATE = 10;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private KinesisSourceConfig kinesisSourceConfig;

    @Mock
    private KinesisStreamConfig kinesisStreamConfig;

    @Mock
    private InitializationInput initializationInput;

    @Mock
    private ProcessRecordsInput processRecordsInput;

    @Mock
    private RecordProcessorCheckpointer checkpointer;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    StreamIdentifier streamIdentifier;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private Counter recordProcessingErrors;

    @Mock
    private Counter checkpointFailures;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);
        acknowledgementSet = mock(AcknowledgementSet.class);

        when(initializationInput.shardId()).thenReturn(shardId);
        when(streamIdentifier.streamName()).thenReturn(streamId);
        when(kinesisStreamConfig.getName()).thenReturn(streamId);
        PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn(codec_plugin_name);
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(kinesisSourceConfig.getCodec()).thenReturn(pluginModel);

        InputCodec codec = mock(InputCodec.class);
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any())).thenReturn(codec);
        when(kinesisStreamConfig.getCheckPointIntervalInMilliseconds()).thenReturn(CHECKPOINT_INTERVAL_MS);
        when(kinesisSourceConfig.getNumberOfRecordsToAccumulate()).thenReturn(NUMBER_OF_RECORDS_TO_ACCUMULATE);
        when(kinesisSourceConfig.getStreams()).thenReturn(List.of(kinesisStreamConfig));
        when(processRecordsInput.checkpointer()).thenReturn(checkpointer);
    }

    @Test
    void testProcessRecordsWithoutAcknowledgementsCheckpointsEnabled()
            throws Exception {
        when(processRecordsInput.records()).thenReturn(createInputKinesisClientRecords());
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        when(kinesisStreamConfig.isEnableCheckPoint()).thenReturn(true);
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);

        kinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        kinesisRecordProcessor.initialize(initializationInput);

        Thread.sleep(2000);

        kinesisRecordProcessor.processRecords(processRecordsInput);

        verify(checkpointer).checkpoint();
        verify(buffer).writeAll(anyCollection(), anyInt());
        verify(acknowledgementSetManager, times(0)).create(any(), any(Duration.class));
    }

    @Test
    void testProcessRecordsWithNDJsonInputCodec()
            throws Exception {
        when(processRecordsInput.records()).thenReturn(createInputKinesisClientRecords());
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        when(kinesisStreamConfig.isEnableCheckPoint()).thenReturn(true);

        PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn("ndjson");
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(kinesisSourceConfig.getCodec()).thenReturn(pluginModel);

        InputCodec codec = mock(InputCodec.class);
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any())).thenReturn(codec);

        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);
        kinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        kinesisRecordProcessor.initialize(initializationInput);

        Thread.sleep(2000);
        kinesisRecordProcessor.processRecords(processRecordsInput);

        verify(checkpointer).checkpoint();
        verify(buffer).writeAll(anyCollection(), anyInt());
        verify(acknowledgementSetManager, times(0)).create(any(), any(Duration.class));
    }


    @Test
    void testProcessRecordsNoThrowException()
            throws Exception {
        when(processRecordsInput.records()).thenReturn(createInputKinesisClientRecords());
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        when(kinesisStreamConfig.isEnableCheckPoint()).thenReturn(false);

        final Throwable exception = mock(RuntimeException.class);
        doThrow(exception).when(buffer).writeAll(any(), anyInt());

        recordProcessingErrors = mock(Counter.class);
        when(pluginMetrics.counterWithTags(KINESIS_RECORD_PROCESSING_ERRORS, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName())).thenReturn(recordProcessingErrors);

        kinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        kinesisRecordProcessor.initialize(initializationInput);

        assertDoesNotThrow(() -> kinesisRecordProcessor.processRecords(processRecordsInput));
        verify(recordProcessingErrors, times(1)).increment();
    }

    @Test
    void testProcessRecordsWithoutAcknowledgementsAndCheckpoints()
            throws Exception {
        when(processRecordsInput.records()).thenReturn(createInputKinesisClientRecords());
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        when(kinesisStreamConfig.isEnableCheckPoint()).thenReturn(false);
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);

        kinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        kinesisRecordProcessor.initialize(initializationInput);

        kinesisRecordProcessor.processRecords(processRecordsInput);

        verifyNoInteractions(checkpointer);
        verify(buffer).writeAll(anyCollection(), anyInt());
        verify(acknowledgementSetManager, times(0)).create(any(), any(Duration.class));
    }

    @Test
    void testProcessRecordsWithAcknowledgements()
            throws Exception {
        when(processRecordsInput.records()).thenReturn(createInputKinesisClientRecords());
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(true);
        when(kinesisStreamConfig.isEnableCheckPoint()).thenReturn(false);
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);

        kinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        kinesisRecordProcessor.initialize(initializationInput);

        kinesisRecordProcessor.processRecords(processRecordsInput);

        verifyNoInteractions(checkpointer);
        verify(buffer).writeAll(anyCollection(), anyInt());
        verify(acknowledgementSetManager, times(1)).create(any(), any(Duration.class));
    }

    @Test
    void testShardEndedFlushCalled() throws Exception {
        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        ShardEndedInput shardEndedInput = mock(ShardEndedInput.class);
        when(shardEndedInput.checkpointer()).thenReturn(checkpointer);

        mockKinesisRecordProcessor.shardEnded(shardEndedInput);

        verify(shardEndedInput).checkpointer();
    }

    @ParameterizedTest
    @ValueSource(classes = {ShutdownException.class, ThrottlingException.class, InvalidStateException.class})
    void testShardEndedCheckpointerThrowsNoThrowException(final Class<Throwable> exceptionType) throws Exception {
        checkpointFailures = mock(Counter.class);
        when(pluginMetrics.counterWithTags(KINESIS_CHECKPOINT_FAILURES, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName())).thenReturn(checkpointFailures);

        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        ShardEndedInput shardEndedInput = mock(ShardEndedInput.class);
        when(shardEndedInput.checkpointer()).thenReturn(checkpointer);
        doThrow(exceptionType).when(checkpointer).checkpoint();

        assertDoesNotThrow(() -> mockKinesisRecordProcessor.shardEnded(shardEndedInput));

        verify(checkpointer).checkpoint();
        verify(shardEndedInput, times(1)).checkpointer();
        verify(checkpointFailures, times(1)).increment();
    }

    @Test
    void testShutdownRequested() {
        checkpointFailures = mock(Counter.class);
        when(pluginMetrics.counterWithTags(KINESIS_CHECKPOINT_FAILURES, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName())).thenReturn(checkpointFailures);

        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        ShutdownRequestedInput shutdownRequestedInput = mock(ShutdownRequestedInput.class);
        when(shutdownRequestedInput.checkpointer()).thenReturn(checkpointer);

        mockKinesisRecordProcessor.shutdownRequested(shutdownRequestedInput);

        verify(shutdownRequestedInput).checkpointer();
        verify(checkpointFailures, times(0)).increment();
    }

    @ParameterizedTest
    @ValueSource(classes = {ShutdownException.class, ThrottlingException.class, InvalidStateException.class})
    void testShutdownRequestedCheckpointerThrowsNoThrowException(final Class<Throwable> exceptionType) throws Exception {
        checkpointFailures = mock(Counter.class);
        when(pluginMetrics.counterWithTags(KINESIS_CHECKPOINT_FAILURES, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName())).thenReturn(checkpointFailures);

        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
        ShutdownRequestedInput shutdownRequestedInput = mock(ShutdownRequestedInput.class);
        when(shutdownRequestedInput.checkpointer()).thenReturn(checkpointer);
        doThrow(exceptionType).when(checkpointer).checkpoint();

        assertDoesNotThrow(() -> mockKinesisRecordProcessor.shutdownRequested(shutdownRequestedInput));

        verify(checkpointer).checkpoint();
        verify(shutdownRequestedInput, times(1)).checkpointer();
        verify(checkpointFailures, times(1)).increment();
    }

    private List<KinesisClientRecord> createInputKinesisClientRecords() {
        List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        for (int i = 0; i< KinesisRecordProcessorTest.NUMBER_OF_RECORDS_TO_ACCUMULATE; i++) {
            Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
            KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder().data(ByteBuffer.wrap(event.toJsonString().getBytes())).build();
            kinesisClientRecords.add(kinesisClientRecord);
        }
        return kinesisClientRecords;
    }
}
