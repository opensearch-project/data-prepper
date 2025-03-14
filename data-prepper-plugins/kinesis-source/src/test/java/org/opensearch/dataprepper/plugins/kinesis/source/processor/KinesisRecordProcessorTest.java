/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.converter.KinesisRecordConverter;
import org.opensearch.dataprepper.plugins.kinesis.source.converter.MetadataKeyAttributes;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisStreamNotFoundException;
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
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_CHECKPOINT_FAILURES;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_RECORD_BYTES_PROCESSED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_RECORD_BYTES_RECEIVED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_RECORD_PROCESSED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_RECORD_PROCESSING_ERRORS_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_STREAM_TAG_KEY;

public class KinesisRecordProcessorTest {
    private KinesisRecordProcessor kinesisRecordProcessor;
    private static final String shardId = "123";
    private static final String streamId = "stream-1";
    private static final String codec_plugin_name = "json";
    private static final String sequence_number = "10001";
    private static final Long sub_sequence_number = 1L;

    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMillis(1000);
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
    StreamIdentifier streamIdentifier;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private Counter recordProcessed;

    @Mock
    private Counter recordProcessingErrors;

    @Mock
    private DistributionSummary bytesReceivedSummary;

    @Mock
    private DistributionSummary bytesProcessedSummary;

    @Mock
    private Counter checkpointFailures;

    @Mock
    private Counter acknowledgementSetSuccesses;

    @Mock
    private Counter acknowledgementSetFailures;

    @Mock
    private InputCodec codec;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    @Mock
    private KinesisRecordConverter kinesisRecordConverter;

    @Mock
    private KinesisCheckpointerTracker kinesisCheckpointerTracker;

    @Mock
    private DecompressionEngine decompressionEngine;

    @BeforeEach
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);
        acknowledgementSet = mock(AcknowledgementSet.class);
        bufferAccumulator = mock(BufferAccumulator.class);
        kinesisRecordConverter = mock(KinesisRecordConverter.class);
        kinesisCheckpointerTracker = mock(KinesisCheckpointerTracker.class);

        when(initializationInput.shardId()).thenReturn(shardId);
        when(streamIdentifier.streamName()).thenReturn(streamId);
        when(kinesisStreamConfig.getName()).thenReturn(streamId);
        when(kinesisStreamConfig.getCompression()).thenReturn(CompressionOption.NONE);
        PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn(codec_plugin_name);
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(kinesisSourceConfig.getCodec()).thenReturn(pluginModel);

        codec = mock(InputCodec.class);
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any())).thenReturn(codec);
        when(kinesisStreamConfig.getCheckPointInterval()).thenReturn(CHECKPOINT_INTERVAL);
        when(kinesisSourceConfig.getNumberOfRecordsToAccumulate()).thenReturn(NUMBER_OF_RECORDS_TO_ACCUMULATE);
        when(kinesisSourceConfig.getStreams()).thenReturn(List.of(kinesisStreamConfig));
        when(processRecordsInput.checkpointer()).thenReturn(checkpointer);
        when(pluginMetrics.counterWithTags(ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME, KINESIS_STREAM_TAG_KEY,
                streamIdentifier.streamName())).thenReturn(acknowledgementSetSuccesses);
        when(pluginMetrics.counterWithTags(ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME, KINESIS_STREAM_TAG_KEY,
                streamIdentifier.streamName())).thenReturn(acknowledgementSetFailures);

        InputStream inputStream = mock(InputStream.class);
        when(decompressionEngine.createInputStream(any(InputStream.class))).thenReturn(inputStream);
        CompressionOption compressionOption = mock(CompressionOption.class);
        when(kinesisStreamConfig.getCompression()).thenReturn(compressionOption);
        when(compressionOption.getDecompressionEngine()).thenReturn(decompressionEngine);

        recordProcessed = mock(Counter.class);
        when(pluginMetrics.counterWithTags(KINESIS_RECORD_PROCESSED_METRIC_NAME, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName())).thenReturn(recordProcessed);

        recordProcessingErrors = mock(Counter.class);
        when(pluginMetrics.counterWithTags(KINESIS_RECORD_PROCESSING_ERRORS_METRIC_NAME, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName())).thenReturn(recordProcessingErrors);

        bytesReceivedSummary = mock(DistributionSummary.class);
        when(pluginMetrics.summary(KINESIS_RECORD_BYTES_RECEIVED_METRIC_NAME)).thenReturn(bytesReceivedSummary);

        bytesProcessedSummary = mock(DistributionSummary.class);
        when(pluginMetrics.summary(KINESIS_RECORD_BYTES_PROCESSED_METRIC_NAME)).thenReturn(bytesProcessedSummary);
    }

    @Test
    void testProcessRecordsWithoutAcknowledgementsWithCheckpointApplied()
            throws Exception {
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        when(kinesisStreamConfig.getCheckPointInterval()).thenReturn(Duration.ofMillis(0));
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);

        List<KinesisInputOutputRecord> records = new ArrayList<>();
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        event.getMetadata().setAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE, streamId);
        Record<Event> record = new Record<>(event);
        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(event.toJsonString().getBytes()))
                .sequenceNumber(Integer.toString(1000)).subSequenceNumber(1).build();
        List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        kinesisClientRecords.add(kinesisClientRecord);
        final long recordsSize = kinesisClientRecords.stream()
                .map(kclRecord -> kclRecord.data().position())
                .mapToLong(Integer::longValue).sum();

        records.add(KinesisInputOutputRecord.builder()
                .withDataPrepperRecord(record)
                .withIncomingRecordSizeBytes(recordsSize).build()
        );
        when(processRecordsInput.records()).thenReturn(kinesisClientRecords);


        InputStream inputStream = mock(InputStream.class);
        when(decompressionEngine.createInputStream(inputStream)).thenReturn(inputStream);
        when(kinesisRecordConverter.convert(eq(decompressionEngine), eq(kinesisClientRecords), eq(streamId))).thenReturn(records);

        kinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        KinesisCheckpointerRecord kinesisCheckpointerRecord = mock(KinesisCheckpointerRecord.class);
        ExtendedSequenceNumber extendedSequenceNumber = mock(ExtendedSequenceNumber.class);
        when(extendedSequenceNumber.sequenceNumber()).thenReturn(sequence_number);
        when(extendedSequenceNumber.subSequenceNumber()).thenReturn(sub_sequence_number);
        when(kinesisCheckpointerRecord.getExtendedSequenceNumber()).thenReturn(extendedSequenceNumber);
        when(kinesisCheckpointerRecord.getCheckpointer()).thenReturn(checkpointer);
        when(kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord()).thenReturn(Optional.of(kinesisCheckpointerRecord));
        kinesisRecordProcessor.initialize(initializationInput);

        kinesisRecordProcessor.processRecords(processRecordsInput);

        verify(checkpointer).checkpoint(eq(sequence_number), eq(sub_sequence_number));

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        verify(bufferAccumulator).flush();

        List<Record<Event>> recordsCaptured = recordArgumentCaptor.getAllValues();
        assertEquals(recordsCaptured.size(), records.size());
        for (Record<Event> eventRecord: recordsCaptured) {
            EventMetadata eventMetadata = eventRecord.getData().getMetadata();
            assertEquals(eventMetadata.getAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE), streamIdentifier.streamName());
        }

        verify(acknowledgementSetManager, times(0)).create(any(), any(Duration.class));
        verify(recordProcessed, times(1)).increment(anyDouble());
        verify(bytesReceivedSummary, times(1)).record(eq((double) recordsSize));
        verify(bytesProcessedSummary, times(1)).record(eq((double) recordsSize));
    }

    @Test
    public void testProcessRecordsWithoutAcknowledgementsEnabled()
            throws Exception {
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        when(kinesisStreamConfig.getCheckPointInterval()).thenReturn(Duration.ofMillis(0));
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);

        List<KinesisInputOutputRecord> records = new ArrayList<>();
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        event.getMetadata().setAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE, streamId);
        Record<Event> record = new Record<>(event);
        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(event.toJsonString().getBytes()))
                .sequenceNumber(Integer.toString(1000)).subSequenceNumber(1).build();
        List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        kinesisClientRecords.add(kinesisClientRecord);
        final long recordsSize = kinesisClientRecords.stream()
                .map(kclRecord -> kclRecord.data().position())
                .mapToLong(Integer::longValue).sum();
        records.add(KinesisInputOutputRecord.builder()
                .withDataPrepperRecord(record)
                .withIncomingRecordSizeBytes(recordsSize).build()
        );

        when(processRecordsInput.records()).thenReturn(kinesisClientRecords);
        when(kinesisRecordConverter.convert(eq(decompressionEngine), eq(kinesisClientRecords), eq(streamId))).thenReturn(records);

        kinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        when(kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord()).thenReturn(Optional.empty());
        kinesisRecordProcessor.initialize(initializationInput);

        kinesisRecordProcessor.processRecords(processRecordsInput);

        verifyNoInteractions(checkpointer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        verify(bufferAccumulator).flush();

        List<Record<Event>> recordsCaptured = recordArgumentCaptor.getAllValues();
        assertEquals(recordsCaptured.size(), records.size());
        for (Record<Event> eventRecord: recordsCaptured) {
            EventMetadata eventMetadata = eventRecord.getData().getMetadata();
            assertEquals(eventMetadata.getAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE), streamIdentifier.streamName());
        }

        verify(acknowledgementSetManager, times(0)).create(any(), any(Duration.class));
        verify(recordProcessed, times(1)).increment(anyDouble());
        verify(bytesReceivedSummary, times(1)).record(eq((double) recordsSize));
        verify(bytesProcessedSummary, times(1)).record(eq((double) recordsSize));
    }

    @Test
    void testProcessRecordsWithAcknowledgementsEnabled()
            throws Exception {
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(true);
        when(kinesisStreamConfig.getCheckPointInterval()).thenReturn(Duration.ofMillis(0));
        AtomicReference<Integer> numEventsAdded = new AtomicReference<>(0);
        doAnswer(a -> {
            numEventsAdded.getAndSet(numEventsAdded.get() + 1);
            return null;
        }).when(acknowledgementSet).add(any(Event.class));

        doAnswer(invocation -> {
            Consumer<Boolean> consumer = invocation.getArgument(0);
            consumer.accept(true);
            return acknowledgementSet;
        }).when(acknowledgementSetManager).create(any(Consumer.class), any(Duration.class));

        List<KinesisInputOutputRecord> records = new ArrayList<>();
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        event.getMetadata().setAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE, streamId);
        Record<Event> record = new Record<>(event);
        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(event.toJsonString().getBytes()))
                .sequenceNumber(Integer.toString(1000)).subSequenceNumber(1).build();
        List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        when(processRecordsInput.records()).thenReturn(kinesisClientRecords);
        kinesisClientRecords.add(kinesisClientRecord);
        final long recordsSize = kinesisClientRecords.stream()
                .map(kclRecord -> kclRecord.data().position())
                .mapToLong(Integer::longValue).sum();
        records.add(KinesisInputOutputRecord.builder()
                .withDataPrepperRecord(record)
                .withIncomingRecordSizeBytes(recordsSize).build()
        );
        when(kinesisRecordConverter.convert(eq(decompressionEngine), eq(kinesisClientRecords), eq(streamId))).thenReturn(records);

        kinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        KinesisCheckpointerRecord kinesisCheckpointerRecord = mock(KinesisCheckpointerRecord.class);
        ExtendedSequenceNumber extendedSequenceNumber = mock(ExtendedSequenceNumber.class);
        when(extendedSequenceNumber.sequenceNumber()).thenReturn(sequence_number);
        when(extendedSequenceNumber.subSequenceNumber()).thenReturn(sub_sequence_number);
        when(kinesisCheckpointerRecord.getExtendedSequenceNumber()).thenReturn(extendedSequenceNumber);
        when(kinesisCheckpointerRecord.getCheckpointer()).thenReturn(checkpointer);
        when(kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord()).thenReturn(Optional.of(kinesisCheckpointerRecord));
        kinesisRecordProcessor.initialize(initializationInput);

        kinesisRecordProcessor.processRecords(processRecordsInput);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        verify(bufferAccumulator).flush();

        List<Record<Event>> recordsCaptured = recordArgumentCaptor.getAllValues();
        assertEquals(recordsCaptured.size(), records.size());
        for (Record<Event> eventRecord: recordsCaptured) {
            EventMetadata eventMetadata = eventRecord.getData().getMetadata();
            assertEquals(eventMetadata.getAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE), streamIdentifier.streamName());
        }
        verify(acknowledgementSetManager, times(1)).create(any(), any(Duration.class));
        verify(acknowledgementSetSuccesses, atLeastOnce()).increment();
        verify(recordProcessed, times(1)).increment(anyDouble());
        verifyNoInteractions(recordProcessingErrors);
        verify(bytesReceivedSummary, times(1)).record(eq((double) recordsSize));
        verify(bytesProcessedSummary, times(1)).record(eq((double) recordsSize));
    }

    @Test
    void testProcessRecordsWithNDJsonInputCodec()
            throws Exception {
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        when(kinesisStreamConfig.getCheckPointInterval()).thenReturn(Duration.ofMillis(0));

        PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn("ndjson");
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(kinesisSourceConfig.getCodec()).thenReturn(pluginModel);

        InputCodec codec = mock(InputCodec.class);
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any())).thenReturn(codec);

        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);

        List<KinesisInputOutputRecord> records = new ArrayList<>();
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        event.getMetadata().setAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE, streamId);
        Record<Event> record = new Record<>(event);
        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(event.toJsonString().getBytes()))
                .sequenceNumber(Integer.toString(1000)).subSequenceNumber(1).build();
        List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        when(processRecordsInput.records()).thenReturn(kinesisClientRecords);
        kinesisClientRecords.add(kinesisClientRecord);
        final long recordsSize = kinesisClientRecords.stream()
                .map(kclRecord -> kclRecord.data().position())
                .mapToLong(Integer::longValue).sum();
        records.add(KinesisInputOutputRecord.builder()
                .withDataPrepperRecord(record)
                .withIncomingRecordSizeBytes(recordsSize).build()
        );
        when(kinesisRecordConverter.convert(eq(decompressionEngine), eq(kinesisClientRecords), eq(streamId))).thenReturn(records);

        kinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        KinesisCheckpointerRecord kinesisCheckpointerRecord = mock(KinesisCheckpointerRecord.class);
        ExtendedSequenceNumber extendedSequenceNumber = mock(ExtendedSequenceNumber.class);
        when(extendedSequenceNumber.sequenceNumber()).thenReturn(sequence_number);
        when(extendedSequenceNumber.subSequenceNumber()).thenReturn(sub_sequence_number);
        when(kinesisCheckpointerRecord.getCheckpointer()).thenReturn(checkpointer);
        when(kinesisCheckpointerRecord.getExtendedSequenceNumber()).thenReturn(extendedSequenceNumber);
        when(kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord()).thenReturn(Optional.of(kinesisCheckpointerRecord));
        kinesisRecordProcessor.initialize(initializationInput);

        kinesisRecordProcessor.processRecords(processRecordsInput);

        verify(checkpointer).checkpoint(eq(sequence_number), eq(sub_sequence_number));
        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        verify(bufferAccumulator).add(recordArgumentCaptor.capture());

        List<Record<Event>> recordsCaptured = recordArgumentCaptor.getAllValues();
        assertEquals(recordsCaptured.size(), records.size());
        for (Record<Event> eventRecord: recordsCaptured) {
            EventMetadata eventMetadata = eventRecord.getData().getMetadata();
            assertEquals(eventMetadata.getAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE), streamIdentifier.streamName());
        }

        verify(acknowledgementSetManager, times(0)).create(any(), any(Duration.class));
        verify(recordProcessed, times(1)).increment(anyDouble());
        verify(bytesReceivedSummary, times(1)).record(eq((double) recordsSize));
        verify(bytesProcessedSummary, times(1)).record(eq((double) recordsSize));
    }

    @Test
    void testProcessRecordsNoThrowException()
            throws Exception {
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);

        List<KinesisInputOutputRecord> records = new ArrayList<>();
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        event.getMetadata().setAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE, streamId);
        Record<Event> record = new Record<>(event);
        List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        when(processRecordsInput.records()).thenReturn(kinesisClientRecords);
        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(event.toJsonString().getBytes()))
                .sequenceNumber(Integer.toString(1000)).subSequenceNumber(1).build();
        kinesisClientRecords.add(kinesisClientRecord);
        final long recordsSize = kinesisClientRecords.stream()
                .map(kclRecord -> kclRecord.data().position())
                .mapToLong(Integer::longValue).sum();
        records.add(KinesisInputOutputRecord.builder()
                .withDataPrepperRecord(record)
                .withIncomingRecordSizeBytes(recordsSize).build()
        );
        when(kinesisRecordConverter.convert(eq(decompressionEngine), eq(kinesisClientRecords), eq(streamId))).thenReturn(records);
        final Throwable exception = mock(RuntimeException.class);
        doThrow(exception).when(bufferAccumulator).add(any(Record.class));

        kinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        kinesisRecordProcessor.initialize(initializationInput);

        assertDoesNotThrow(() -> kinesisRecordProcessor.processRecords(processRecordsInput));
        verify(recordProcessingErrors, times(1)).increment();
        verify(recordProcessed, times(0)).increment(anyDouble());
        verify(bytesReceivedSummary, times(1)).record(eq((double) recordsSize));
        verify(bytesProcessedSummary, times(0)).record(eq((double) recordsSize));
    }

    @Test
    void testProcessRecordsBufferFlushNoThrowException()
            throws Exception {
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);

        List<KinesisInputOutputRecord> records = new ArrayList<>();
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        event.getMetadata().setAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE, streamId);
        Record<Event> record = new Record<>(event);
        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(event.toJsonString().getBytes()))
                .sequenceNumber(Integer.toString(1000)).subSequenceNumber(1).build();
        List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        when(processRecordsInput.records()).thenReturn(kinesisClientRecords);
        kinesisClientRecords.add(kinesisClientRecord);
        final long recordsSize = kinesisClientRecords.stream()
                .map(kclRecord -> kclRecord.data().position())
                .mapToLong(Integer::longValue).sum();
        records.add(KinesisInputOutputRecord.builder()
                .withDataPrepperRecord(record)
                .withIncomingRecordSizeBytes(recordsSize).build()
        );

        when(kinesisRecordConverter.convert(eq(decompressionEngine), eq(kinesisClientRecords), eq(streamId))).thenReturn(records);
        final Throwable exception = mock(RuntimeException.class);
        doThrow(exception).when(bufferAccumulator).flush();

        kinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        kinesisRecordProcessor.initialize(initializationInput);

        assertDoesNotThrow(() -> kinesisRecordProcessor.processRecords(processRecordsInput));
        verify(recordProcessingErrors, times(1)).increment();
        verify(recordProcessed, times(0)).increment(anyDouble());
        verify(bytesReceivedSummary, times(1)).record(eq((double) recordsSize));
        verify(bytesProcessedSummary, times(1)).record(eq((double) recordsSize));
    }

    @Test
    void testShardEndedLatestCheckpoint() {
        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
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

        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        ShardEndedInput shardEndedInput = mock(ShardEndedInput.class);
        when(shardEndedInput.checkpointer()).thenReturn(checkpointer);
        doThrow(exceptionType).when(checkpointer).checkpoint();

        assertDoesNotThrow(() -> mockKinesisRecordProcessor.shardEnded(shardEndedInput));

        verify(checkpointer).checkpoint();
        verify(shardEndedInput, times(1)).checkpointer();
        verify(checkpointFailures, times(1)).increment();
    }

    @Test
    void testShutdownRequestedWithLatestCheckpoint() {
        checkpointFailures = mock(Counter.class);
        when(pluginMetrics.counterWithTags(KINESIS_CHECKPOINT_FAILURES, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName())).thenReturn(checkpointFailures);

        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
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

        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        doThrow(exceptionType).when(checkpointer).checkpoint(eq(sequence_number), eq(sub_sequence_number));

        assertDoesNotThrow(() -> mockKinesisRecordProcessor.checkpoint(checkpointer, sequence_number, sub_sequence_number));

        verify(checkpointer).checkpoint(eq(sequence_number), eq(sub_sequence_number));
        verify(checkpointFailures, times(1)).increment();
    }

    @ParameterizedTest
    @ValueSource(classes = {ShutdownException.class, ThrottlingException.class, InvalidStateException.class})
    void testShutdownRequestedCheckpointerThrowsNoThrowExceptionRegularCheckpoint(final Class<Throwable> exceptionType) throws Exception {
        checkpointFailures = mock(Counter.class);
        when(pluginMetrics.counterWithTags(KINESIS_CHECKPOINT_FAILURES, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName())).thenReturn(checkpointFailures);

        KinesisRecordProcessor mockKinesisRecordProcessor = new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier);
        ShutdownRequestedInput shutdownRequestedInput = mock(ShutdownRequestedInput.class);
        when(shutdownRequestedInput.checkpointer()).thenReturn(checkpointer);
        doThrow(exceptionType).when(checkpointer).checkpoint();

        assertDoesNotThrow(() -> mockKinesisRecordProcessor.shutdownRequested(shutdownRequestedInput));

        verify(checkpointer).checkpoint();
        verify(shutdownRequestedInput, times(1)).checkpointer();
        verify(checkpointFailures, times(1)).increment();
    }

    @Test
    void testGetStreamConfig_StreamFound() {
        final String streamName = UUID.randomUUID().toString();
        when(kinesisStreamConfig.getName()).thenReturn(streamName);
        when(streamIdentifier.streamName()).thenReturn(streamName);
        when(kinesisSourceConfig.getStreams()).thenReturn(Arrays.asList(kinesisStreamConfig));

        assertDoesNotThrow(() -> new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier));
    }

    @Test
    void testGetStreamConfig_StreamNotFound() {
        // Arrange
        when(streamIdentifier.streamName()).thenReturn(UUID.randomUUID().toString());
        when(kinesisSourceConfig.getStreams()).thenReturn(Collections.emptyList());

        Exception actualException  = assertThrows(KinesisStreamNotFoundException.class, () -> new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier));

        assertThat(actualException.getMessage(), containsString(streamIdentifier.streamName()));
    }

    @Test
    void testGetStreamConfig_MultipleStreams() {
        List<KinesisStreamConfig> streamConfigs = new ArrayList<>();
        final String streamName1 = UUID.randomUUID().toString();
        when(streamIdentifier.streamName()).thenReturn(streamName1);
        KinesisStreamConfig streamConfig1 = mock(KinesisStreamConfig.class);
        when(streamConfig1.getName()).thenReturn(streamName1);
        streamConfigs.add(streamConfig1);
        KinesisStreamConfig streamConfig2 = mock(KinesisStreamConfig.class);
        when(streamConfig2.getName()).thenReturn(UUID.randomUUID().toString());
        streamConfigs.add(streamConfig2);

        when(kinesisSourceConfig.getStreams()).thenReturn(streamConfigs);

        assertDoesNotThrow(() -> new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig,
                acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier));
    }

    private List<KinesisClientRecord> createInputKinesisClientRecords() {
        List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        for (int i = 0; i< KinesisRecordProcessorTest.NUMBER_OF_RECORDS_TO_ACCUMULATE; i++) {
            Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
            KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                    .data(ByteBuffer.wrap(event.toJsonString().getBytes()))
                    .sequenceNumber(Integer.toString(100 + i)).subSequenceNumber(i).build();
            kinesisClientRecords.add(kinesisClientRecord);
        }
        return kinesisClientRecords;
    }
}
