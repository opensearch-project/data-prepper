/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ObjectWorkerTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private InputCodec codec;

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    private Duration bufferTimeout;
    private int recordsToAccumulate;

    @Mock
    private S3ObjectReference s3ObjectReference;
    @Mock
    private Counter s3ObjectsFailedCounter;

    @Mock
    private Counter s3ObjectsCodecParsingFailedCounter;
    @Mock
    private Counter s3ObjectsFailedNotFoundCounter;
    @Mock
    private Counter s3ObjectsFailedAccessDeniedCounter;
    @Mock
    private Counter s3ObjectsSucceededCounter;
    @Mock
    private Counter s3ObjectNoRecordsFound;
    @Mock
    private Timer s3ObjectReadTimer;
    @Mock
    private DistributionSummary s3ObjectSizeSummary;
    @Mock
    private DistributionSummary s3ObjectSizeProcessedSummary;
    @Mock
    private DistributionSummary s3ObjectEventsSummary;
    @Mock
    private BiConsumer<Event, S3ObjectReference> eventConsumer;
    private String bucketName;
    private String key;
    @Mock
    private InputStream objectInputStream;
    @Mock
    private GetObjectResponse getObjectResponse;

    private AtomicInteger recordsWritten;

    private Record<Event> receivedRecord;

    @Mock
    private HeadObjectResponse headObjectResponse;

    private Exception exceptionThrownByCallable;
    private Random random;
    private long objectSize;
    private Instant testTime;
    @Mock
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    @Mock
    private SourceCoordinator<S3SourceProgressState> sourceCoordinator;

    private int numEventsAdded;

    @BeforeEach
    void setUp() throws Exception {
        random = new Random();
        bufferTimeout = Duration.ofMillis(random.nextInt(100) + 100);
        recordsToAccumulate = random.nextInt(10) + 2;

        acknowledgementSet = mock(AcknowledgementSet.class);
        lenient().doAnswer(a -> {
            numEventsAdded++;
            return null;
        }).when(acknowledgementSet).add(any(Event.class));
        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);

        s3ObjectPluginMetrics = mock(S3ObjectPluginMetrics.class);
        lenient().when(s3ObjectPluginMetrics.getS3ObjectReadTimer()).thenReturn(s3ObjectReadTimer);
        objectSize = random.nextInt(100_000) + 10_000;

        exceptionThrownByCallable = null;
        lenient().when(s3ObjectReadTimer.recordCallable(any(Callable.class)))
                .thenAnswer(a -> {
                    try {
                        a.getArgument(0, Callable.class).call();
                    } catch (final Exception ex) {
                        exceptionThrownByCallable = ex;
                        throw ex;
                    }
                    return null;
                });
        lenient().when(getObjectResponse.contentLength()).thenReturn(objectSize);
        lenient().when(headObjectResponse.contentLength()).thenReturn(objectSize);
        testTime = Instant.now();
        lenient().when(headObjectResponse.lastModified()).thenReturn(testTime);
    }

    private S3ObjectWorker createObjectUnderTest(final S3ObjectPluginMetrics s3ObjectPluginMetrics) {
        final S3ObjectRequest request = new S3ObjectRequest
                .Builder(buffer, recordsToAccumulate, bufferTimeout, s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .eventConsumer(eventConsumer).codec(codec).s3Client(s3Client)
                .compressionOption(CompressionOption.NONE)
                .build();
        return new S3ObjectWorker(request);
    }

    @Test
    void processS3Object_calls_getObject_with_correct_GetObjectRequest() throws IOException {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);
    }

    @Test
    void processS3Object_calls_getObject_with_correct_GetObjectRequest_with_AcknowledgementSet() throws IOException {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        numEventsAdded = 0;
        doAnswer(a -> {
            Record record = mock(Record.class);
            final Event event = mock(Event.class);
            final EventMetadata metadata = mock(EventMetadata.class);
            final EventHandle eventHandle = mock(EventHandle.class);
            when(record.getData()).thenReturn(event);
            when(event.getMetadata()).thenReturn(metadata);
            when(event.getEventHandle()).thenReturn(eventHandle);
            Consumer c = (Consumer)a.getArgument(2);
            c.accept(record);
            return null;
        }).when(codec).parse(any(InputFile.class), any(DecompressionEngine.class), any(Consumer.class));
        createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);
        assertThat(numEventsAdded, equalTo(1));
        verifyNoInteractions(s3ObjectNoRecordsFound);
    }

    @Test
    void processS3Object_calls_getObject_with_correct_GetObjectRequest_when_bucketOwner_is_present() throws IOException {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        final String bucketOwner = UUID.randomUUID().toString();
        lenient().when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(bucketOwner));
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);
        createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);
    }

    @Test
    void processS3Object_calls_Codec_parse_on_S3InputStream() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);

        final ArgumentCaptor<InputFile> inputFileArgumentCaptor = ArgumentCaptor.forClass(InputFile.class);
        verify(codec).parse(inputFileArgumentCaptor.capture(), any(DecompressionEngine.class), any(Consumer.class));
        final InputFile actualInputFile = inputFileArgumentCaptor.getValue();
        assertThat(actualInputFile, instanceOf(S3InputFile.class));
    }

    @Test
    void S3ObjectWorker_with_MetadataOnly_Test() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        recordsWritten = new AtomicInteger(0);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        doAnswer(a -> {
            receivedRecord = a.getArgument(0);
            recordsWritten.incrementAndGet();
            return null;
        }).when(bufferAccumulator).add(any(Record.class));

        doAnswer(a -> {
            return recordsWritten.get();
        }).when(bufferAccumulator).getTotalWritten();

        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.METADATA_ONLY, acknowledgementSet, null, null);
            assertThat(recordsWritten.get(), equalTo(1));
            Event event = receivedRecord.getData();
            assertThat(event.get("bucket", String.class), equalTo(bucketName));
            assertThat(event.get("key", String.class), equalTo(key));
            assertThat(event.get("length", Long.class), equalTo(objectSize));
            assertThat(event.get("time", Instant.class), equalTo(testTime));
        }
    }

    @Test
    void processS3Object_codec_parse_exception() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);

        doThrow(IOException.class).when(codec).parse(any(InputFile.class), any(DecompressionEngine.class), any(Consumer.class));

        assertThrows(
            IOException.class,
            () -> createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null));

        final ArgumentCaptor<InputFile> inputFileArgumentCaptor = ArgumentCaptor.forClass(InputFile.class);
        verify(codec).parse(inputFileArgumentCaptor.capture(), any(DecompressionEngine.class), any(Consumer.class));
        final InputFile actualInputFile = inputFileArgumentCaptor.getValue();
        assertThat(actualInputFile, instanceOf(S3InputFile.class));
    }

    @Test
    void processS3Object_calls_Codec_parse_with_Consumer_that_adds_to_BufferAccumulator() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);
        }

        final ArgumentCaptor<Consumer<Record<Event>>> eventConsumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(codec).parse(any(InputFile.class), any(DecompressionEngine.class), eventConsumerArgumentCaptor.capture());

        final Consumer<Record<Event>> consumerUnderTest = eventConsumerArgumentCaptor.getValue();

        final Record<Event> record = mock(Record.class);
        final Event event = mock(Event.class);
        final EventMetadata metadata = mock(EventMetadata.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(event);
        when(event.getMetadata()).thenReturn(metadata);
        when(event.getEventHandle()).thenReturn(eventHandle);

        consumerUnderTest.accept(record);

        final InOrder inOrder = inOrder(eventConsumer, bufferAccumulator);
        inOrder.verify(eventConsumer).accept(event, s3ObjectReference);
        inOrder.verify(bufferAccumulator).add(record);
    }

    @Test
    void processS3Object_calls_Codec_parse_with_Consumer_that_adds_to_BufferAccumulator_and_saves_state() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        final String testPartitionKey = UUID.randomUUID().toString();

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, sourceCoordinator, testPartitionKey);
        }

        final ArgumentCaptor<Consumer<Record<Event>>> eventConsumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(codec).parse(any(InputFile.class), any(DecompressionEngine.class), eventConsumerArgumentCaptor.capture());

        final Consumer<Record<Event>> consumerUnderTest = eventConsumerArgumentCaptor.getValue();

        final Record<Event> record = mock(Record.class);
        final Event event = mock(Event.class);
        final EventMetadata metadata = mock(EventMetadata.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(event);
        when(event.getMetadata()).thenReturn(metadata);
        when(event.getEventHandle()).thenReturn(eventHandle);
        consumerUnderTest.accept(record);

        final InOrder inOrder = inOrder(eventConsumer, bufferAccumulator, sourceCoordinator);
        inOrder.verify(eventConsumer).accept(event, s3ObjectReference);
        inOrder.verify(bufferAccumulator).add(record);
        inOrder.verify(sourceCoordinator, times(0)).saveProgressStateForPartition(testPartitionKey, null);
    }

    @Test
    void processS3Object_calls_BufferAccumulator_flush_after_Codec_parse() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);
        }

        final InOrder inOrder = inOrder(codec, bufferAccumulator);

        inOrder.verify(codec).parse(any(InputFile.class), any(DecompressionEngine.class), any(Consumer.class));
        inOrder.verify(bufferAccumulator).flush();
    }

    @Test
    void processS3Object_increments_success_counter_after_parsing_S3_object() throws IOException {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectReadTimer()).thenReturn(s3ObjectReadTimer);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        objectUnderTest.processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);

        verify(s3ObjectsSucceededCounter).increment();
        verifyNoInteractions(s3ObjectsFailedCounter);
        assertThat(exceptionThrownByCallable, nullValue());
    }

    @Test
    void processS3Object_throws_Exception_and_increments_failure_counter_when_unable_to_parse_S3_object() throws IOException {
        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);

        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);

        final IOException expectedException = mock(IOException.class);
        doThrow(expectedException)
                .when(codec).parse(any(InputFile.class), any(DecompressionEngine.class), any(Consumer.class));

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        final IOException actualException = assertThrows(IOException.class, () -> objectUnderTest.processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void processS3Object_throws_Exception_and_increments_failure_counter_when_unable_to_GetObject_from_S3() {
        final RuntimeException expectedException = mock(S3Exception.class);
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenThrow(expectedException);

        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        final RuntimeException actualException = assertThrows(RuntimeException.class, () -> objectUnderTest.processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void processS3Object_calls_HeadObject_after_Callable() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        objectUnderTest.processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);

        final InOrder inOrder = inOrder(s3ObjectReadTimer, s3Client);

        inOrder.verify(s3ObjectReadTimer).recordCallable(any(Callable.class));
        inOrder.verify(s3Client).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void processS3Object_records_BufferAccumulator_getTotalWritten() throws IOException {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);

        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        final int totalWritten = new Random().nextInt(10_000) + 5_000;
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        when(bufferAccumulator.getTotalWritten()).thenReturn(totalWritten);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);
        }

        verify(s3ObjectEventsSummary).record(totalWritten);
    }

    @Test
    void processS3Object_records_S3_ObjectSize() throws IOException {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);

        verify(s3ObjectSizeSummary).record(objectSize);
    }

    @Test
    void processS3Object_records_S3_Object_No_Records_Found() throws IOException {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);

        verify(s3ObjectNoRecordsFound).increment();
    }

    @Test
    void processS3Object_records_input_file_bytes_read() throws IOException {
        final int inputStringLength = random.nextInt(1000) + 10;
        final byte[] inputStreamBytes = new byte[inputStringLength];
        random.nextBytes(inputStreamBytes);

        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(new ByteArrayInputStream(inputStreamBytes));
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        doAnswer(a -> {
            final S3InputFile inputFile = a.getArgument(0);
            try (InputStream inputStream = inputFile.newStream()) {
                inputStream.readAllBytes();
            }
            return a;
        }).when(codec).parse(any(InputFile.class), any(DecompressionEngine.class), any(Consumer.class));
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(s3ObjectNoRecordsFound);

        createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);

        verify(s3ObjectSizeProcessedSummary).record(inputStringLength);
    }

    @Test
    void deleteS3Object_calls_delete_object_with_expected_request_success() {
        final ArgumentCaptor<DeleteObjectRequest> deleteObjectRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        final String accountOwner = UUID.randomUUID().toString();

        when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(accountOwner));

        when(s3Client.deleteObject(deleteObjectRequestArgumentCaptor.capture())).thenReturn(mock(DeleteObjectResponse.class));


        final S3ObjectWorker objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);

        objectUnderTest.deleteS3Object(s3ObjectReference);

        final DeleteObjectRequest deleteObjectRequest = deleteObjectRequestArgumentCaptor.getValue();
        assertThat(deleteObjectRequest, notNullValue());
        assertThat(deleteObjectRequest.bucket(), equalTo(bucketName));
        assertThat(deleteObjectRequest.key(), equalTo(key));
        assertThat(deleteObjectRequest.expectedBucketOwner(), equalTo(accountOwner));
    }

    @Test
    void deleteS3Object_increments_failed_deletion_metric_after_max_retries() {
        final ArgumentCaptor<DeleteObjectRequest> deleteObjectRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        final String accountOwner = UUID.randomUUID().toString();

        when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(accountOwner));

        when(s3Client.deleteObject(deleteObjectRequestArgumentCaptor.capture())).thenThrow(RuntimeException.class);

        final Counter s3ObjectDeteleFailedCounter = mock(Counter.class);
        when(s3ObjectPluginMetrics.getS3ObjectsDeleteFailed()).thenReturn(s3ObjectDeteleFailedCounter);


        final S3ObjectWorker objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);

        objectUnderTest.deleteS3Object(s3ObjectReference);

        verify(s3ObjectDeteleFailedCounter).increment();
    }

}
