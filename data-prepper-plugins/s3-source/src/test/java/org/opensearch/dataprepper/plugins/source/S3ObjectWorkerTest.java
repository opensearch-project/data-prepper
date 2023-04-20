/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.compress.utils.CountingInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
    private CompressionEngine compressionEngine;

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
    private ResponseInputStream<GetObjectResponse> objectInputStream;
    @Mock
    private GetObjectResponse getObjectResponse;

    private Exception exceptionThrownByCallable;
    private Random random;
    private long objectSize;
    @Mock
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;

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
        }).when(acknowledgementSet).add(any());
        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);

        s3ObjectPluginMetrics = mock(S3ObjectPluginMetrics.class);
        when(s3ObjectPluginMetrics.getS3ObjectReadTimer()).thenReturn(s3ObjectReadTimer);
        objectSize = random.nextInt(100_000) + 10_000;

        exceptionThrownByCallable = null;
        when(s3ObjectReadTimer.recordCallable(any(Callable.class)))
                .thenAnswer(a -> {
                    try {
                        a.getArgument(0, Callable.class).call();
                    } catch (final Exception ex) {
                        exceptionThrownByCallable = ex;
                        throw ex;
                    }
                    return null;
                });
        lenient().when(objectInputStream.response()).thenReturn(getObjectResponse);
        lenient().when(getObjectResponse.contentLength()).thenReturn(objectSize);
        lenient().when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);
    }

    private S3ObjectWorker createObjectUnderTest(final S3ObjectPluginMetrics s3ObjectPluginMetrics) {
        final S3ObjectRequest request = new S3ObjectRequest.Builder(buffer, recordsToAccumulate,
                bufferTimeout, s3ObjectPluginMetrics).bucketOwnerProvider(bucketOwnerProvider)
                .eventConsumer(eventConsumer).codec(codec).s3Client(s3Client)
                .compressionEngine(compressionEngine).build();
        return new S3ObjectWorker(request);
    }

    @Test
    void parseS3Object_calls_getObject_with_correct_GetObjectRequest() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);

        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();

        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo(key));
        assertThat(actualGetObjectRequest.expectedBucketOwner(), nullValue());
    }

    @Test
    void parseS3Object_calls_getObject_with_correct_GetObjectRequest_with_AcknowledgementSet() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        numEventsAdded = 0;
        doAnswer(a -> {
            Record record = mock(Record.class);
            Consumer c = (Consumer)a.getArgument(1);
            c.accept(record);
            return null;
        }).when(codec).parse(any(InputStream.class), any(Consumer.class));
        createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);
        assertThat(numEventsAdded, equalTo(1));

        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();

        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo(key));
        assertThat(actualGetObjectRequest.expectedBucketOwner(), nullValue());
    }

    @Test
    void parseS3Object_calls_getObject_with_correct_GetObjectRequest_when_bucketOwner_is_present() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);

        final String bucketOwner = UUID.randomUUID().toString();
        when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(bucketOwner));
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);

        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();

        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo(key));
        assertThat(actualGetObjectRequest.expectedBucketOwner(), equalTo(bucketOwner));
    }

    @Test
    void parseS3Object_calls_Codec_parse_on_S3InputStream() throws Exception {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);

        createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);

        final ArgumentCaptor<InputStream> inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(codec).parse(inputStreamArgumentCaptor.capture(), any(Consumer.class));
        final InputStream actualInputStream = inputStreamArgumentCaptor.getValue();
        assertThat(actualInputStream, instanceOf(CountingInputStream.class));
    }

    @Test
    void parseS3Object_calls_Codec_parse_with_Consumer_that_adds_to_BufferAccumulator() throws Exception {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);
        }

        final ArgumentCaptor<Consumer<Record<Event>>> eventConsumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(codec).parse(any(InputStream.class), eventConsumerArgumentCaptor.capture());

        final Consumer<Record<Event>> consumerUnderTest = eventConsumerArgumentCaptor.getValue();

        final Record<Event> record = mock(Record.class);
        final Event event = mock(Event.class);
        when(record.getData()).thenReturn(event);

        consumerUnderTest.accept(record);

        final InOrder inOrder = inOrder(eventConsumer, bufferAccumulator);
        inOrder.verify(eventConsumer).accept(event, s3ObjectReference);
        inOrder.verify(bufferAccumulator).add(record);
    }

    @Test
    void parseS3Object_calls_BufferAccumulator_flush_after_Codec_parse() throws Exception {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);
        }

        final InOrder inOrder = inOrder(codec, bufferAccumulator);

        inOrder.verify(codec).parse(any(InputStream.class), any(Consumer.class));
        inOrder.verify(bufferAccumulator).flush();
    }

    @Test
    void parseS3Object_increments_success_counter_after_parsing_S3_object() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(s3ObjectPluginMetrics.getS3ObjectReadTimer()).thenReturn(s3ObjectReadTimer);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        objectUnderTest.parseS3Object(s3ObjectReference, acknowledgementSet);

        verify(s3ObjectsSucceededCounter).increment();
        verifyNoInteractions(s3ObjectsFailedCounter);
        assertThat(exceptionThrownByCallable, nullValue());
    }

    @Test
    void parseS3Object_throws_Exception_and_increments_failure_counter_when_unable_to_parse_S3_object() throws IOException {
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);

        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        final IOException expectedException = mock(IOException.class);
        doThrow(expectedException)
                .when(codec).parse(any(InputStream.class), any(Consumer.class));

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        final IOException actualException = assertThrows(IOException.class, () -> objectUnderTest.parseS3Object(s3ObjectReference, acknowledgementSet));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void parseS3Object_throws_Exception_and_increments_failure_counter_when_unable_to_GetObject_from_S3() {
        final RuntimeException expectedException = mock(S3Exception.class);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(expectedException);

        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        final RuntimeException actualException = assertThrows(RuntimeException.class, () -> objectUnderTest.parseS3Object(s3ObjectReference, acknowledgementSet));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void parseS3Object_throws_Exception_and_increments_NotFound_counter_when_GetObject_from_S3_is_404() {
        final S3Exception expectedException = mock(S3Exception.class);
        when(expectedException.statusCode()).thenReturn(404);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(expectedException);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedNotFoundCounter()).thenReturn(s3ObjectsFailedNotFoundCounter);

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        final S3Exception actualException = assertThrows(S3Exception.class, () -> objectUnderTest.parseS3Object(s3ObjectReference, acknowledgementSet));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verify(s3ObjectsFailedNotFoundCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        verifyNoInteractions(s3ObjectsFailedAccessDeniedCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void parseS3Object_throws_Exception_and_increments_NotFound_counter_when_GetObject_from_S3_is_403() {
        final S3Exception expectedException = mock(S3Exception.class);
        when(expectedException.statusCode()).thenReturn(403);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(expectedException);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedAccessDeniedCounter()).thenReturn(s3ObjectsFailedAccessDeniedCounter);
        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        final S3Exception actualException = assertThrows(S3Exception.class, () -> objectUnderTest.parseS3Object(s3ObjectReference, acknowledgementSet));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verify(s3ObjectsFailedAccessDeniedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        verifyNoInteractions(s3ObjectsFailedNotFoundCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void parseS3Object_throws_Exception_and_increments_failure_counter_when_CompressionEngine_fails() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        final IOException expectedException = mock(IOException.class);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenThrow(expectedException);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        final IOException actualException = assertThrows(IOException.class, () -> objectUnderTest.parseS3Object(s3ObjectReference, acknowledgementSet));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void parseS3Object_calls_GetObject_after_Callable() throws Exception {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        final S3ObjectHandler objectUnderTest = createObjectUnderTest(s3ObjectPluginMetrics);
        objectUnderTest.parseS3Object(s3ObjectReference, acknowledgementSet);

        final InOrder inOrder = inOrder(s3ObjectReadTimer, s3Client);

        inOrder.verify(s3ObjectReadTimer).recordCallable(any(Callable.class));
        inOrder.verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    @Test
    void parseS3Object_records_BufferAccumulator_getTotalWritten() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);

        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        final int totalWritten = new Random().nextInt(10_000) + 5_000;
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        when(bufferAccumulator.getTotalWritten()).thenReturn(totalWritten);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);
        }

        verify(s3ObjectEventsSummary).record(totalWritten);
    }

    @Test
    void parseS3Object_records_S3_ObjectSize() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);


        createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);

        verify(s3ObjectSizeSummary).record(objectSize);
    }

    @Test
    void parseS3Object_records_input_stream_bytes_read() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        final int inputStringLength = random.nextInt(1000) + 10;
        final byte[] inputStreamBytes = new byte[inputStringLength];
        random.nextBytes(inputStreamBytes);
        final InputStream compressionInputStream = new ByteArrayInputStream(inputStreamBytes);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(compressionInputStream);
        doAnswer(a -> {
            final InputStream inputStream = a.getArgument(0);
            IOUtils.copy(inputStream, new ByteArrayOutputStream());
            return a;
        }).when(codec).parse(any(InputStream.class), any(Consumer.class));
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);

        createObjectUnderTest(s3ObjectPluginMetrics).parseS3Object(s3ObjectReference, acknowledgementSet);

        verify(s3ObjectSizeProcessedSummary).record(inputStringLength);
    }

}
