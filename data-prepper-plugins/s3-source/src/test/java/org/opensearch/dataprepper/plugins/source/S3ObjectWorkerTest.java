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
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
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

    private Duration bufferTimeout;
    private int recordsToAccumulate;

    @Mock
    private S3ObjectReference s3ObjectReference;

    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter s3ObjectsFailedCounter;
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

    @BeforeEach
    void setUp() throws Exception {
        random = new Random();
        bufferTimeout = Duration.ofMillis(random.nextInt(100) + 100);
        recordsToAccumulate = random.nextInt(10) + 2;

        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);

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

        when(pluginMetrics.counter(S3ObjectWorker.S3_OBJECTS_FAILED_METRIC_NAME)).thenReturn(s3ObjectsFailedCounter);
        when(pluginMetrics.counter(S3ObjectWorker.S3_OBJECTS_FAILED_NOT_FOUND_METRIC_NAME)).thenReturn(s3ObjectsFailedNotFoundCounter);
        when(pluginMetrics.counter(S3ObjectWorker.S3_OBJECTS_FAILED_NOT_FOUND_ACCESS_DENIED)).thenReturn(s3ObjectsFailedAccessDeniedCounter);
        when(pluginMetrics.counter(S3ObjectWorker.S3_OBJECTS_SUCCEEDED_METRIC_NAME)).thenReturn(s3ObjectsSucceededCounter);
        when(pluginMetrics.timer(S3ObjectWorker.S3_OBJECTS_TIME_ELAPSED_METRIC_NAME)).thenReturn(s3ObjectReadTimer);
        when(pluginMetrics.summary(S3ObjectWorker.S3_OBJECTS_SIZE)).thenReturn(s3ObjectSizeSummary);
        when(pluginMetrics.summary(S3ObjectWorker.S3_OBJECTS_SIZE_PROCESSED)).thenReturn(s3ObjectSizeProcessedSummary);
        when(pluginMetrics.summary(S3ObjectWorker.S3_OBJECTS_EVENTS)).thenReturn(s3ObjectEventsSummary);

        lenient().when(objectInputStream.response()).thenReturn(getObjectResponse);
        lenient().when(getObjectResponse.contentLength()).thenReturn(objectSize);
        lenient().when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);
    }

    private S3ObjectWorker createObjectUnderTest() {
        return new S3ObjectWorker(s3Client, buffer, compressionEngine, codec, bucketOwnerProvider, bufferTimeout, recordsToAccumulate, eventConsumer, pluginMetrics);
    }

    @Test
    void parseS3Object_calls_getObject_with_correct_GetObjectRequest() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);

        createObjectUnderTest().parseS3Object(s3ObjectReference);

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

        createObjectUnderTest().parseS3Object(s3ObjectReference);

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
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);

        createObjectUnderTest().parseS3Object(s3ObjectReference);

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

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest().parseS3Object(s3ObjectReference);
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

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest().parseS3Object(s3ObjectReference);
        }

        final InOrder inOrder = inOrder(codec, bufferAccumulator);

        inOrder.verify(codec).parse(any(InputStream.class), any(Consumer.class));
        inOrder.verify(bufferAccumulator).flush();
    }

    @Test
    void parseS3Object_increments_success_counter_after_parsing_S3_object() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);

        final S3ObjectWorker objectUnderTest = createObjectUnderTest();
        objectUnderTest.parseS3Object(s3ObjectReference);

        verify(s3ObjectsSucceededCounter).increment();
        verifyNoInteractions(s3ObjectsFailedCounter);
        assertThat(exceptionThrownByCallable, nullValue());
    }

    @Test
    void parseS3Object_throws_Exception_and_increments_counter_when_unable_to_get_S3_object() {
        final RuntimeException expectedException = mock(RuntimeException.class);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(expectedException);

        final S3ObjectWorker objectUnderTest = createObjectUnderTest();
        final RuntimeException actualException = assertThrows(RuntimeException.class, () -> objectUnderTest.parseS3Object(s3ObjectReference));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void parseS3Object_throws_Exception_and_increments_failure_counter_when_unable_to_parse_S3_object() throws IOException {
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        final IOException expectedException = mock(IOException.class);
        doThrow(expectedException)
                .when(codec).parse(any(InputStream.class), any(Consumer.class));

        final S3ObjectWorker objectUnderTest = createObjectUnderTest();
        final IOException actualException = assertThrows(IOException.class, () -> objectUnderTest.parseS3Object(s3ObjectReference));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void parseS3Object_throws_Exception_and_increments_failure_counter_when_unable_to_GetObject_from_S3() {
        final RuntimeException expectedException = mock(RuntimeException.class);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(expectedException);

        final S3ObjectWorker objectUnderTest = createObjectUnderTest();
        final RuntimeException actualException = assertThrows(RuntimeException.class, () -> objectUnderTest.parseS3Object(s3ObjectReference));

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

        final S3ObjectWorker objectUnderTest = createObjectUnderTest();
        final S3Exception actualException = assertThrows(S3Exception.class, () -> objectUnderTest.parseS3Object(s3ObjectReference));

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

        final S3ObjectWorker objectUnderTest = createObjectUnderTest();
        final S3Exception actualException = assertThrows(S3Exception.class, () -> objectUnderTest.parseS3Object(s3ObjectReference));

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

        final S3ObjectWorker objectUnderTest = createObjectUnderTest();
        final IOException actualException = assertThrows(IOException.class, () -> objectUnderTest.parseS3Object(s3ObjectReference));

        assertThat(actualException, sameInstance(expectedException));

        verify(s3ObjectsFailedCounter).increment();
        verifyNoInteractions(s3ObjectsSucceededCounter);
        assertThat(exceptionThrownByCallable, sameInstance(expectedException));
    }

    @Test
    void parseS3Object_calls_GetObject_after_Callable() throws Exception {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);

        final S3ObjectWorker objectUnderTest = createObjectUnderTest();
        objectUnderTest.parseS3Object(s3ObjectReference);

        final InOrder inOrder = inOrder(s3ObjectReadTimer, s3Client);

        inOrder.verify(s3ObjectReadTimer).recordCallable(any(Callable.class));
        inOrder.verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    @Test
    void parseS3Object_records_BufferAccumulator_getTotalWritten() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);

        final int totalWritten = new Random().nextInt(10_000) + 5_000;
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        when(bufferAccumulator.getTotalWritten()).thenReturn(totalWritten);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest().parseS3Object(s3ObjectReference);
        }

        verify(s3ObjectEventsSummary).record(totalWritten);
    }

    @Test
    void parseS3Object_records_S3_ObjectSize() throws IOException {
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        when(compressionEngine.createInputStream(key, objectInputStream)).thenReturn(objectInputStream);

        createObjectUnderTest().parseS3Object(s3ObjectReference);

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

        createObjectUnderTest().parseS3Object(s3ObjectReference);

        verify(s3ObjectSizeProcessedSummary).record(inputStringLength);
    }
}