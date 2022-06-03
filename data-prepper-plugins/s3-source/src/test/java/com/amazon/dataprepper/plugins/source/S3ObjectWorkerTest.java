/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.codec.Codec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ObjectWorkerTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private Codec codec;

    private Duration bufferTimeout;
    private int recordsToAccumulate;

    @Mock
    private S3ObjectReference s3ObjectReference;
    private String bucketName;
    private String key;

    @BeforeEach
    void setUp() {
        final Random random = new Random();
        bufferTimeout = Duration.ofMillis(random.nextInt(100) + 100);
        recordsToAccumulate = random.nextInt(10) + 2;

        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);
    }

    private S3ObjectWorker createObjectUnderTest() {
        return new S3ObjectWorker(s3Client, buffer, codec, bufferTimeout, recordsToAccumulate);
    }

    @Test
    void parseS3Object_calls_getObject_with_correct_GetObjectRequest() throws IOException {
        final ResponseInputStream<GetObjectResponse> objectInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);

        createObjectUnderTest().parseS3Object(s3ObjectReference);

        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();

        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo(key));
    }

    @Test
    void parseS3Object_calls_Codec_parse_on_S3InputStream() throws Exception {
        final ResponseInputStream<GetObjectResponse> objectInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);

        createObjectUnderTest().parseS3Object(s3ObjectReference);

        verify(codec).parse(eq(objectInputStream), any(Consumer.class));
    }

    @Test
    void parseS3Object_calls_Codec_parse_with_Consumer_that_adds_to_BufferAccumulator() throws Exception {
        final ResponseInputStream<GetObjectResponse> objectInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest().parseS3Object(s3ObjectReference);
        }

        final ArgumentCaptor<Consumer<Record<Event>>> eventConsumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(codec).parse(any(InputStream.class), eventConsumerArgumentCaptor.capture());

        final Consumer<Record<Event>> consumerUnderTest = eventConsumerArgumentCaptor.getValue();

        final Record record = mock(Record.class);
        consumerUnderTest.accept(record);
        verify(bufferAccumulator).add(record);
    }

    @Test
    void parseS3Object_calls_BufferAccumulator_flush_after_Codec_parse() throws Exception {
        final ResponseInputStream<GetObjectResponse> objectInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);

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
}