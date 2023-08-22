/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionEngine;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CompressionBufferTest {
    @Mock
    private Buffer innerBuffer;

    @Mock
    private CompressionEngine compressionEngine;

    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
    }

    private CompressionBuffer createObjectUnderTest() {
        return new CompressionBuffer(innerBuffer, compressionEngine);
    }

    @Test
    void constructor_throws_if_innerBuffer_is_null() {
        innerBuffer = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_compressionEngine_is_null() {
        compressionEngine = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getSize_returns_inner_getSize() {
        final long size = random.nextInt(10_000) + 1_000;

        final CompressionBuffer objectUnderTest = createObjectUnderTest();
        when(innerBuffer.getSize()).thenReturn(size);

        assertThat(objectUnderTest.getSize(), equalTo(size));
    }

    @Test
    void getEventCount_returns_inner_getEventCount() {
        final int eventCount = random.nextInt(10_000) + 1_000;

        final CompressionBuffer objectUnderTest = createObjectUnderTest();
        when(innerBuffer.getEventCount()).thenReturn(eventCount);

        assertThat(objectUnderTest.getEventCount(), equalTo(eventCount));
    }

    @Test
    void getDuration_returns_inner_getDuration() {
        final long duration = random.nextInt(10_000) + 1_000;

        final CompressionBuffer objectUnderTest = createObjectUnderTest();
        when(innerBuffer.getDuration()).thenReturn(duration);

        assertThat(objectUnderTest.getDuration(), equalTo(duration));
    }

    @Test
    void flushToS3_calls_inner_flushToS3() {
        final S3Client s3Client = mock(S3Client.class);
        final String bucket = UUID.randomUUID().toString();
        final String key = UUID.randomUUID().toString();

        createObjectUnderTest().flushToS3();

        verify(innerBuffer).flushToS3();
    }

    @Test
    void getOutputStream_returns_outputStream_via_CompressionEngine() throws IOException {
        final OutputStream innerBufferOutputStream = mock(OutputStream.class);
        when(innerBuffer.getOutputStream()).thenReturn(innerBufferOutputStream);
        final OutputStream compressionEngineOutputStream = mock(OutputStream.class);
        when(compressionEngine.createOutputStream(innerBufferOutputStream)).thenReturn(compressionEngineOutputStream);

        final OutputStream actualOutputStream = createObjectUnderTest().getOutputStream();


        assertThat(actualOutputStream, sameInstance(compressionEngineOutputStream));
    }

    @Test
    void getOutputStream_wraps_OutputStream_only_once() throws IOException {
        final OutputStream innerBufferOutputStream = mock(OutputStream.class);
        when(innerBuffer.getOutputStream()).thenReturn(innerBufferOutputStream);
        final OutputStream compressionEngineOutputStream = mock(OutputStream.class);
        when(compressionEngine.createOutputStream(innerBufferOutputStream)).thenReturn(compressionEngineOutputStream);

        final CompressionBuffer objectUnderTest = createObjectUnderTest();
        final OutputStream outputStream = objectUnderTest.getOutputStream();
        assertThat(objectUnderTest.getOutputStream(), sameInstance(outputStream));
        assertThat(objectUnderTest.getOutputStream(), sameInstance(outputStream));
        assertThat(objectUnderTest.getOutputStream(), sameInstance(outputStream));

        verify(compressionEngine, times(1)).createOutputStream(any());
    }

    @Test
    void setEventCount_calls_inner_setEventCount() {
        final int eventCount = random.nextInt(10_000) + 1_000;

        createObjectUnderTest().setEventCount(eventCount);

        verify(innerBuffer).setEventCount(eventCount);
    }
}