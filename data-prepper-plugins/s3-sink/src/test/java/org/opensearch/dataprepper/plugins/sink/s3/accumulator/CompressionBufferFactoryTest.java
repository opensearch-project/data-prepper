/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionEngine;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.UUID;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CompressionBufferFactoryTest {
    @Mock
    private BufferFactory innerBufferFactory;

    @Mock
    private CompressionEngine compressionEngine;

    @Mock
    private S3Client s3Client;

    @Mock
    private Supplier<String> bucketSupplier;

    @Mock
    private Supplier<String> keySupplier;

    @Mock
    private OutputCodec codec;

    private String defaultBucket;

    @BeforeEach
    void setup() {
        defaultBucket = UUID.randomUUID().toString();
    }

    private CompressionBufferFactory createObjectUnderTest() {
        return new CompressionBufferFactory(innerBufferFactory, compressionEngine, codec);
    }

    @Test
    void constructor_throws_if_inner_BufferFactory_is_null() {
        innerBufferFactory = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_CompressionEngine_is_null() {
        compressionEngine = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_Codec_is_null() {
        codec = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Nested
    class WithBuffer {
        @Mock
        private Buffer innerBuffer;

        @BeforeEach
        void setUp() {
            when(innerBufferFactory.getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket)).thenReturn(innerBuffer);
        }

        @Test
        void getBuffer_returns_CompressionBuffer() {
            final Buffer buffer = createObjectUnderTest().getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket);
            assertThat(buffer, instanceOf(CompressionBuffer.class));
        }

        @Test
        void getBuffer_returns_new_on_each_call() {
            final CompressionBufferFactory objectUnderTest = createObjectUnderTest();
            final Buffer firstBuffer = objectUnderTest.getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket);

            assertThat(objectUnderTest.getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket), not(equalTo(firstBuffer)));
        }

        @Nested
        class WithInternalCompression {
            @BeforeEach
            void setUp() {
                when(codec.isCompressionInternal()).thenReturn(true);
            }

            @Test
            void getBuffer_returns_innerBuffer_directly() {
                final Buffer buffer = createObjectUnderTest().getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket);
                assertThat(buffer, sameInstance(innerBuffer));
            }

            @Test
            void getBuffer_calls_on_each_call() {
                final CompressionBufferFactory objectUnderTest = createObjectUnderTest();
                objectUnderTest.getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket);
                objectUnderTest.getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket);

                verify(innerBufferFactory, times(2)).getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket);
            }
        }
    }
}