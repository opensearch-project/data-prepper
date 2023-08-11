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
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionEngine;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CompressionBufferFactoryTest {
    @Mock
    private BufferFactory innerBufferFactory;

    @Mock
    private CompressionEngine compressionEngine;

    private CompressionBufferFactory createObjectUnderTest() {
        return new CompressionBufferFactory(innerBufferFactory, compressionEngine);
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

    @Nested
    class WithBuffer {
        @Mock
        private Buffer innerBuffer;

        @BeforeEach
        void setUp() {
            when(innerBufferFactory.getBuffer()).thenReturn(innerBuffer);
        }

        @Test
        void getBuffer_returns_CompressionBuffer() {
            final Buffer buffer = createObjectUnderTest().getBuffer();
            assertThat(buffer, instanceOf(CompressionBuffer.class));
        }

        @Test
        void getBuffer_returns_new_on_each_call() {
            final CompressionBufferFactory objectUnderTest = createObjectUnderTest();
            final Buffer firstBuffer = objectUnderTest.getBuffer();

            assertThat(objectUnderTest.getBuffer(), not(equalTo(firstBuffer)));
        }
    }
}