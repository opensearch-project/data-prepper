/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.accumulator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {

    public static final int MAX_EVENTS = 55;

    private InMemoryBuffer inMemoryBuffer;

    @Test
    void test_with_write_event_into_buffer() throws IOException {
        inMemoryBuffer = new InMemoryBuffer();

        int i = 0;
        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            inMemoryBuffer.getOutputStream().write(generateByteArray());
            inMemoryBuffer.setEventCount(++i);
        }
        assertThat(inMemoryBuffer.getSize(), greaterThanOrEqualTo(54110L));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(MAX_EVENTS));
        assertThat(inMemoryBuffer.getDuration(), greaterThanOrEqualTo(0L));

    }

    @Test
    void test_getSinkData_success() {
        inMemoryBuffer = new InMemoryBuffer();
        Assertions.assertNotNull(inMemoryBuffer);
        assertDoesNotThrow(() -> {
            inMemoryBuffer.getSinkBufferData();
        });
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}
