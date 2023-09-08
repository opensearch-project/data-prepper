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

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {

    public static final int MAX_EVENTS = 55;

    private InMemoryBuffer inMemoryBuffer;


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