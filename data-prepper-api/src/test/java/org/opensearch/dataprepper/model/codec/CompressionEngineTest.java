/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.codec;

import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import org.mockito.invocation.InvocationOnMock;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;

public class CompressionEngineTest {
    @Test
    void defaultCompressionEngineTest() {
        CompressionEngine compressionEngine = mock(CompressionEngine.class, InvocationOnMock::callRealMethod);
        byte[] bytes = "test".getBytes(StandardCharsets.UTF_8);
        assertThrows(RuntimeException.class, () -> compressionEngine.compress(bytes));
    }
}
