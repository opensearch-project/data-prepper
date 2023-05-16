/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

class LocalFileBufferFactoryTest {
    @Test
    void test_localFileBufferFactory_notNull() {
        LocalFileBufferFactory localFileBufferFactory = new LocalFileBufferFactory();
        Assertions.assertNotNull(localFileBufferFactory);
    }

    @Test
    void test_buffer_notNull() {
        LocalFileBufferFactory localFileBufferFactory = new LocalFileBufferFactory();
        Assertions.assertNotNull(localFileBufferFactory);
        Buffer buffer = localFileBufferFactory.getBuffer();
        Assertions.assertNotNull(buffer);
        assertThat(buffer, instanceOf(LocalFileBuffer.class));
    }
}