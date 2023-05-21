/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

class InMemoryBufferFactoryTest {

    @Test
    void test_inMemoryBufferFactory_notNull(){
        InMemoryBufferFactory inMemoryBufferFactory = new InMemoryBufferFactory();
        Assertions.assertNotNull(inMemoryBufferFactory);
    }

    @Test
    void test_buffer_notNull(){
        InMemoryBufferFactory inMemoryBufferFactory = new InMemoryBufferFactory();
        Assertions.assertNotNull(inMemoryBufferFactory);
        Buffer buffer = inMemoryBufferFactory.getBuffer();
        Assertions.assertNotNull(buffer);
        assertThat(buffer, instanceOf(Buffer.class));
    }
}