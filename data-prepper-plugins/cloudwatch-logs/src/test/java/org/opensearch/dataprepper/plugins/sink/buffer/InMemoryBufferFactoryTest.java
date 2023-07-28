/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.buffer;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.typeCompatibleWith;

class InMemoryBufferFactoryTest {
    @Test
    void check_buffer_not_null() {
        Buffer buffer = new InMemoryBufferFactory().getBuffer();
        assertThat(buffer, notNullValue());
        assertThat(buffer.getClass(), typeCompatibleWith(Buffer.class));
    }
}
