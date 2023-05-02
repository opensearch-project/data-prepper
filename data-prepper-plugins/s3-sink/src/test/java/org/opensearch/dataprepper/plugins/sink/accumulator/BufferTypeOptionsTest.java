/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class BufferTypeOptionsTest {

    @Test
    void notNull_test() {
        assertNotNull(BufferTypeOptions.INMEMORY);
    }

    @Test
    void get_buffer_type_test() {
        assertNotNull(BufferTypeOptions.INMEMORY.getBufferType());
    }

    @Test
    void fromOptionValue_test() {
        BufferTypeOptions bufferTypeOptions = BufferTypeOptions.fromOptionValue("in_memory");
        assertNotNull(bufferTypeOptions);
        assertThat(bufferTypeOptions.toString(), equalTo("INMEMORY"));
    }
}