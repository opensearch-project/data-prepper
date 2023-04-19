/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.junit.Assert.assertNotNull;
import org.junit.jupiter.api.Test;

class BufferTypeOptionsTest {

    @Test
    void test_notNull() {
        assertNotNull(BufferTypeOptions.LOCALFILE);
    }
}