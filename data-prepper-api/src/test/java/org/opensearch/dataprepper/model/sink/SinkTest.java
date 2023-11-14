/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

public class SinkTest {
    private static class SinkTestClass implements Sink<Record<?>> {

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void initialize() {
        }

        @Override
        public void output(Collection<Record<?>> records) {
        }
    
    };

    SinkTestClass sink;

    @Test
    public void testSinkUpdateLatencyMetrics() {
        sink = new SinkTestClass();
        sink.updateLatencyMetrics(Collections.emptyList());
    }
}
