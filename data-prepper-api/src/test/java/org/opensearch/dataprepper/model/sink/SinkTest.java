/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.sink;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doCallRealMethod;

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
        public void setFailurePipeline(HeadlessPipeline failurePipeline) {
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

    @Test
    public void testSetFailurePipeline() {
        Sink testSink = mock(Sink.class);
        HeadlessPipeline failurePipeline = mock(HeadlessPipeline.class);
        doCallRealMethod().when(testSink).setFailurePipeline(failurePipeline);
        testSink.setFailurePipeline(failurePipeline);
    }
}
