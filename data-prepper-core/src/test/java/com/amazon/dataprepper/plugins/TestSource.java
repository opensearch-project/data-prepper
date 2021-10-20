/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DataPrepperPlugin(name = "test_source", pluginType = Source.class)
public class TestSource implements Source<Record<String>> {
    public static final List<Record<String>> TEST_DATA = Stream.of("TEST")
            .map(Record::new).collect(Collectors.toList());
    private boolean isStopRequested;
    private boolean failSourceForTest;

    public TestSource() {
        this.isStopRequested = false;
        this.failSourceForTest = false;
    }

    public TestSource(final boolean failSourceForTest) {
        this.isStopRequested = false;
        this.failSourceForTest = failSourceForTest;
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        if(failSourceForTest) {
            throw new RuntimeException("Source is expected to fail");
        }
        final Iterator<Record<String>> iterator = TEST_DATA.iterator();
            while (iterator.hasNext() && !isStopRequested) {
                try {
                    buffer.write(iterator.next(), 1_000);
                } catch (TimeoutException e) {
                    throw new RuntimeException("Timed out writing to buffer");
                }
            }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }
}
