/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DataPrepperPlugin(name = "test-source", pluginType = Source.class)
public class TestSource implements Source<Record<String>> {
    public static final List<Record<String>> TEST_DATA = Stream.of("THIS", "IS", "TEST", "DATA")
            .map(Record::new).collect(Collectors.toList());
    private static final int WRITE_TIMEOUT = 5_000;
    private boolean isStopRequested;

    public TestSource() {
        isStopRequested = false;
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        final Iterator<Record<String>> iterator = TEST_DATA.iterator();
        while (iterator.hasNext() && !isStopRequested) {
            try {
                buffer.write(iterator.next(), WRITE_TIMEOUT);
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
