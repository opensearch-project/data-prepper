package com.amazon.situp;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.source.Source;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SitupPlugin(name = "test-source", type = PluginType.SOURCE)
public class TestSource implements Source<Record<String>> {
    public static final List<Record<String>> TEST_DATA = Stream.of("THIS", "IS", "TEST", "DATA")
            .map(Record::new).collect(Collectors.toList());
    private boolean isStopRequested;

    public TestSource() {
        isStopRequested = false;
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        final Iterator<Record<String>> iterator = TEST_DATA.iterator();
        while (iterator.hasNext() && !isStopRequested) {
            buffer.write(iterator.next(), 1_000);
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }
}
