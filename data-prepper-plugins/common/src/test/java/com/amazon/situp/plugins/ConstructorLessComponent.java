package com.amazon.situp.plugins;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.source.Source;

import java.util.concurrent.TimeoutException;

@SitupPlugin(name = "junit-test", type = PluginType.SOURCE)
public class ConstructorLessComponent implements Source<Record<String>> {

    @Override
    public void start(Buffer<Record<String>> buffer) {
        try {
            buffer.write(new Record<>("Junit Testing"), 1_000);
        } catch (TimeoutException ex) {
            throw new RuntimeException("Timed out writing to buffer");
        }
    }

    @Override
    public void stop() {
        //no op
    }
}
