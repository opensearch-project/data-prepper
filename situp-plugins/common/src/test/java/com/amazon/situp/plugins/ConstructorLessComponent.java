package com.amazon.situp.plugins;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.source.Source;

@SitupPlugin(name = "junit-test", type = PluginType.SOURCE)
public class ConstructorLessComponent implements Source<Record<String>> {

    @Override
    public void start(Buffer<Record<String>> buffer) {
        buffer.write(new Record<>("Junit Testing"), 1_000);
    }

    @Override
    public void stop() {
        //no op
    }
}
