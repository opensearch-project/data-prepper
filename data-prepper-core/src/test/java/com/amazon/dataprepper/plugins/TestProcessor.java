package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import java.util.Collection;

public class TestProcessor implements Processor<Record<String>, Record<String>> {
    public boolean isShutdown = false;

    @Override
    public Collection<Record<String>> execute(Collection<Record<String>> records) {
        return null;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
    }
}
