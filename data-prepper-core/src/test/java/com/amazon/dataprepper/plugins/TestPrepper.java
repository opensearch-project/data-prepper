package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import java.util.Collection;

public class TestPrepper implements Prepper<Record<String>, Record<String>> {
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
