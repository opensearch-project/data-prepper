package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import java.util.Collection;

@SingleThread
@DataPrepperPlugin(name = "test_prepper", type = PluginType.PREPPER)
public class TestPrepper implements Prepper<Record<String>, Record<String>> {
    public boolean isShutdown = false;

    public TestPrepper(final PluginSetting pluginSetting) {}

    @Override
    public Collection<Record<String>> execute(Collection<Record<String>> records) {
        return null;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
    }
}
