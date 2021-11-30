package com.amazon.dataprepper.pipeline.common;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;

@SingleThread
@DataPrepperPlugin(name = "test_processor", pluginType = Prepper.class)
public class TestPrepper extends TestProcessor implements Prepper<Record<String>, Record<String>> {
    public TestPrepper(PluginSetting pluginSetting) {
        super(pluginSetting);
    }
}
