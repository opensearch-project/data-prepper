package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class StdOutSinkTests {
    private static String PLUGIN_NAME = "stdout";

    private final String TEST_DATA_1 = "data_prepper";
    private final String TEST_DATA_2 = "stdout_sink";
    private final Record<String> TEST_RECORD_1 = new Record<>(TEST_DATA_1);
    private final Record<String> TEST_RECORD_2 = new Record<>(TEST_DATA_2);
    private final List<Record<String>> TEST_RECORDS = Arrays.asList(TEST_RECORD_1, TEST_RECORD_2);

    @Test
    public void testSimple() {
        final StdOutSink stdOutSink = new StdOutSink(new PluginSetting(PLUGIN_NAME, new HashMap<>()));
        stdOutSink.output(TEST_RECORDS);
        stdOutSink.shutdown();
    }
}
