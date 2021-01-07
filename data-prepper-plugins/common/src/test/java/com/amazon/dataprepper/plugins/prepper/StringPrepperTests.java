package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StringPrepperTests {

    private final String PLUGIN_NAME = "string_converter";

    private final String TEST_DATA_1 = "data_prepper";
    private final String TEST_DATA_2 = "STRING_CONVERTER";
    private final Record<String> TEST_RECORD_1 = new Record<>(TEST_DATA_1);
    private final Record<String> TEST_RECORD_2 = new Record<>(TEST_DATA_2);
    private final List<Record<String>> TEST_RECORDS = Arrays.asList(TEST_RECORD_1, TEST_RECORD_2);

    @Test
    public void testStringPrepperDefault() {
        final StringPrepper stringPrepper = new StringPrepper(new PluginSetting(PLUGIN_NAME, new HashMap<>()));
        final List<Record<String>> modifiedRecords = (List<Record<String>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<String> modifiedRecordData = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());
        final List<String> expectedRecordData = Arrays.asList(TEST_DATA_1.toUpperCase(), TEST_DATA_2);

        Assert.assertTrue(modifiedRecordData.containsAll(expectedRecordData));
        Assert.assertTrue(expectedRecordData.containsAll(modifiedRecordData));
    }

    @Test
    public void testStringPrepperLowerCase() {
        final StringPrepper stringPrepper = new StringPrepper(completePluginSettingForStringPrepper(false));
        final List<Record<String>> modifiedRecords = (List<Record<String>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<String> modifiedRecordData = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());
        final List<String> expectedRecordData = Arrays.asList(TEST_DATA_1, TEST_DATA_2.toLowerCase());

        Assert.assertTrue(modifiedRecordData.containsAll(expectedRecordData));
        Assert.assertTrue(expectedRecordData.containsAll(modifiedRecordData));
    }

    private PluginSetting completePluginSettingForStringPrepper(final boolean upperCase) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(StringPrepper.UPPER_CASE, upperCase);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
