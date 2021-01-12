package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StringPrepperTests {

    private final String PLUGIN_NAME = "string_converter";

    private final String UPPERCASE_TEST_STRING = "data_prepper";
    private final String LOWERCASE_TEST_STRING = "STRING_CONVERTER";
    private final Record<String> TEST_RECORD_1 = new Record<>(UPPERCASE_TEST_STRING);
    private final Record<String> TEST_RECORD_2 = new Record<>(LOWERCASE_TEST_STRING);
    private final List<Record<String>> TEST_RECORDS = Arrays.asList(TEST_RECORD_1, TEST_RECORD_2);

    @Test
    public void testStringPrepperDefault() {
        final StringPrepper stringPrepper = new StringPrepper(new PluginSetting(PLUGIN_NAME, new HashMap<>()));
        final List<Record<String>> modifiedRecords = (List<Record<String>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<String> modifiedRecordData = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());
        final List<String> expectedRecordData = Arrays.asList(UPPERCASE_TEST_STRING.toUpperCase(), LOWERCASE_TEST_STRING);

        assertEquals(expectedRecordData, modifiedRecordData);
    }

    @Test
    public void testStringPrepperLowerCase() {
        final StringPrepper stringPrepper = new StringPrepper(completePluginSettingForStringPrepper(false));
        final List<Record<String>> modifiedRecords = (List<Record<String>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<String> modifiedRecordData = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());
        final List<String> expectedRecordData = Arrays.asList(UPPERCASE_TEST_STRING, LOWERCASE_TEST_STRING.toLowerCase());

        assertTrue(modifiedRecordData.containsAll(expectedRecordData));
        assertTrue(expectedRecordData.containsAll(modifiedRecordData));
    }

    private PluginSetting completePluginSettingForStringPrepper(final boolean upperCase) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(StringPrepper.UPPER_CASE, upperCase);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
