/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileSinkTests {
    private static String PLUGIN_NAME = "file";

    private File TEST_OUTPUT_FILE;
    private final String TEST_DATA_1 = "data_prepper";
    private final String TEST_DATA_2 = "file_sink";
    private final String TEST_KEY = "test_key";
    private final Record<Object> TEST_STRING_RECORD_1 = new Record<>(TEST_DATA_1);
    private final Record<Object> TEST_STRING_RECORD_2 = new Record<>(TEST_DATA_2);
    // TODO: remove with the completion of: https://github.com/opensearch-project/data-prepper/issues/546
    private final List<Record<Object>> TEST_STRING_RECORDS = Arrays.asList(TEST_STRING_RECORD_1, TEST_STRING_RECORD_2);
    private List<Record<Object>> TEST_RECORDS;

    @Before
    public void setUp() throws IOException {
        TEST_OUTPUT_FILE = Files.createTempFile("", "output.txt").toFile();
        TEST_RECORDS = new ArrayList<>();

        TEST_RECORDS.add(new Record<>(JacksonEvent
                .builder()
                .withEventType("event")
                .withData(Map.of(TEST_KEY, TEST_DATA_1))
                .build()));
        TEST_RECORDS.add(new Record<>(JacksonEvent
                .builder()
                .withEventType("event")
                .withData(Map.of(TEST_KEY, TEST_DATA_2))
                .build()));
    }

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(TEST_OUTPUT_FILE);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidFilePath() {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(""));
        fileSink.output(TEST_STRING_RECORDS);
    }

    // TODO: remove with the completion of: https://github.com/opensearch-project/data-prepper/issues/546
    @Test
    public void testValidFilePathStringRecord() throws IOException {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(TEST_OUTPUT_FILE.getPath()));
        fileSink.output(TEST_STRING_RECORDS);
        fileSink.shutdown();

        final String outputData = readDocFromFile(TEST_OUTPUT_FILE);
        Assert.assertTrue(outputData.contains(TEST_DATA_1));
        Assert.assertTrue(outputData.contains(TEST_DATA_2));
    }

    @Test
    public void testValidFilePath() throws IOException {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(TEST_OUTPUT_FILE.getPath()));
        fileSink.output(TEST_RECORDS);
        fileSink.shutdown();

        final String outputData = readDocFromFile(TEST_OUTPUT_FILE);
        Assert.assertTrue(outputData.contains(TEST_DATA_1));
        Assert.assertTrue(outputData.contains(TEST_DATA_2));
    }

    private PluginSetting completePluginSettingForFileSink(final String filepath) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(FileSink.FILE_PATH, filepath);
        return new PluginSetting(PLUGIN_NAME, settings);
    }

    private String readDocFromFile(final File file) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }
}
