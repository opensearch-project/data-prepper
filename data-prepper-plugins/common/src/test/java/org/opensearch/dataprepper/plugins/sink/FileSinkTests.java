/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileSinkTests {
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

    @BeforeEach
    void setUp() throws IOException {
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

    @AfterEach
    void tearDown() {
        FileUtils.deleteQuietly(TEST_OUTPUT_FILE);
    }

    @Test
    void testInvalidFilePath() {
        assertThrows(RuntimeException.class, () -> new FileSink(completePluginSettingForFileSink("")));
    }

    // TODO: remove with the completion of: https://github.com/opensearch-project/data-prepper/issues/546
    @Test
    void testValidFilePathStringRecord() throws IOException {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(TEST_OUTPUT_FILE.getPath()));
        fileSink.output(TEST_STRING_RECORDS);
        fileSink.shutdown();

        final String outputData = readDocFromFile(TEST_OUTPUT_FILE);
        Assertions.assertTrue(outputData.contains(TEST_DATA_1));
        Assertions.assertTrue(outputData.contains(TEST_DATA_2));
    }

    // TODO: remove with the completion of: https://github.com/opensearch-project/data-prepper/issues/546
    @Test
    void testValidFilePathCustomTypeRecord() throws IOException {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(TEST_OUTPUT_FILE.getPath()));
        final TestObject testObject = new TestObject();
        fileSink.output(Collections.singleton(new Record<>(testObject)));
        fileSink.shutdown();

        final String outputData = readDocFromFile(TEST_OUTPUT_FILE);
        Assertions.assertTrue(outputData.contains(testObject.toString()));
    }

    @Test
    void testValidFilePath() throws IOException {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(TEST_OUTPUT_FILE.getPath()));
        fileSink.output(TEST_RECORDS);
        fileSink.shutdown();

        final String outputData = readDocFromFile(TEST_OUTPUT_FILE);
        Assertions.assertTrue(outputData.contains(TEST_DATA_1));
        Assertions.assertTrue(outputData.contains(TEST_DATA_2));
    }

    @Test
    void testMultipleCallsToOutput() throws IOException {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(TEST_OUTPUT_FILE.getPath()));
        fileSink.output(Collections.singletonList(TEST_RECORDS.get(0)));
        fileSink.output(Collections.singletonList(TEST_RECORDS.get(1)));
        fileSink.shutdown();

        final String outputData = readDocFromFile(TEST_OUTPUT_FILE);
        assertThat(outputData, containsString(TEST_DATA_1));
        assertThat(outputData, containsString(TEST_DATA_2));
    }

    @Test
    void testCallingOutputAfterShutdownDoesNotWrite() throws IOException {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(TEST_OUTPUT_FILE.getPath()));
        fileSink.output(Collections.singletonList(TEST_RECORDS.get(0)));
        fileSink.shutdown();
        fileSink.output(Collections.singletonList(TEST_RECORDS.get(1)));

        final String outputData = readDocFromFile(TEST_OUTPUT_FILE);
        assertThat(outputData, containsString(TEST_DATA_1));
        assertThat(outputData, not(containsString(TEST_DATA_2)));
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

    private static class TestObject {

    }
}
