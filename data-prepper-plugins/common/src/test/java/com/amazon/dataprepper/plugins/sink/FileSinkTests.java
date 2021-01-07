package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileSinkTests {
    private static String PLUGIN_NAME = "file";

    private File TEST_OUTPUT_FILE;
    private final String TEST_DATA_1 = "data_prepper";
    private final String TEST_DATA_2 = "file_sink";
    private final Record<String> TEST_RECORD_1 = new Record<>(TEST_DATA_1);
    private final Record<String> TEST_RECORD_2 = new Record<>(TEST_DATA_2);
    private final List<Record<String>> TEST_RECORDS = Arrays.asList(TEST_RECORD_1, TEST_RECORD_2);

    @Before
    public void setUp() throws IOException {
        TEST_OUTPUT_FILE = Files.createTempFile("", "output.txt").toFile();
    }

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(TEST_OUTPUT_FILE);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidFilePath() {
        final FileSink fileSink = new FileSink(completePluginSettingForFileSink(""));
        fileSink.output(TEST_RECORDS);
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
