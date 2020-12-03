package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.BlockingBuffer;
import com.amazon.dataprepper.plugins.buffer.TestBuffer;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class FileSourceTests {
    private static final String TEST_PIPELINE_NAME = "test-pipeline";
    private static final int TEST_WRITE_TIMEOUT = 100;
    private static final String TEST_FILE_PATH = "src/test/resources/test-file-source.tst";
    private static final String FILE_DOES_NOT_EXIST = "file_does_not_exist";
    private static final String FILE_CONTENT = "THIS IS A TEST";

    @Test
    public void testFileSourceWithEmptyFilePath() {
        try {
            new FileSource("", TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is(equalTo(format("Pipeline [%s] - path is a required attribute for file " +
                    "source", TEST_PIPELINE_NAME))));
        }
    }

    @Test
    public void testFileSourceWithNullFilePath() {
        try {
            new FileSource(null, TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is(equalTo(format("Pipeline [%s] - path is a required attribute for file " +
                    "source", TEST_PIPELINE_NAME))));
        }
    }

    @Test
    public void testFileSourceCreationWithValues() {
        final FileSource fileSource = new FileSource(TEST_FILE_PATH, TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        assertThat(fileSource, notNullValue());
    }

    @Test
    public void testFileSourceCreationWithNullPipelineName() {
        try {
            new FileSource(TEST_FILE_PATH, TEST_WRITE_TIMEOUT, null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo("Pipeline name cannot be null")));
        }
    }

    @Test
    public void testFileSourceCreationUsingPluginSettings() {
        final Map<String, Object> settingMap = ImmutableMap.of(
                "path", TEST_FILE_PATH);
        final PluginSetting pluginSetting = new PluginSetting("file", settingMap);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        final FileSource fileSource = new FileSource(pluginSetting);
        assertThat(fileSource, notNullValue());
    }

    @Test
    public void testFileSourceCreationWithNullPluginSetting() {
        try {
            new FileSource(null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo("PluginSetting cannot be null")));
        }
    }

    @Test
    public void testFileSourceWithNonExistentFile() {
        final FileSource fileSource = new FileSource(FILE_DOES_NOT_EXIST, TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        try {
            fileSource.start(new BlockingBuffer<Record<String>>(TEST_PIPELINE_NAME));
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is(equalTo(format("Pipeline [%s] - Error processing the input file %s",
                    TEST_PIPELINE_NAME, FILE_DOES_NOT_EXIST))));
        }
    }

    @Test
    public void testFileSourceWriteTimeoutException() {
        final Queue<Record<String>> bufferQueue = new LinkedList<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1, true);
        final FileSource fileSource = new FileSource(TEST_FILE_PATH, TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        try {
            fileSource.start(buffer);
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is(equalTo(format("Pipeline [%s] - Error processing the input file %s",
                    TEST_PIPELINE_NAME, TEST_FILE_PATH))));
        }
    }

    @Test
    public void testFileSourceWritingToBuffer() {
        final Queue<Record<String>> bufferQueue = new LinkedList<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1);
        final FileSource fileSource = new FileSource(TEST_FILE_PATH, TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        assertThat(buffer.size(), is(equalTo(0)));
        fileSource.start(buffer);
        assertThat(buffer.size(), is(equalTo(1)));
        final Collection<Record<String>> recordsFromBuffer = buffer.read(TEST_WRITE_TIMEOUT);
        assertThat(recordsFromBuffer.size(), is(equalTo(1)));
        recordsFromBuffer.forEach(actualRecord -> assertThat(actualRecord.getData(), is(equalTo(FILE_CONTENT))));
    }

    @Test
    public void testFileSourceStartAfterStop() {
        final Queue<Record<String>> bufferQueue = new LinkedList<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1);
        final FileSource fileSource = new FileSource(TEST_FILE_PATH, TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        assertThat(buffer.size(), is(equalTo(0)));
        fileSource.stop();
        fileSource.start(buffer); //should not write any records to buffer
        assertThat(buffer.size(), is(equalTo(0)));
    }

    @Test
    public void testFileSourceWithNullBuffer() {
        final FileSource fileSource = new FileSource(TEST_FILE_PATH, TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        try {
            fileSource.start(null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo(format("Pipeline [%s] - buffer cannot be null for file source to start",
                    TEST_PIPELINE_NAME))));
        }
    }

}
