package com.amazon.situp.plugins.buffer;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class BlockingBufferTests {
    private static final String ATTRIBUTE_BATCH_SIZE = "batch_size";
    private static final String ATTRIBUTE_BUFFER_SIZE = "buffer_size";
    private static final String TEST_PIPELINE_NAME = "test-pipeline";
    private static final int TEST_BATCH_SIZE = 3;
    private static final int TEST_BUFFER_SIZE = 13;
    private static final int TEST_WRITE_TIMEOUT = 1_00;
    private static final int TEST_BATCH_READ_TIMEOUT = 5_000;

    @Test
    public void testCreationUsingPluginSetting() {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(blockingBuffer, notNullValue());
    }

    @Test
    public void testCreationUsinNullPluginSetting() {
        try{
            new BlockingBuffer<Record<String>>((PluginSetting) null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo("PluginSetting cannot be null")));
        }
    }

    @Test
    public void testCreationUsingDefaultPluginSettings() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(
                BlockingBuffer.getDefaultPluginSettings());
        assertThat(blockingBuffer, notNullValue());
    }

    @Test
    public void testCreationUsingValues() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
    }

    @Test(expected = NullPointerException.class)
    public void testInsertNull() throws TimeoutException {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        blockingBuffer.write(null, TEST_WRITE_TIMEOUT);
    }

    @Test(expected = TimeoutException.class)
    public void testNoEmptySpace() throws TimeoutException {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(1, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        blockingBuffer.write(new Record<>("FILL_THE_BUFFER"), TEST_WRITE_TIMEOUT);
        blockingBuffer.write(new Record<>("TIMEOUT"), TEST_WRITE_TIMEOUT);
    }

    @Test
    public void testReadEmptyBuffer() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        Collection<Record<String>> records = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        assertThat(records.size(), is(0));
    }

    @Test
    public void testBatchRead() throws Exception {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(blockingBuffer, notNullValue());
        final int testSize = 5;
        for (int i = 0; i < testSize; i++) {
            Record<String> record = new Record<>("TEST" + i);
            blockingBuffer.write(record, TEST_WRITE_TIMEOUT);
        }
        Collection<Record<String>> partialRecords = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        final int expectedBatchSize = (Integer) completePluginSetting.getAttributeFromSettings(ATTRIBUTE_BATCH_SIZE);
        assertThat(partialRecords.size(), is(expectedBatchSize));
        int i = 0;
        for (Record<String> record : partialRecords) {
            assertThat(record.getData(), equalTo("TEST" + i));
            i++;
        }
        Collection<Record<String>> finalBatch = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        assertThat(finalBatch.size(), is(testSize - expectedBatchSize));
        for (Record<String> record : finalBatch) {
            assertThat(record.getData(), equalTo("TEST" + i));
            i++;
        }
    }

    private PluginSetting completePluginSettingForBlockingBuffer() {
        final String pluginName = "bounded_blocking";
        final Map<String, Object> settings = new HashMap<>();
        settings.put(ATTRIBUTE_BUFFER_SIZE, TEST_BUFFER_SIZE);
        settings.put(ATTRIBUTE_BATCH_SIZE, TEST_BATCH_SIZE);
        final PluginSetting testSettings =  new PluginSetting(pluginName, settings);
        testSettings.setPipelineName(TEST_PIPELINE_NAME);
        return testSettings;
    }

    private void completelyShutdownExecutorService(final ExecutorService executorService) {

    }
}
