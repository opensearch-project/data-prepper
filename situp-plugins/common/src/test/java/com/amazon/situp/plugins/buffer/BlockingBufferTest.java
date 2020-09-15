package com.amazon.situp.plugins.buffer;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class BlockingBufferTest {
    private static final String ATTRIBUTE_BATCH_SIZE = "batch-size";
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
    public void testCreationUsingDefaults() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE);
        assertThat(blockingBuffer, notNullValue());
    }

    @Test(expected = NullPointerException.class)
    public void testInsertNull() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE);
        assertThat(blockingBuffer, notNullValue());
        blockingBuffer.write(null, TEST_WRITE_TIMEOUT);
    }

    @Test
    public void testReadEmptyBuffer() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE);
        assertThat(blockingBuffer, notNullValue());
        Collection<Record<String>> records = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        assertThat(records.size(), is(0));
    }

    @Test
    public void testBatchRead() {
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
        final String pluginName = "bounded-blocking";
        final Map<String, Object> settings = new HashMap<>();
        settings.put("buffer-size", TEST_BUFFER_SIZE);
        settings.put("batch-size", TEST_BATCH_SIZE);
        return new PluginSetting(pluginName, settings);
    }
}
