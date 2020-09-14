package com.amazon.situp.plugins.buffer;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class BlockingBufferTest {
    private static final String ATTRIBUTE_BATCH_SIZE = "batch-size";
    private static final int TEST_BATCH_SIZE = 3;
    private static final int TEST_BUFFER_SIZE = 13;
    private static final int TEST_TIMEOUT = 5_000;

    @Test
    public void testBlockingBufferCreationUsingPluginSetting() {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(blockingBuffer, CoreMatchers.notNullValue());
    }

    @Test
    public void testBlockingBufferCreationUsingDefault() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_TIMEOUT, TEST_BATCH_SIZE);
        assertThat(blockingBuffer, CoreMatchers.notNullValue());
    }

    @Test
    public void testBlockingBufferReadAndWrite() {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(blockingBuffer, CoreMatchers.notNullValue());
        final Record<String> expected = new Record<>("TEST");
        blockingBuffer.write(expected);
        final Record<String> actual = blockingBuffer.read();
        assertThat(actual, is(expected));
    }

    @Test
    public void testBlockingBufferBatchRead() {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(blockingBuffer, CoreMatchers.notNullValue());
        final int testSize = 5;
        for (int i = 0; i < testSize; i++) {
            Record<String> record = new Record<>("TEST" + i);
            blockingBuffer.write(record);
        }
        Collection<Record<String>> partialRecords = blockingBuffer.readBatch();
        final int expectedBatchSize = (Integer) completePluginSetting.getAttributeFromSettings(ATTRIBUTE_BATCH_SIZE);
        assertThat(partialRecords.size(), is(expectedBatchSize));
        int i = 0;
        for (Record<String> record : partialRecords) {
            assertThat(record.getData(), equalTo("TEST" + i));
            i++;
        }
        Collection<Record<String>> finalBatch = blockingBuffer.readBatch();
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
        settings.put("timeout", TEST_TIMEOUT);
        settings.put("batch-size", TEST_BATCH_SIZE);
        return new PluginSetting(pluginName, settings);
    }
}
