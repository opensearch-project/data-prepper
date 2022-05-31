/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class S3SourceTest {
    private final String PLUGIN_NAME = "s3";
    private final String TEST_PIPELINE_NAME = "test_pipeline";

    private S3Source s3Source;
    private BlockingBuffer<Record<Event>> testBuffer;
    private PluginMetrics pluginMetrics;
    private S3SourceConfig s3SourceConfig;


    @BeforeEach
    void setUp() {
        testBuffer = getBuffer();
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        s3SourceConfig = mock(S3SourceConfig.class);

        s3Source = new S3Source(pluginMetrics, s3SourceConfig);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null() {
        testBuffer = null;
        assertThrows(IllegalStateException.class, () -> s3Source.start(testBuffer));
    }

    private BlockingBuffer<Record<Event>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap) {{
            setPipelineName(TEST_PIPELINE_NAME);
        }};
        return new BlockingBuffer<>(pluginSetting);
    }
}