package com.amazon.dataprepper.plugins.buffer;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.PluginException;
import org.junit.Test;

import java.util.HashMap;

import static com.amazon.dataprepper.plugins.PluginFactoryTests.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

@SuppressWarnings("rawtypes")
public class BufferFactoryTests {
    private static String TEST_PIPELINE = "test-pipeline";
    /**
     * Tests if BufferFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewBufferClassByNameThatExists() {
        final PluginSetting pluginSetting = new PluginSetting("bounded_blocking", new HashMap<>());
        pluginSetting.setPipelineName(TEST_PIPELINE);
        final Buffer actualBuffer = BufferFactory.newBuffer(pluginSetting);
        final Buffer expectedBuffer = new BlockingBuffer(TEST_PIPELINE);
        assertThat(actualBuffer, notNullValue());
        assertThat(actualBuffer.getClass().getSimpleName(), is(equalTo(expectedBuffer.getClass().getSimpleName())));
    }

    /**
     * Tests if BufferFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSinkRetrieval() {
        assertThrows(PluginException.class, () -> BufferFactory.newBuffer(NON_EXISTENT_EMPTY_CONFIGURATION));
    }
}
