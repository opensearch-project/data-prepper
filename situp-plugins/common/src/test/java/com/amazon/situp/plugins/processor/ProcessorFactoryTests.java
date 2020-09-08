package com.amazon.situp.plugins.processor;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.plugins.PluginException;
import com.amazon.situp.plugins.sink.SinkFactory;
import org.junit.Test;

import java.util.HashMap;

import static com.amazon.situp.plugins.PluginFactoryTests.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

@SuppressWarnings("rawtypes")
public class ProcessorFactoryTests {

    /**
     * Tests if ProcessorFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewSinkClassByNameThatExists() {
        final PluginSetting noOpProcessorConfiguration = new PluginSetting("no-op", new HashMap<>());
        final Processor actualProcessor = ProcessorFactory.newProcessor(noOpProcessorConfiguration);
        final Processor expectedProcessor = new NoOpProcessor();
        assertNotNull(actualProcessor);
        assertEquals(expectedProcessor.getClass().getSimpleName(), actualProcessor.getClass().getSimpleName());
    }

    /**
     * Tests if ProcessorFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSinkRetrieval() {
        assertThrows(PluginException.class, () -> SinkFactory.newSink(NON_EXISTENT_EMPTY_CONFIGURATION));
    }
}
