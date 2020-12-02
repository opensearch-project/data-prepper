package com.amazon.dataprepper.plugins.processor;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.plugins.PluginException;
import com.amazon.dataprepper.plugins.sink.SinkFactory;
import org.junit.Test;

import java.util.HashMap;

import static com.amazon.dataprepper.plugins.PluginFactoryTests.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

@SuppressWarnings("rawtypes")
public class ProcessorFactoryTests {
    private static String TEST_PIPELINE = "test-pipeline";
    /**
     * Tests if ProcessorFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewSinkClassByNameThatExists() {
        final PluginSetting noOpProcessorConfiguration = new PluginSetting("no-op", new HashMap<>());
        noOpProcessorConfiguration.setPipelineName(TEST_PIPELINE);
        final Processor actualProcessor = ProcessorFactory.newProcessor(noOpProcessorConfiguration);
        final Processor expectedProcessor = new NoOpProcessor();
        assertThat(actualProcessor, notNullValue());
        assertThat(actualProcessor.getClass().getSimpleName(), is(equalTo(expectedProcessor.getClass().getSimpleName())));
    }

    /**
     * Tests if ProcessorFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSinkRetrieval() {
        assertThrows(PluginException.class, () -> SinkFactory.newSink(NON_EXISTENT_EMPTY_CONFIGURATION));
    }
}
