package com.amazon.dataprepper.plugins.processor;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
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
public class PrepperFactoryTests {
    private static String TEST_PIPELINE = "test-pipeline";
    /**
     * Tests if PrepperFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewSinkClassByNameThatExists() {
        final PluginSetting noOpPrepperConfiguration = new PluginSetting("no-op", new HashMap<>());
        noOpPrepperConfiguration.setPipelineName(TEST_PIPELINE);
        final Prepper actualPrepper = PrepperFactory.newPrepper(noOpPrepperConfiguration);
        final Prepper expectedPrepper = new NoOpPrepper();
        assertThat(actualPrepper, notNullValue());
        assertThat(actualPrepper.getClass().getSimpleName(), is(equalTo(expectedPrepper.getClass().getSimpleName())));
    }

    /**
     * Tests if PrepperFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSinkRetrieval() {
        assertThrows(PluginException.class, () -> SinkFactory.newSink(NON_EXISTENT_EMPTY_CONFIGURATION));
    }
}
