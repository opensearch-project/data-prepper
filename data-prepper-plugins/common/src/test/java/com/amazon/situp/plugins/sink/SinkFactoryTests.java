package com.amazon.situp.plugins.sink;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.sink.Sink;
import com.amazon.situp.plugins.PluginException;
import org.junit.Test;

import java.util.HashMap;

import static com.amazon.situp.plugins.PluginFactoryTests.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

@SuppressWarnings("rawtypes")
public class SinkFactoryTests {

    /**
     * Tests if SinkFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewSinkClassByNameThatExists() {
        final PluginSetting stdOutSinkConfiguration = new PluginSetting("stdout", new HashMap<>());
        final Sink actualSink = SinkFactory.newSink(stdOutSinkConfiguration);
        final Sink expectedSink = new StdOutSink();
        assertThat(actualSink, notNullValue());
        assertThat(actualSink.getClass().getSimpleName(), is(equalTo(expectedSink.getClass().getSimpleName())));
    }

    /**
     * Tests if SinkFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSinkRetrieval() {
        assertThrows(PluginException.class, () -> SinkFactory.newSink(NON_EXISTENT_EMPTY_CONFIGURATION));
    }

}
