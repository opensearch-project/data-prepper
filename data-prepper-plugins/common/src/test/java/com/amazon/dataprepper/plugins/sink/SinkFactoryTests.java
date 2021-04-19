/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.sink.Sink;
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
