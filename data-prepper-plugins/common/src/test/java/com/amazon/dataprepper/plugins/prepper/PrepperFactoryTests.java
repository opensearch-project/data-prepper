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

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.plugins.PluginException;
import com.amazon.dataprepper.plugins.sink.SinkFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static com.amazon.dataprepper.plugins.PluginFactoryTests.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@SuppressWarnings("rawtypes")
public class PrepperFactoryTests {
    private static String TEST_PIPELINE = "test-pipeline";

    @AfterEach
    void cleanUp() {
        PrepperFactory.dangerousMethod_setPluginFunction(null);
    }

    /**
     * Tests if PrepperFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewSingletonPrepperClassByNameThatExists() {
        PrepperFactory.dangerousMethod_setPluginFunction((s, c) -> new NoOpPrepper<>());

        final PluginSetting noOpPrepperConfiguration = new PluginSetting("no-op", new HashMap<>());
        noOpPrepperConfiguration.setPipelineName(TEST_PIPELINE);
        final List<Prepper> actualPrepperSets = PrepperFactory.newPreppers(noOpPrepperConfiguration);
        assertEquals(1, actualPrepperSets.size());
        final Prepper actualPrepper = actualPrepperSets.get(0);
        final Prepper expectedPrepper = new NoOpPrepper();
        assertThat(actualPrepper, notNullValue());
        assertThat(actualPrepper.getClass().getSimpleName(), is(equalTo(expectedPrepper.getClass().getSimpleName())));
    }

    @Test
    public void testNewMultiInstancePrepperClassByNameThatExists() {
        final PluginSetting testPrepperConfiguration = new PluginSetting("test_prepper", new HashMap<>());
        testPrepperConfiguration.setProcessWorkers(2);
        testPrepperConfiguration.setPipelineName(TEST_PIPELINE);
        final List<Prepper> actualPrepperSets = PrepperFactory.newPreppers(testPrepperConfiguration);
        assertEquals(2, actualPrepperSets.size());
        final Prepper expectedPrepper = new TestPrepper(testPrepperConfiguration);
        assertThat(actualPrepperSets.get(0), notNullValue());
        assertThat(actualPrepperSets.get(0).getClass().getSimpleName(), is(equalTo(expectedPrepper.getClass().getSimpleName())));
        assertThat(actualPrepperSets.get(1), notNullValue());
        assertThat(actualPrepperSets.get(1).getClass().getSimpleName(), is(equalTo(expectedPrepper.getClass().getSimpleName())));
    }

    /**
     * Tests if PrepperFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentPrepperRetrieval() {
        assertThrows(PluginException.class, () -> SinkFactory.newSink(NON_EXISTENT_EMPTY_CONFIGURATION));
    }
}
