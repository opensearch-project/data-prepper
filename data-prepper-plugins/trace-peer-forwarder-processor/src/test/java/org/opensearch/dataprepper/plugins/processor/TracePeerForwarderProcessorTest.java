/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith(MockitoExtension.class)
class TracePeerForwarderProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Record record;

    private TracePeerForwarderProcessor createObjectUnderTest() {
        return new TracePeerForwarderProcessor(pluginMetrics);
    }

    @Test
    void test_doExecute_should_return_same_records() {
        final TracePeerForwarderProcessor objectUnderTest = createObjectUnderTest();
        final Collection<Record<Event>> testRecords = Collections.singletonList(record);

        final Collection<Record<Event>> expectedRecords = objectUnderTest.doExecute(testRecords);

        assertThat(expectedRecords.size(), equalTo(testRecords.size()));
        assertThat(expectedRecords, equalTo(testRecords));
    }

    @Test
    void test_getIdentificationKeys() {
        final TracePeerForwarderProcessor objectUnderTest = createObjectUnderTest();
        final Collection<String> expectedIdentificationKeys = objectUnderTest.getIdentificationKeys();

        assertThat(expectedIdentificationKeys, CoreMatchers.equalTo(Collections.singleton("traceId")));
    }
}
