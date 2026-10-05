/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.osgitestecho;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class OsgiTestEchoProcessorTest {

    @Mock
    private PluginMetrics pluginMetrics;

    private OsgiTestEchoProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new OsgiTestEchoProcessor(pluginMetrics);
    }

    @Test
    void doExecute_returns_same_records() {
        final Record<Event> record = new Record<>(JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap("key", "value"))
                .build());

        final Collection<Record<Event>> result = processor.doExecute(Arrays.asList(record));

        assertThat(result.size(), is(1));
        assertThat(result.iterator().next(), is(record));
    }

    @Test
    void doExecute_with_empty_collection_returns_empty() {
        final Collection<Record<Event>> result = processor.doExecute(Collections.emptyList());
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void isReadyForShutdown_returns_true() {
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void shutdown_does_not_throw() {
        processor.shutdown();
    }

    @Test
    void prepareForShutdown_does_not_throw() {
        processor.prepareForShutdown();
    }
}
