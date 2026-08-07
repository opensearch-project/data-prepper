/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EventTypeTest {

    @Test
    public void testEventTypeGetByName_log() {
        final EventType log = EventType.getByName("Log");
        assertThat(log, is(equalTo(EventType.LOG)));
        assertThat(log.toString(), is(equalTo("LOG")));
    }

    @Test
    public void testEventTypeGetByName_trace() {
        final EventType trace = EventType.getByName("trace");
        assertThat(trace, is(equalTo(EventType.TRACE)));
        assertThat(trace.toString(), is(equalTo("TRACE")));
    }

    @Test
    public void testEventTypeGetByName_metric() {
        final EventType metric = EventType.getByName("metric");
        assertThat(metric, is(equalTo(EventType.METRIC)));
        assertThat(metric.toString(), is(equalTo("METRIC")));
    }
}
