/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;

import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import org.mockito.Mock;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import java.util.List;
import java.util.Random;

public class DefaultSinkFlushResultTest {
    private List<Event> events;
    @Mock
    private Throwable exception;

    private DefaultSinkFlushResult defaultSinkFlushResult;

    private int statusCode;

    private DefaultSinkFlushResult createObjectUnderTest() {
        return new DefaultSinkFlushResult(events, statusCode, exception);
    }

    @Test
    public void test_basic() {
        exception = mock(Throwable.class);
        events = List.of();
        Random random = new Random();
        statusCode = random.nextInt(500);
        defaultSinkFlushResult = createObjectUnderTest();
        assertThat(defaultSinkFlushResult.getEvents(), sameInstance(events));
        assertThat(defaultSinkFlushResult.getException(), sameInstance(exception));
        assertThat(defaultSinkFlushResult.getStatusCode(), equalTo(statusCode));
    }
}
