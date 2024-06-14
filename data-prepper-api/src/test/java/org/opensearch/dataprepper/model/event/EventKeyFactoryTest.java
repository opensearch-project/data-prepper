/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventKeyFactoryTest {

    private String keyPath;

    @Mock
    private EventKey eventKey;

    @BeforeEach
    void setUp() {
        keyPath = UUID.randomUUID().toString();
    }

    private EventKeyFactory createObjectUnderTest() {
        return mock(EventKeyFactory.class);
    }

    @Test
    void createEventKey_calls_with_ALL_action() {
        final EventKeyFactory objectUnderTest = createObjectUnderTest();
        when(objectUnderTest.createEventKey(anyString())).thenCallRealMethod();
        when(objectUnderTest.createEventKey(keyPath, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey);

        assertThat(objectUnderTest.createEventKey(keyPath), equalTo(eventKey));
    }
}