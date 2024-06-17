/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestEventKeyFactoryTest {

    @Mock
    private EventKeyFactory innerEventKeyFactory;

    @Mock
    private EventKey eventKey;

    private TestEventKeyFactory createObjectUnderTest() {
        return new TestEventKeyFactory(innerEventKeyFactory);
    }

    @Test
    void createEventKey_returns_from_inner_EventKeyFactory() {
        final String keyPath = UUID.randomUUID().toString();
        when(innerEventKeyFactory.createEventKey(keyPath, EventKeyFactory.EventAction.ALL))
                .thenReturn(eventKey);

        assertThat(createObjectUnderTest().createEventKey(keyPath),
                equalTo(eventKey));
    }

    @ParameterizedTest
    @EnumSource(EventKeyFactory.EventAction.class)
    void createEventKey_with_Actions_returns_from_inner_EventKeyFactory(final EventKeyFactory.EventAction eventAction) {
        final String keyPath = UUID.randomUUID().toString();
        when(innerEventKeyFactory.createEventKey(keyPath, eventAction))
                .thenReturn(eventKey);

        assertThat(createObjectUnderTest().createEventKey(keyPath, eventAction),
                equalTo(eventKey));
    }
}