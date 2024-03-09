/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestEventFactoryTest {
    @Mock
    private EventFactory eventFactory;

    private TestEventFactory createObjectUnderTest() {
        return new TestEventFactory(eventFactory);
    }

    @Test
    void eventBuilder_returns_EventBuilder_from_inner() {
        final BaseEventBuilder innerEventBuilder = mock(BaseEventBuilder.class);
        final Class<? extends BaseEventBuilder> inputClass = innerEventBuilder.getClass();
        when(eventFactory.eventBuilder(inputClass))
                .thenReturn(innerEventBuilder);

        assertThat(createObjectUnderTest().eventBuilder(inputClass),
                equalTo(innerEventBuilder));
    }
}