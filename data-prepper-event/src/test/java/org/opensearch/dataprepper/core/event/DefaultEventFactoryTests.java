/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class DefaultEventFactoryTests {
    private DefaultEventFactory eventFactory;
    private List<DefaultEventBuilderFactory> factories;
    @Mock
    private DefaultEventBuilderFactory factory1;
    @Mock
    private DefaultEventBuilderFactory factory2;
    @Mock
    DefaultBaseEventBuilder builder1;
    @Mock
    DefaultBaseEventBuilder builder2;

    private Class class1;
    private Class class2;

    private DefaultEventFactory createObjectUnderTest() {
        return new DefaultEventFactory(factories);
    }

    @BeforeEach
    void setup() {
        class1 = Object.class;
        factory1 = mock(DefaultEventBuilderFactory.class);
        lenient().when(factory1.getEventClass()).thenReturn(class1);
        builder1 = mock(DefaultBaseEventBuilder.class);
        lenient().when(factory1.createNew()).thenReturn(builder1);

        class2 = Class.class;
        factory2 = mock(DefaultEventBuilderFactory.class);
        lenient().when(factory2.getEventClass()).thenReturn(class2);
        builder2 = mock(DefaultBaseEventBuilder.class);
        lenient().when(factory2.createNew()).thenReturn(builder2);

        factories = new ArrayList<>();
        factories.add(factory1);
        factories.add(factory2);
        eventFactory = createObjectUnderTest();
    }

    @Test
    void testDefaultEventFactory() {
        assertThat(eventFactory.eventBuilder(class1), equalTo(builder1));
        assertThat(eventFactory.eventBuilder(class2), equalTo(builder2));
    }

    @Test
    void testInvalidEventBuilderCalss() throws UnsupportedOperationException {
        assertThrows(UnsupportedOperationException.class, () -> eventFactory.eventBuilder(BaseEventBuilder.class));
    }
}
