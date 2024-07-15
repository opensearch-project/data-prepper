/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventKeyDeserializerTest {

    @Mock
    private EventKeyFactory eventKeyFactory;

    @Mock
    private DeserializationContext deserializationContext;
    @Mock
    private BeanProperty property;
    @Mock(lenient = true)
    private JsonParser parser;
    @Mock
    private EventKey eventKey;

    private String eventKeyString;

    @BeforeEach
    void setUp() throws IOException {
        eventKeyString = UUID.randomUUID().toString();

        when(parser.getValueAsString()).thenReturn(eventKeyString);
    }

    private EventKeyDeserializer createObjectUnderTest() {
        return new EventKeyDeserializer(eventKeyFactory);
    }

    @Test
    void createContextual_returns_EventKeyDeserializer_that_deserializes_with_ALL_when_no_BeanProperty() throws IOException {
        when(eventKeyFactory.createEventKey(eventKeyString, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey);
        final JsonDeserializer<?> contextualDeserializer = createObjectUnderTest().createContextual(deserializationContext, null);
        assertThat(contextualDeserializer, notNullValue());
        assertThat(contextualDeserializer.deserialize(parser, deserializationContext), equalTo(eventKey));
    }

    @Test
    void createContextual_returns_EventKeyDeserializer_that_deserializes_with_ALL_when_no_annotation() throws IOException {
        when(eventKeyFactory.createEventKey(eventKeyString, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey);
        final JsonDeserializer<?> contextualDeserializer = createObjectUnderTest().createContextual(deserializationContext, property);
        assertThat(contextualDeserializer, notNullValue());
        assertThat(contextualDeserializer.deserialize(parser, deserializationContext), equalTo(eventKey));
    }

    @Test
    void createContextual_returns_same_EventKeyDeserializer_as_self_when_no_BeanProperty() {
        final EventKeyDeserializer objectUnderTest = createObjectUnderTest();
        final JsonDeserializer<?> contextualDeserializer = objectUnderTest.createContextual(deserializationContext, null);
        assertThat(contextualDeserializer, sameInstance(objectUnderTest));
    }

    @Test
    void createContextual_returns_same_EventKeyDeserializer_as_self_when_no_annotation() {
        final EventKeyDeserializer objectUnderTest = createObjectUnderTest();
        final JsonDeserializer<?> contextualDeserializer = objectUnderTest.createContextual(deserializationContext, property);
        assertThat(contextualDeserializer, sameInstance(objectUnderTest));
    }

    @ParameterizedTest
    @EnumSource(value = EventKeyFactory.EventAction.class)
    void createContextual_returns_EventKeyDeserializer_that_deserializes_with_action_from_annotated_Event(final EventKeyFactory.EventAction eventAction) throws IOException {
        final EventKeyConfiguration eventKeyConfiguration = mock(EventKeyConfiguration.class);
        when(eventKeyConfiguration.value()).thenReturn(new EventKeyFactory.EventAction[] { eventAction });
        when(property.getAnnotation(EventKeyConfiguration.class)).thenReturn(eventKeyConfiguration);
        when(eventKeyFactory.createEventKey(eventKeyString, eventAction)).thenReturn(eventKey);

        final JsonDeserializer<?> contextualDeserializer = createObjectUnderTest().createContextual(deserializationContext, property);

        assertThat(contextualDeserializer, notNullValue());
        assertThat(contextualDeserializer.deserialize(parser, deserializationContext), equalTo(eventKey));
    }

    @Test
    void createContextual_returns_EventKeyDeserializer_that_deserializes_with_action_from_annotated_Event_when_multiple() throws IOException {
        final EventKeyConfiguration eventKeyConfiguration = mock(EventKeyConfiguration.class);
        when(eventKeyConfiguration.value()).thenReturn(new EventKeyFactory.EventAction[] { EventKeyFactory.EventAction.PUT, EventKeyFactory.EventAction.DELETE });
        when(property.getAnnotation(EventKeyConfiguration.class)).thenReturn(eventKeyConfiguration);
        when(eventKeyFactory.createEventKey(eventKeyString, EventKeyFactory.EventAction.PUT, EventKeyFactory.EventAction.DELETE)).thenReturn(eventKey);

        final JsonDeserializer<?> contextualDeserializer = createObjectUnderTest().createContextual(deserializationContext, property);

        assertThat(contextualDeserializer, notNullValue());
        assertThat(contextualDeserializer.deserialize(parser, deserializationContext), equalTo(eventKey));
    }

    @Nested
    class UsingRealObjectMapper {
        private ObjectMapper objectMapper;

        @BeforeEach
        void setUp() {
            objectMapper = new ObjectMapper();

            final SimpleModule simpleModule = new SimpleModule();
            simpleModule.addDeserializer(EventKey.class, createObjectUnderTest());
            objectMapper.registerModule(simpleModule);
        }

        @Test
        void quick() {
            when(eventKeyFactory.createEventKey(eventKeyString, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey);

            assertThat(objectMapper.convertValue(eventKeyString, EventKey.class),
                    equalTo(eventKey));
        }
    }
}