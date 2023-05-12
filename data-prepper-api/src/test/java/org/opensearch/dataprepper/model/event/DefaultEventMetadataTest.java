/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultEventMetadataTest {

    private EventMetadata eventMetadata;

    private String testEventType;

    private Instant testTimeReceived;

    private Map<String, Object> testAttributes;

    @BeforeEach
    public void setup() {
        testAttributes = new HashMap<>();
        testAttributes.put(UUID.randomUUID().toString(), UUID.randomUUID());
        testAttributes.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        testTimeReceived = Instant.now();

        testEventType = UUID.randomUUID().toString();

        eventMetadata = DefaultEventMetadata.builder()
                .withAttributes(testAttributes)
                .withEventType(testEventType)
                .withTimeReceived(testTimeReceived)
                .build();
    }

    @Test
    public void testGetEventType() {
        final String eventType = eventMetadata.getEventType();
        assertThat(eventType, is(not(emptyOrNullString())));
        assertThat(eventType, is(equalTo(testEventType)));
    }

    @Test
    public void testGetTimeReceived() {
        final Instant timeReceived = eventMetadata.getTimeReceived();
        assertThat(timeReceived, is(notNullValue()));
        assertThat(timeReceived, is(equalTo(testTimeReceived)));
    }

    @Test
    public void testGetAttributes() {
        final Map<String, Object> attributes = eventMetadata.getAttributes();

        assertThat(attributes, is(not(anEmptyMap())));
        assertThat(attributes, is(equalTo(testAttributes)));
    }

    @Test
    public void testAttributesMutation_throwsAnException() {
        final Map<String, Object> attributes = eventMetadata.getAttributes();

        assertThrows(UnsupportedOperationException.class, () -> attributes.put("foo", "bar"));
    }

    @Test
    public void testAttributesMutation_without_attributes_throwsAnException() {
        eventMetadata = DefaultEventMetadata.builder()
                .withEventType(testEventType)
                .withTimeReceived(testTimeReceived)
                .build();
        final Map<String, Object> attributes = eventMetadata.getAttributes();

        assertThrows(UnsupportedOperationException.class, () -> attributes.put("foo", "bar"));
    }

    @Test
    public void testAttributes_without_attributes_is_empty() {
        eventMetadata = DefaultEventMetadata.builder()
                .withEventType(testEventType)
                .withTimeReceived(testTimeReceived)
                .build();
        final Map<String, Object> attributes = eventMetadata.getAttributes();
        assertThat(attributes, notNullValue());
        assertThat(attributes.size(), equalTo(0));

        assertThrows(UnsupportedOperationException.class, () -> attributes.put("foo", "bar"));
    }

    @Test
    public void testBuild_withoutTimeReceived() {

        final Instant before = Instant.now();

        final EventMetadata result = DefaultEventMetadata.builder()
                .withEventType(testEventType)
                .build();

        assertThat(result, notNullValue());
        final Instant timeReceived = result.getTimeReceived();
        assertThat(timeReceived, notNullValue());
        assertThat(timeReceived, is(greaterThanOrEqualTo(before)));
        assertThat(timeReceived, is(lessThanOrEqualTo(Instant.now())));
    }

    @Test
    public void testBuild_withoutMap() {

        final EventMetadata result = DefaultEventMetadata.builder()
                .withEventType(testEventType)
                .build();

        assertThat(result, notNullValue());

        final Map<String, Object> attributes = result.getAttributes();
        assertThat(attributes, notNullValue());
        assertThat(attributes, is(anEmptyMap()));
    }

    @Test
    public void testBuild_withoutEventType_throwsAnException() {
        final DefaultEventMetadata.Builder builder = DefaultEventMetadata.builder();
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuild_withoutEmptyEventType_throwsAnException() {
        final DefaultEventMetadata.Builder builder = DefaultEventMetadata.builder()
                .withEventType("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void fromEventMetadata_returns_matching_EventMetadata() {
        final EventMetadata originalMetadata = mock(EventMetadata.class);

        final String eventType = UUID.randomUUID().toString();
        final Instant timeReceived = Instant.now();
        final Map<String, Object> attributes = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(originalMetadata.getEventType()).thenReturn(eventType);
        when(originalMetadata.getTimeReceived()).thenReturn(timeReceived);
        when(originalMetadata.getAttributes()).thenReturn(attributes);

        final EventMetadata copiedMetadata = DefaultEventMetadata.fromEventMetadata(originalMetadata);

        assertThat(copiedMetadata, notNullValue());
        assertThat(copiedMetadata.getEventType(), equalTo(eventType));
        assertThat(copiedMetadata.getTimeReceived(), equalTo(timeReceived));
        assertThat(copiedMetadata.getAttributes(), equalTo(attributes));
        assertThat(copiedMetadata.getAttributes(), not(sameInstance(attributes)));
    }

    @Test
    public void testBuild_withTags() {
        final String testEventType = UUID.randomUUID().toString();

        final Set<String> testTags = Set.of("tag1", "tag2");
        final EventMetadata result = DefaultEventMetadata.builder()
                .withEventType(testEventType)
                .withTags(testTags)
                .build();

        assertThat(result, notNullValue());

        assertThat(result.getTags(), equalTo(testTags));
        assertFalse(result.hasTags(List.of("tag3")));
        assertFalse(result.hasTags(List.of("tag3", "tag1")));
        result.addTag("tag3");
        assertTrue(result.hasTags(List.of("tag1")));
        assertTrue(result.hasTags(List.of("tag1", "tag2")));
        assertTrue(result.hasTags(List.of("tag1", "tag2", "tag3")));
        assertFalse(result.hasTags(List.of("notPresentTag")));
        assertFalse(result.hasTags(List.of("notPresentTag1", "notPresentTag2")));
        assertFalse(result.hasTags(List.of("tag1", "notPresentTag")));
        assertFalse(result.hasTags(List.of("tag1", "tag2", "notPresentTag")));
    }

    @Nested
    class EqualsAndHashCodeAndToString {
        private String eventType;
        private Instant timeReceived;
        private String attributeKey;
        private String attributeValue;
        private DefaultEventMetadata event;

        @BeforeEach
        void setUp() {
            eventType = UUID.randomUUID().toString();
            timeReceived = Instant.now();
            attributeKey = UUID.randomUUID().toString();
            attributeValue = UUID.randomUUID().toString();

            event = DefaultEventMetadata.builder()
                    .withEventType(eventType)
                    .withTimeReceived(timeReceived)
                    .withAttributes(Collections.singletonMap(attributeKey, attributeValue))
                    .build();

        }

        @Test
        void equals_returns_false_for_null() {
            assertThat(event.equals(null), equalTo(false));
        }

        @Test
        void equals_on_same_instance_returns_true() {
            assertThat(event, equalTo(event));
        }

        @Test
        void equals_returns_true_for_two_instances_with_same_value() {
            final DefaultEventMetadata otherEvent = DefaultEventMetadata.builder()
                    .withEventType(eventType)
                    .withTimeReceived(timeReceived)
                    .withAttributes(Collections.singletonMap(attributeKey, attributeValue))
                    .build();

            assertThat(event, equalTo(otherEvent));
        }

        @Test
        void hashCode_are_equal_for_two_instances_with_same_value() {
            final DefaultEventMetadata otherEvent = DefaultEventMetadata.builder()
                    .withEventType(eventType)
                    .withTimeReceived(timeReceived)
                    .withAttributes(Collections.singletonMap(attributeKey, attributeValue))
                    .build();

            assertThat(event.hashCode(), equalTo(otherEvent.hashCode()));
        }

        @Test
        void equals_returns_false_for_two_instances_with_different_eventType() {
            final DefaultEventMetadata otherEvent = DefaultEventMetadata.builder()
                    .withEventType(UUID.randomUUID().toString())
                    .withTimeReceived(timeReceived)
                    .withAttributes(Collections.singletonMap(attributeKey, attributeValue))
                    .build();

            assertThat(event, not(equalTo(otherEvent)));
        }

        @Test
        void toString_has_all_values() {
            final String string = event.toString();

            assertThat(string, notNullValue());
            assertThat(string, allOf(
                    containsString("DefaultEventMetadata"),
                    containsString(eventType),
                    containsString(timeReceived.toString()),
                    containsString(attributeKey),
                    containsString(attributeValue)
            ));
        }
    }
}
