package com.amazon.dataprepper.model.event;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    public void testBuild_withoutTimeReceived() {

        final Instant before = Instant.now();

        EventMetadata result = DefaultEventMetadata.builder()
                .withEventType(testEventType)
                .build();

        assertThat(result, notNullValue());
        final Instant timeReceived = result.getTimeReceived();
        assertThat(timeReceived, notNullValue());
        assertThat(timeReceived, is(greaterThan(before)));
        assertThat(timeReceived, is(lessThan(Instant.now())));
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
}
