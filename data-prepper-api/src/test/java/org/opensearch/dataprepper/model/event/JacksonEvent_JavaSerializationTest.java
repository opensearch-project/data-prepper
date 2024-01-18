/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

class JacksonEvent_JavaSerializationTest {

    private ObjectOutputStream objectOutputStream;
    private ByteArrayOutputStream byteArrayOutputStream;

    @BeforeEach
    void setUp() throws IOException {
        byteArrayOutputStream = new ByteArrayOutputStream(1000);
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    }

    private JacksonEvent createObjectUnderTest() {
        return JacksonEvent.builder()
                .withEventType("TEST")
                .withData(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build();
    }

    @Test
    void serialize_without_acknowledgementSet_includes_data() throws IOException, ClassNotFoundException {
        final JacksonEvent objectUnderTest = createObjectUnderTest();

        final Object deserializedObject = serializeAndDeserialize(objectUnderTest);

        assertThat(deserializedObject, instanceOf(JacksonEvent.class));
        final JacksonEvent deserializedEvent = (JacksonEvent) deserializedObject;

        assertThat(deserializedEvent.toMap(), equalTo(objectUnderTest.toMap()));
        assertThat(deserializedEvent.getMetadata(), equalTo(objectUnderTest.getMetadata()));

        assertThat(deserializedEvent.getEventHandle(), instanceOf(InternalEventHandle.class));
        assertThat(((InternalEventHandle) deserializedEvent.getEventHandle()).getAcknowledgementSet(), nullValue());
        assertThat(deserializedEvent.getEventHandle().getInternalOriginationTime(), equalTo(objectUnderTest.getMetadata().getTimeReceived()));

    }

    @Test
    void serialize_with_acknowledgementSet_does_not_include_old_acknowledgement_set() throws IOException, ClassNotFoundException {
        final JacksonEvent objectUnderTest = createObjectUnderTest();
        final InternalEventHandle internalEventHandle = (InternalEventHandle) objectUnderTest.getEventHandle();
        internalEventHandle.setAcknowledgementSet(mock(AcknowledgementSet.class));

        final Object deserializedObject = serializeAndDeserialize(objectUnderTest);

        assertThat(deserializedObject, instanceOf(JacksonEvent.class));
        final JacksonEvent deserializedEvent = (JacksonEvent) deserializedObject;

        assertThat(deserializedEvent.toMap(), equalTo(objectUnderTest.toMap()));
        assertThat(deserializedEvent.getMetadata(), equalTo(objectUnderTest.getMetadata()));

        assertThat(deserializedEvent.getEventHandle(), instanceOf(InternalEventHandle.class));
        assertThat(((InternalEventHandle) deserializedEvent.getEventHandle()).getAcknowledgementSet(), nullValue());
        assertThat(deserializedEvent.getEventHandle().getInternalOriginationTime(), equalTo(objectUnderTest.getMetadata().getTimeReceived()));
    }

    private Object serializeAndDeserialize(final JacksonEvent objectUnderTest) throws IOException, ClassNotFoundException {
        objectOutputStream.writeObject(objectUnderTest);
        final ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        return objectInputStream.readObject();
    }

}