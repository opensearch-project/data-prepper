/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.document;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class JacksonDocumentTest {

    @Test
    public void testBuilderUsesDocumentEventType() {
        final Document document = JacksonDocument.builder().build();

        assertThat(document, is(notNullValue()));
        assertThat(document.getMetadata().getEventType(), is(equalTo("DOCUMENT")));
    }

    @Test
    public void testBuilderUsesCustomEventType() {
        final Document document = JacksonDocument.builder()
                        .withEventType("custom")
                        .getThis()
                        .build();

        assertThat(document, is(notNullValue()));
        assertThat(document.getMetadata().getEventType(), is(equalTo("DOCUMENT")));
    }

    @Test
    public void testBuilderWithoutDocumentEventType_throwsIllegalArgumentException() {
        final EventMetadata eventMetadata = DefaultEventMetadata.builder()
                .withEventType("someEventType")
                .build();

        final JacksonEvent.Builder<JacksonDocument.Builder> builder = JacksonDocument.builder()
                .withEventMetadata(eventMetadata);

        assertThat(builder, is(notNullValue()));
        assertThrows(IllegalArgumentException.class, builder:: build);
    }
}
