/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import org.mockito.Mockito;

public class AttributeHandlerTest {

    @Test
    void testCollectMetadataAttributes() {
        final Map<MessageSystemAttributeName, String> systemAttributes = new HashMap<>();
        systemAttributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, "1234567890");
        systemAttributes.put(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "5");
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

        messageAttributes.put("CustomKey", MessageAttributeValue.builder()
                .stringValue("customValue")
                .dataType("String")
                .build());

        final Message message = Message.builder()
                .messageId("id-1")
                .receiptHandle("rh-1")
                .body("Test message")
                .attributes(systemAttributes)
                .messageAttributes(messageAttributes)
                .build();

        final String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue";
        final Map<String, String> metadata = AttributeHandler.collectMetadataAttributes(message, queueUrl);
        assertThat(metadata.get("queueUrl"), equalTo(queueUrl));
        assertThat(metadata.get("sentTimestamp"), equalTo("1234567890"));
        assertThat(metadata.get("approximateReceiveCount"), equalTo("5"));
        assertThat(metadata.get("customKey"), equalTo("customValue"));
    }

    @Test
    void testApplyMetadataAttributes() {
        final Event event = Mockito.mock(Event.class);
        final EventMetadata metadata = Mockito.mock(EventMetadata.class);
        when(event.getMetadata()).thenReturn(metadata);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        AttributeHandler.applyMetadataAttributes(event, attributes);
        verify(metadata).setAttribute("key1", "value1");
        verify(metadata).setAttribute("key2", "value2");
    }
}
