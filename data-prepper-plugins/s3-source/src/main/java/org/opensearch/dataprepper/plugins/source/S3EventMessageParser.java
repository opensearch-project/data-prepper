/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles parsing of S3 Event messages.
 */
public class S3EventMessageParser {
    private static final String SNS_MESSAGE_KEY = "Message";
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a message body into an {@link S3EventNotification} class.
     *
     * @param messageBody Input body
     * @return The parsed event notification
     * @throws JsonProcessingException An exception with parsing the event notification.
     */
    S3EventNotification parseMessage(final String messageBody) throws JsonProcessingException {
        final JsonNode parsedNode = objectMapper.readTree(messageBody);

        final JsonNode eventNode = getS3EventNode(parsedNode);

        return objectMapper.treeToValue(eventNode, S3EventNotification.class);
    }

    private JsonNode getS3EventNode(final JsonNode parsedNode) throws JsonProcessingException {
        if(isSnsWrappedMessage(parsedNode)) {
            final String messageString = parsedNode.get(SNS_MESSAGE_KEY).asText();
            return objectMapper.readValue(messageString, JsonNode.class);
        }

        return parsedNode;
    }

    private boolean isSnsWrappedMessage(final JsonNode parsedNode) {
        return parsedNode.has(SNS_MESSAGE_KEY);
    }
}
