package org.opensearch.dataprepper.plugins.source.s3.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.source.s3.S3EventNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

public class S3EventNotificationParser implements S3NotificationParser {
    private static final Logger LOG = LoggerFactory.getLogger(S3EventNotificationParser.class);
    private static final String SNS_MESSAGE_KEY = "Message";
    @Override
    public ParsedMessage parseMessage(final Message message, final ObjectMapper objectMapper) {
        final JsonNode parsedNode;
        try {
            parsedNode = objectMapper.readTree(message.body());

            final JsonNode eventNode = getS3EventNode(parsedNode, objectMapper);

            final S3EventNotification s3EventNotification = objectMapper.treeToValue(eventNode, S3EventNotification.class);
            if (s3EventNotification.getRecords() != null) {
                return new ParsedMessage(message, s3EventNotification.getRecords());
            } else {
                LOG.debug("SQS message with ID:{} does not have any S3 event notification records.", message.messageId());
                return new ParsedMessage(message, true);
            }
        } catch (final JsonProcessingException e) {
            if (message.body().contains("s3:TestEvent") && message.body().contains("Amazon S3")) {
                LOG.info("Received s3:TestEvent message. Deleting from SQS queue.");
                return new ParsedMessage(message, false);
            } else {
                LOG.error("SQS message with message ID:{} has invalid body which cannot be parsed into S3EventNotification. {}.", message.messageId(), e.getMessage());
            }
        }
        return new ParsedMessage(message, true);
    }

    private JsonNode getS3EventNode(final JsonNode parsedNode, final ObjectMapper objectMapper) throws JsonProcessingException {
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
