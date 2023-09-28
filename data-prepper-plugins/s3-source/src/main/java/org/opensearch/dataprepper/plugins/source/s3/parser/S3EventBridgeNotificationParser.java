package org.opensearch.dataprepper.plugins.source.s3.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.source.s3.S3EventBridgeNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

public class S3EventBridgeNotificationParser implements S3NotificationParser {
    private static final Logger LOG = LoggerFactory.getLogger(S3EventBridgeNotificationParser.class);

    @Override
    public ParsedMessage parseMessage(final Message message, final ObjectMapper objectMapper) {
        try {
            final S3EventBridgeNotification s3EventBridgeNotification = objectMapper.readValue(message.body(), S3EventBridgeNotification.class);
            return new ParsedMessage(message, s3EventBridgeNotification);
        } catch (final JsonProcessingException e) {
            LOG.error("SQS message with message ID:{} has invalid body which cannot be parsed into EventBridgeNotification. {}.", message.messageId(), e.getMessage());
            return new ParsedMessage(message, true);
        }
    }
}
