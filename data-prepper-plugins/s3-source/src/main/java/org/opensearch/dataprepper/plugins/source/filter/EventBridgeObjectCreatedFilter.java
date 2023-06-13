package org.opensearch.dataprepper.plugins.source.filter;

import org.opensearch.dataprepper.plugins.source.parser.ParsedMessage;

import java.util.Optional;

public class EventBridgeObjectCreatedFilter implements S3EventFilter {
    /**
     * Filters {@link ParsedMessage} of {@link org.opensearch.dataprepper.plugins.source.S3EventBridgeNotification} by detail-type
     *
     * @return Returns Optional of {@link ParsedMessage} if detail-type is Object Created for EventBridge notification
     * does not have detail-type and returns Optional.empty() if detail-type is not Object Created
     */
    @Override
    public Optional<ParsedMessage> filter(final ParsedMessage parsedMessage) {
        if (parsedMessage.getDetailType() != null) {
            if (parsedMessage.getDetailType().equals("Object Created")) {
                return Optional.of(parsedMessage);
            }
            else {
                return Optional.empty();
            }
        } else {
            return Optional.of(parsedMessage);
        }
    }
}
