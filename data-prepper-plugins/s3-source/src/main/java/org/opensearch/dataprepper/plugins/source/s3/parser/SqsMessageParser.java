/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.source.s3.S3SourceConfig;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.Collection;
import java.util.stream.Collectors;

public class SqsMessageParser {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final S3SourceConfig s3SourceConfig;
    private final S3NotificationParser s3NotificationParser;

    public SqsMessageParser(final S3SourceConfig s3SourceConfig) {
        this.s3SourceConfig = s3SourceConfig;
        s3NotificationParser = createNotificationParser(s3SourceConfig);
    }

    public Collection<ParsedMessage> parseSqsMessages(final Collection<Message> sqsMessages) {
        return sqsMessages.stream()
                .map(this::convertS3EventMessages)
                .collect(Collectors.toList());
    }

    private ParsedMessage convertS3EventMessages(final Message message) {
        return s3NotificationParser.parseMessage(message, OBJECT_MAPPER);
    }

    private static S3NotificationParser createNotificationParser(final S3SourceConfig s3SourceConfig) {
        switch (s3SourceConfig.getNotificationSource()) {
            case EVENTBRIDGE:
                return new S3EventBridgeNotificationParser();
            case S3:
            default:
                return new S3EventNotificationParser();
        }
    }
}
