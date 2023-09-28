package org.opensearch.dataprepper.plugins.source.s3.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * A S3NotificationParser interface must be extended/implement for SQS message parsing
 *
 */
public interface S3NotificationParser {
    /**
     * Parse SQS message body using ObjectMapper and return a ParsedMessage object
     * @param message Raw SQS message
     * @param objectMapper ObjectMapper to use for parsing
     *
     * @return ParsedMessage after deserializing the SQS message body
     */
    ParsedMessage parseMessage(Message message, ObjectMapper objectMapper);
}
