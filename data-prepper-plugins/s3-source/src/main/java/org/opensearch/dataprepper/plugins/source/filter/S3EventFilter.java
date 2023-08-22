/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.filter;

import org.opensearch.dataprepper.plugins.source.parser.ParsedMessage;

import java.util.Optional;

/**
 * A S3EventFilter interface must be extended/implement for filtering event type or detail type in notification
 */
public interface S3EventFilter {
    /**
     * Filter {@link ParsedMessage} based on the S3EventFilter implementation.
     * @param parsedMessage Parsed object of SQS message
     *
     * @return Optional of {@link ParsedMessage} if the message is to be filtered out, else empty
     */
    Optional<ParsedMessage> filter(ParsedMessage parsedMessage);
}
