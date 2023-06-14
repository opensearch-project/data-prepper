/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.filter;

import org.opensearch.dataprepper.plugins.source.parser.ParsedMessage;

import java.util.Optional;

public class S3ObjectCreatedFilter implements S3EventFilter {
    @Override
    public Optional<ParsedMessage> filter(final ParsedMessage parsedMessage) {
        if (parsedMessage.getEventName().startsWith("ObjectCreated"))
            return Optional.of(parsedMessage);
        else
            return Optional.empty();
    }
}
