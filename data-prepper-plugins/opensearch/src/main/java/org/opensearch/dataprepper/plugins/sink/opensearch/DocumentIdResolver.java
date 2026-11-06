/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;

import java.util.Optional;

public class DocumentIdResolver {
    private final String documentIdField;
    private final String documentId;
    private final ExpressionEvaluator expressionEvaluator;

    public DocumentIdResolver(final String documentIdField, final String documentId, final ExpressionEvaluator expressionEvaluator) {
        this.documentIdField = documentIdField;
        this.documentId = documentId;
        this.expressionEvaluator = expressionEvaluator;
    }

    public Optional<String> resolve(final Event event) {
        if (documentIdField != null) {
            return Optional.ofNullable(event.get(documentIdField, String.class));
        }
        if (documentId != null) {
            return Optional.ofNullable(event.formatString(documentId, expressionEvaluator));
        }
        return Optional.empty();
    }
}
