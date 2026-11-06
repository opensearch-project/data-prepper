/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DocumentIdResolverTest {

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private Event event;

    private String documentIdField;
    private String documentIdExpression;
    private String fieldValue;
    private String expressionValue;

    @BeforeEach
    void setUp() {
        documentIdField = UUID.randomUUID().toString();
        documentIdExpression = "${/" + UUID.randomUUID().toString() + "}";
        fieldValue = UUID.randomUUID().toString();
        expressionValue = UUID.randomUUID().toString();
    }

    private DocumentIdResolver createObjectUnderTest() {
        return new DocumentIdResolver(documentIdField, documentIdExpression, expressionEvaluator);
    }

    @Test
    void resolve_uses_document_id_field_when_present() {
        when(event.get(documentIdField, String.class)).thenReturn(fieldValue);

        assertThat(createObjectUnderTest().resolve(event), equalTo(Optional.of(fieldValue)));
    }

    @Test
    void resolve_uses_document_id_expression_when_field_is_null() {
        documentIdField = null;
        when(event.formatString(documentIdExpression, expressionEvaluator)).thenReturn(expressionValue);

        assertThat(createObjectUnderTest().resolve(event), equalTo(Optional.of(expressionValue)));
    }

    @Test
    void resolve_prefers_field_over_expression() {
        when(event.get(documentIdField, String.class)).thenReturn(fieldValue);

        assertThat(createObjectUnderTest().resolve(event), equalTo(Optional.of(fieldValue)));
    }

    @Test
    void resolve_returns_empty_when_both_null() {
        documentIdField = null;
        documentIdExpression = null;

        assertThat(createObjectUnderTest().resolve(event), equalTo(Optional.empty()));
    }

    @Test
    void resolve_returns_empty_when_field_returns_null() {
        documentIdExpression = null;
        when(event.get(documentIdField, String.class)).thenReturn(null);

        assertThat(createObjectUnderTest().resolve(event), equalTo(Optional.empty()));
    }
}
