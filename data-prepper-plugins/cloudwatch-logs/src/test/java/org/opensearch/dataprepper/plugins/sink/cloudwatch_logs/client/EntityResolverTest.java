/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EntityResolverTest {

    private ExpressionEvaluator expressionEvaluator;
    private CloudWatchLogsMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = mock(CloudWatchLogsMetrics.class);
        // A field-reference-only evaluator: nothing is a valid expression statement, which is how the
        // real evaluator behaves for plain ${field} templates.
        expressionEvaluator = mock(ExpressionEvaluator.class);
    }

    private Event eventWith(final Map<String, Object> data) {
        return JacksonLog.builder().withData(data).build();
    }

    private Map<String, String> keyAttrTemplates() {
        final Map<String, String> m = new LinkedHashMap<>();
        m.put("Type", "Service");
        m.put("Name", "${resourceId}");
        return m;
    }

    private EntityResolver resolver(final Map<String, String> keyAttributes, final Map<String, String> attributes) {
        return new EntityResolver(keyAttributes, attributes, expressionEvaluator);
    }

    private EntityResolver resolver(final Map<String, String> keyAttributes, final Map<String, String> attributes,
                                    final Map<String, String> headers) {
        return new EntityResolver(keyAttributes, attributes, headers, expressionEvaluator, metrics);
    }

    private Entity resolveEntity(final EntityResolver resolver, final Event event) {
        return resolver.buildEntity(resolver.resolveKey(event), event);
    }

    private Optional<String> invalidValueOf(final EntityResolver resolver, final Event event) {
        return resolver.findInvalidValue(resolver.resolveKey(event), event);
    }

    @Test
    void resolve_interpolatesTemplatesFromEventVerbatim() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());
        final Event event = eventWith(Map.of("resourceId", "/Subscriptions/ABC/resourceGroups/RG"));

        final Entity entity = resolveEntity(resolver, event);

        assertThat(entity.keyAttributes().get("Type"), equalTo("Service"));
        assertThat(entity.keyAttributes().get("Name"), equalTo("/Subscriptions/ABC/resourceGroups/RG"));
    }

    @Test
    void resolvedKey_toStringShowsTheResolvedAttributes() {
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Name", "${resourceId}");
        final EntityResolver.ResolvedKey key = resolver(keyAttributes, Map.of())
                .resolveKey(eventWith(Map.of("resourceId", "res-a")));

        assertThat(key.toString(), equalTo("{Name=res-a}"));
    }

    @Test
    void resolve_evaluatesExpressionsInKeyAttributesAttributesAndHeaders() {
        final String expression = "getMetadata(\"resourceId\")";
        final String attributeExpression = "getMetadata(\"source\")";
        final String headerExpression = "getMetadata(\"header\")";
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Type", "Service");
        keyAttributes.put("Name", "${" + expression + "}");
        final Map<String, String> attributes = Map.of("AWS.ServiceNameSource", "${" + attributeExpression + "}");
        final Map<String, String> headers = Map.of("x-source-name", "${" + headerExpression + "}");

        // An expression is not a field on the event, so resolution depends entirely on the evaluator.
        when(expressionEvaluator.isValidExpressionStatement(eq(expression))).thenReturn(true);
        when(expressionEvaluator.evaluate(eq(expression), any(Event.class))).thenReturn("res-from-metadata");
        when(expressionEvaluator.isValidExpressionStatement(eq(attributeExpression))).thenReturn(true);
        when(expressionEvaluator.evaluate(eq(attributeExpression), any(Event.class))).thenReturn("UserConfiguration");
        when(expressionEvaluator.isValidExpressionStatement(eq(headerExpression))).thenReturn(true);
        when(expressionEvaluator.evaluate(eq(headerExpression), any(Event.class))).thenReturn("header-from-metadata");

        final EntityResolver resolver = resolver(keyAttributes, attributes, headers);
        final Event event = eventWith(Map.of("unrelated", "value"));
        final EntityResolver.ResolvedKey key = resolver.resolveKey(event);
        final Entity entity = resolver.buildEntity(key, event);

        // With a null evaluator this silently resolved to "" and every event collapsed into one group.
        assertThat(entity.keyAttributes().get("Name"), equalTo("res-from-metadata"));
        assertThat(entity.keyAttributes().get("Type"), equalTo("Service"));
        // attributes are interpolated on the buildEntity path, so they need the evaluator too.
        assertThat(entity.attributes().get("AWS.ServiceNameSource"), equalTo("UserConfiguration"));
        assertThat(key.getHeaders().get("x-source-name"), equalTo("header-from-metadata"));
    }

    @Test
    void resolveKey_resolvesHeaderTemplatesFromEvent() {
        final Map<String, String> headers = new LinkedHashMap<>();
        headers.put("x-source-name", "${resourceId}");
        headers.put("x-source-type", "${category}");
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of(), headers);

        final EntityResolver.ResolvedKey key = resolver.resolveKey(
                eventWith(Map.of("resourceId", "res-a", "category", "cat-1")));

        assertThat(key.getHeaders().get("x-source-name"), equalTo("res-a"));
        assertThat(key.getHeaders().get("x-source-type"), equalTo("cat-1"));
        verify(metrics, never()).increaseHeaderValueBlankCounter(anyInt());
        verify(metrics, never()).increaseTemplateExpressionFailedCounter(anyInt());
    }

    @Test
    void resolveKey_sameKeyAttributesAndHeadersCollapseToOneGroupAndAnyDifferenceSplits() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of(), Map.of("x-source-type", "${category}"));

        final EntityResolver.ResolvedKey first = resolver.resolveKey(
                eventWith(Map.of("resourceId", "res-a", "category", "cat-1")));
        final EntityResolver.ResolvedKey second = resolver.resolveKey(
                eventWith(Map.of("resourceId", "res-a", "category", "cat-1")));
        final EntityResolver.ResolvedKey otherKeyAttributes = resolver.resolveKey(
                eventWith(Map.of("resourceId", "res-b", "category", "cat-1")));
        final EntityResolver.ResolvedKey otherHeader = resolver.resolveKey(
                eventWith(Map.of("resourceId", "res-a", "category", "cat-2")));

        assertThat(second, equalTo(first));
        assertThat(second.hashCode(), equalTo(first.hashCode()));
        assertThat(new HashMap<>(Map.of(first, "group")).get(second), equalTo("group"));
        assertThat(otherKeyAttributes, not(equalTo(first)));
        assertThat(otherHeader, not(equalTo(first)));
        assertThat(first.equals(first), equalTo(true));
        assertThat(first.equals("not a key"), equalTo(false));
        assertThat(first.equals(null), equalTo(false));
    }

    @Test
    void resolve_fallsBackToLiteralWhenTemplateIsMalformed() {
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Name", "${unclosed");
        final EntityResolver resolver = resolver(keyAttributes, Map.of(), Map.of("x-source-type", "${unclosed"));

        final Event event = eventWith(Map.of("resourceId", "res-1"));
        final EntityResolver.ResolvedKey key = resolver.resolveKey(event);
        final Entity entity = resolver.buildEntity(key, event);

        // formatString throws on a malformed template; the literal is kept rather than failing the batch.
        assertThat(entity.keyAttributes().get("Name"), equalTo("${unclosed"));
        assertThat(key.getHeaders().get("x-source-type"), equalTo("${unclosed"));
        verify(metrics, times(2)).increaseTemplateExpressionFailedCounter(1);
        verify(metrics, never()).increaseHeaderValueBlankCounter(anyInt());
    }

    @Test
    void findInvalidValue_templatedKeyFieldMissing_isBlankValue() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());

        assertThat(invalidValueOf(resolver, eventWith(Map.of("ResourceId", "res-a"))),
                equalTo(Optional.of(EntityResolver.BLANK_VALUE)));
    }

    @Test
    void findInvalidValue_keyValueIsWhitespace_isBlankValueByChoiceThoughCloudWatchAcceptsIt() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());

        assertThat(invalidValueOf(resolver, eventWith(Map.of("resourceId", "   "))),
                equalTo(Optional.of(EntityResolver.BLANK_VALUE)));
    }

    @Test
    void findInvalidValue_keyOrAttributeValueAtLengthLimit_isEmptyAndOneOverIsValueTooLong() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());
        final String atLimit = "a".repeat(EntityResolver.MAX_ENTITY_VALUE_LENGTH);
        final String overLimit = atLimit + "a";

        assertThat(invalidValueOf(resolver, eventWith(Map.of("resourceId", atLimit))), equalTo(Optional.empty()));
        assertThat(invalidValueOf(resolver, eventWith(Map.of("resourceId", overLimit))),
                equalTo(Optional.of(EntityResolver.VALUE_TOO_LONG)));

        final EntityResolver attributeResolver = resolver(keyAttrTemplates(), Map.of("Category", "${category}"));
        assertThat(invalidValueOf(attributeResolver, eventWith(Map.of("resourceId", "res-a", "category", overLimit))),
                equalTo(Optional.of(EntityResolver.VALUE_TOO_LONG)));
    }

    @Test
    void findInvalidValue_supplementaryCharactersAtCodePointLimit_isEmpty() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());
        final String atLimit = "\uD83D\uDE00".repeat(EntityResolver.MAX_ENTITY_VALUE_LENGTH);

        assertThat(invalidValueOf(resolver, eventWith(Map.of("resourceId", atLimit))), equalTo(Optional.empty()));
    }

    @Test
    void findInvalidValue_headerOnlyResolver_isEmpty() {
        final EntityResolver resolver = resolver(Map.of(), Map.of(), Map.of("x-source-type", "${category}"));

        assertThat(invalidValueOf(resolver, eventWith(Map.of("unrelated", "value"))).isPresent(), equalTo(false));
        verify(metrics).increaseHeaderValueBlankCounter(1);
    }

    @Test
    void findInvalidValue_templatedAttributeFieldMissing_isBlankValue() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of("Category", "${category}"));

        assertThat(invalidValueOf(resolver, eventWith(Map.of("resourceId", "res-a"))),
                equalTo(Optional.of(EntityResolver.BLANK_VALUE)));
    }

}
