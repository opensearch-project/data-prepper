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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EntityResolverTest {

    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setUp() {
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

    private Entity resolveEntity(final EntityResolver resolver, final Event event) {
        return resolver.buildEntity(resolver.resolveKey(event), event);
    }

    @Test
    void resolve_interpolatesTemplatesFromEvent() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());
        final Event event = eventWith(Map.of("resourceId", "/subscriptions/abc/resourceGroups/rg"));

        final Entity entity = resolveEntity(resolver, event);

        assertThat(entity.keyAttributes().get("Type"), equalTo("Service"));
        assertThat(entity.keyAttributes().get("Name"), equalTo("/subscriptions/abc/resourceGroups/rg"));
    }

    @Test
    void resolveKey_keyIsDerivedFromResolvedKeyAttributes() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());

        // Events whose templates interpolate to the same key attributes share a grouping key; those
        // that differ get distinct keys.
        final EntityResolver.ResolvedKey keyA = resolver.resolveKey(eventWith(Map.of("resourceId", "res-a")));
        final EntityResolver.ResolvedKey keyASecond = resolver.resolveKey(eventWith(Map.of("resourceId", "res-a")));
        final EntityResolver.ResolvedKey keyB = resolver.resolveKey(eventWith(Map.of("resourceId", "res-b")));

        assertThat(keyASecond, equalTo(keyA));
        assertThat(keyB, not(equalTo(keyA)));
    }

    @Test
    void resolvedKey_equalKeysAgreeOnHashCodeSoTheyCollapseToOneGroup() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());

        final EntityResolver.ResolvedKey first = resolver.resolveKey(eventWith(Map.of("resourceId", "res-a")));
        final EntityResolver.ResolvedKey second = resolver.resolveKey(eventWith(Map.of("resourceId", "res-a")));

        // ResolvedKey is used directly as a map key, so equals() without a matching hashCode() would
        // silently open a second group for the same entity.
        assertThat(second.hashCode(), equalTo(first.hashCode()));
        assertThat(new HashMap<>(Map.of(first, "group")).get(second), equalTo("group"));
    }

    @Test
    void resolvedKey_isNotEqualToOtherTypesOrNull() {
        final EntityResolver.ResolvedKey key = resolver(keyAttrTemplates(), Map.of())
                .resolveKey(eventWith(Map.of("resourceId", "res-a")));

        assertThat(key.equals(key), equalTo(true));
        assertThat(key.equals("not a key"), equalTo(false));
        assertThat(key.equals(null), equalTo(false));
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
    void resolve_distinctKeysProduceDistinctEntities() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());

        final Entity a = resolveEntity(resolver, eventWith(Map.of("resourceId", "res-a")));
        final Entity b = resolveEntity(resolver, eventWith(Map.of("resourceId", "res-b")));

        assertThat(a.keyAttributes().get("Name"), equalTo("res-a"));
        assertThat(b.keyAttributes().get("Name"), equalTo("res-b"));
    }

    @Test
    void resolve_missingTemplateKeyFallsBackToEmptyString() {
        final EntityResolver resolver = resolver(keyAttrTemplates(), Map.of());
        final Event event = eventWith(Map.of("someOtherField", "value"));

        final Entity entity = resolveEntity(resolver, event);

        // ${resourceId} is absent → three-arg formatString substitutes the empty default, not a throw.
        assertThat(entity.keyAttributes().get("Name"), equalTo(""));
        assertThat(entity.keyAttributes().get("Type"), equalTo("Service"));
    }

    @Test
    void resolve_resolvesAttributesInAdditionToKeyAttributes() {
        final Map<String, String> attributes = Map.of("AWS.ServiceNameSource", "UserConfiguration");
        final EntityResolver resolver = resolver(keyAttrTemplates(), attributes);
        final Event event = eventWith(Map.of("resourceId", "res-1"));

        final Entity entity = resolveEntity(resolver, event);

        assertThat(entity.attributes().get("AWS.ServiceNameSource"), equalTo("UserConfiguration"));
    }

    @Test
    void resolve_emitsTemplatedIdentifierVerbatim() {
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Identifier", "${resourceId}");
        final EntityResolver resolver = resolver(keyAttributes, Map.of());
        final Event event = eventWith(Map.of("resourceId", "/Subscriptions/ABC"));

        final Entity entity = resolveEntity(resolver, event);

        assertThat(entity.keyAttributes().get("Identifier"), equalTo("/Subscriptions/ABC"));
    }

    @Test
    void resolve_evaluatesExpressionValuedKeyAttributeThroughTheEvaluator() {
        final String expression = "getMetadata(\"resourceId\")";
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Type", "Service");
        keyAttributes.put("Name", "${" + expression + "}");

        // An expression is not a field on the event, so resolution depends entirely on the evaluator.
        when(expressionEvaluator.isValidExpressionStatement(eq(expression))).thenReturn(true);
        when(expressionEvaluator.evaluate(eq(expression), any(Event.class))).thenReturn("res-from-metadata");

        final EntityResolver resolver = resolver(keyAttributes, Map.of());
        final Entity entity = resolveEntity(resolver, eventWith(Map.of("unrelated", "value")));

        // With a null evaluator this silently resolved to "" and every event collapsed into one group.
        assertThat(entity.keyAttributes().get("Name"), equalTo("res-from-metadata"));
        assertThat(entity.keyAttributes().get("Type"), equalTo("Service"));
    }

    @Test
    void resolveKey_expressionValuedKeyAttributesGroupByEvaluatedValue() {
        final String expression = "getMetadata(\"resourceId\")";
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Name", "${" + expression + "}");

        final Event first = eventWith(Map.of("seq", 1));
        final Event second = eventWith(Map.of("seq", 2));
        when(expressionEvaluator.isValidExpressionStatement(eq(expression))).thenReturn(true);
        when(expressionEvaluator.evaluate(eq(expression), eq(first))).thenReturn("res-a");
        when(expressionEvaluator.evaluate(eq(expression), eq(second))).thenReturn("res-b");

        final EntityResolver resolver = resolver(keyAttributes, Map.of());

        // The grouping key has to follow the evaluated value, otherwise expression-keyed entities would
        // all share one group no matter which resource they describe.
        assertThat(resolver.resolveKey(second), not(equalTo(resolver.resolveKey(first))));
    }

    @Test
    void resolve_evaluatesExpressionValuedAttribute() {
        final String expression = "getMetadata(\"source\")";
        final Map<String, String> attributes = Map.of("AWS.ServiceNameSource", "${" + expression + "}");
        when(expressionEvaluator.isValidExpressionStatement(eq(expression))).thenReturn(true);
        when(expressionEvaluator.evaluate(eq(expression), any(Event.class))).thenReturn("UserConfiguration");

        final EntityResolver resolver = resolver(keyAttrTemplates(), attributes);
        final Entity entity = resolveEntity(resolver, eventWith(Map.of("resourceId", "res-1")));

        // attributes are interpolated on the buildEntity path, so they need the evaluator too.
        assertThat(entity.attributes().get("AWS.ServiceNameSource"), equalTo("UserConfiguration"));
    }

    @Test
    void resolve_fallsBackToLiteralWhenTemplateIsMalformed() {
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Name", "${unclosed");
        final EntityResolver resolver = resolver(keyAttributes, Map.of());

        final Entity entity = resolveEntity(resolver, eventWith(Map.of("resourceId", "res-1")));

        // formatString throws on a malformed template; the literal is kept rather than failing the batch.
        assertThat(entity.keyAttributes().get("Name"), equalTo("${unclosed"));
    }
}
