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

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

class EntityResolverTest {

    private Event eventWith(final Map<String, Object> data) {
        return JacksonLog.builder().withData(data).build();
    }

    private Map<String, String> keyAttrTemplates() {
        final Map<String, String> m = new LinkedHashMap<>();
        m.put("Type", "Service");
        m.put("Name", "${resourceId}");
        return m;
    }

    @Test
    void resolve_interpolatesTemplatesFromEvent() {
        final EntityResolver resolver = new EntityResolver(keyAttrTemplates(), Map.of(), 100);
        final Event event = eventWith(Map.of("resourceId", "/subscriptions/abc/resourceGroups/rg"));

        final Entity entity = resolver.resolve(event).getEntity();

        assertThat(entity.keyAttributes().get("Type"), equalTo("Service"));
        assertThat(entity.keyAttributes().get("Name"), equalTo("/subscriptions/abc/resourceGroups/rg"));
    }

    @Test
    void resolve_keyIsDerivedFromResolvedKeyAttributes() {
        final EntityResolver resolver = new EntityResolver(keyAttrTemplates(), Map.of(), 100);

        // Events whose templates interpolate to the same key attributes share a grouping key; those
        // that differ get distinct keys.
        final String keyA = resolver.resolve(eventWith(Map.of("resourceId", "res-a"))).getKey();
        final String keyASecond = resolver.resolve(eventWith(Map.of("resourceId", "res-a"))).getKey();
        final String keyB = resolver.resolve(eventWith(Map.of("resourceId", "res-b"))).getKey();

        assertThat(keyASecond, equalTo(keyA));
        assertThat(keyB, not(equalTo(keyA)));
    }

    @Test
    void resolve_cachesEntityPerKey() {
        final EntityResolver resolver = new EntityResolver(keyAttrTemplates(), Map.of(), 100);

        final Entity first = resolver.resolve(eventWith(Map.of("resourceId", "res-1"))).getEntity();
        final Entity second = resolver.resolve(eventWith(Map.of("resourceId", "res-1"))).getEntity();

        // The second event resolves to the same key, so the cached instance is returned.
        assertThat(second, sameInstance(first));
        assertThat(second.keyAttributes().get("Name"), equalTo("res-1"));
        assertThat(resolver.cacheSize(), equalTo(1));
    }

    @Test
    void resolve_distinctKeysProduceDistinctEntities() {
        final EntityResolver resolver = new EntityResolver(keyAttrTemplates(), Map.of(), 100);

        final Entity a = resolver.resolve(eventWith(Map.of("resourceId", "res-a"))).getEntity();
        final Entity b = resolver.resolve(eventWith(Map.of("resourceId", "res-b"))).getEntity();

        assertThat(a.keyAttributes().get("Name"), equalTo("res-a"));
        assertThat(b.keyAttributes().get("Name"), equalTo("res-b"));
        assertThat(resolver.cacheSize(), equalTo(2));
    }

    @Test
    void resolve_missingTemplateKeyFallsBackToEmptyString() {
        final EntityResolver resolver = new EntityResolver(keyAttrTemplates(), Map.of(), 100);
        final Event event = eventWith(Map.of("someOtherField", "value"));

        final Entity entity = resolver.resolve(event).getEntity();

        // ${resourceId} is absent → three-arg formatString substitutes the empty default, not a throw.
        assertThat(entity.keyAttributes().get("Name"), equalTo(""));
        assertThat(entity.keyAttributes().get("Type"), equalTo("Service"));
    }

    @Test
    void resolve_resolvesAttributesInAdditionToKeyAttributes() {
        final Map<String, String> attributes = Map.of("AWS.ServiceNameSource", "UserConfiguration");
        final EntityResolver resolver = new EntityResolver(keyAttrTemplates(), attributes, 100);
        final Event event = eventWith(Map.of("resourceId", "res-1"));

        final Entity entity = resolver.resolve(event).getEntity();

        assertThat(entity.attributes().get("AWS.ServiceNameSource"), equalTo("UserConfiguration"));
    }

    @Test
    void resolve_stopsCachingBeyondMaxCardinalityButStillResolves() {
        final EntityResolver resolver = new EntityResolver(keyAttrTemplates(), Map.of(), 2);

        resolver.resolve(eventWith(Map.of("resourceId", "k1")));
        resolver.resolve(eventWith(Map.of("resourceId", "k2")));
        // Third distinct key exceeds the bound: still resolved, but not retained.
        final Entity overflow = resolver.resolve(eventWith(Map.of("resourceId", "k3"))).getEntity();

        assertThat(overflow.keyAttributes().get("Name"), equalTo("k3"));
        assertThat(resolver.cacheSize(), equalTo(2));
    }

    @Test
    void resolve_emitsTemplatedIdentifierVerbatim() {
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Identifier", "${resourceId}");
        final EntityResolver resolver = new EntityResolver(keyAttributes, Map.of(), 100);
        final Event event = eventWith(Map.of("resourceId", "/Subscriptions/ABC"));

        final Entity entity = resolver.resolve(event).getEntity();

        assertThat(entity.keyAttributes().get("Identifier"), equalTo("/Subscriptions/ABC"));
    }
}
