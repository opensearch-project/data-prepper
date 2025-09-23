/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyInfoTest {
    @Mock
    private EventKeyFactory eventKeyFactory;

    @Mock
    private Event event;

    @Mock
    private ExpressionEvaluator evaluator;

    @Mock
    private EventKey mockEventKey;

    private KeyInfo staticKeyInfo;
    private KeyInfo dynamicKeyInfo;
    private KeyInfo nestedPathKeyInfo;

    @BeforeEach
    void setUp() {
        when(eventKeyFactory.createEventKey(anyString())).thenReturn(mockEventKey);

        staticKeyInfo = new KeyInfo("user.id", eventKeyFactory);
        dynamicKeyInfo = new KeyInfo("user.%{type}.id", eventKeyFactory);
        nestedPathKeyInfo = new KeyInfo("parent/child/items/%{index}", eventKeyFactory);
    }

    @Test
    void testStaticKeyResolution() {
        assertThat(staticKeyInfo.isDynamic(), is(false));
        assertThat(staticKeyInfo.getStaticKey(), is(notNullValue()));
        assertThat(staticKeyInfo.resolveKey(event, evaluator), is(mockEventKey));
    }

    @Test
    void testDynamicKeyResolution() {
        assertThat(dynamicKeyInfo.isDynamic(), is(true));
        assertThat(dynamicKeyInfo.getStaticKey(), is(nullValue()));

        // Setup dynamic resolution
        String resolvedKey = "user.admin.id";
        when(event.formatString(eq("user.%{type}.id"), eq(evaluator))).thenReturn(resolvedKey);

        assertThat(dynamicKeyInfo.resolveKey(event, evaluator), is(mockEventKey));
    }

    @Test
    void testDynamicKeyResolutionFailure() {
        when(event.formatString(anyString(), eq(evaluator))).thenThrow(new RuntimeException("Failed to resolve"));

        assertThat(dynamicKeyInfo.resolveKey(event, evaluator), is(nullValue()));
    }

    @Test
    void testNestedPathParsing() {
        String[] components = nestedPathKeyInfo.getParsedComponents();
        assertThat(components.length, is(4));
        assertThat(components[0], is("parent"));
        assertThat(components[1], is("child"));
        assertThat(components[2], is("items"));
        assertThat(components[3], is("%{index}"));
    }

    @Test
    void testNullKey() {
        KeyInfo nullKeyInfo = new KeyInfo(null, eventKeyFactory);
        assertThat(nullKeyInfo.isDynamic(), is(false));
        assertThat(nullKeyInfo.getStaticKey(), is(nullValue()));
        assertThat(nullKeyInfo.getParsedComponents().length, is(0));
    }
}