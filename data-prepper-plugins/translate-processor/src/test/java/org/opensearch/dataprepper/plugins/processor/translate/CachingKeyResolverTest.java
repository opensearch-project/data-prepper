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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingKeyResolverTest {
    @Mock
    private EventKeyFactory eventKeyFactory;

    @Mock
    private Event event;

    @Mock
    private ExpressionEvaluator evaluator;

    @Mock
    private EventKey mockEventKey;

    private CachingKeyResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new CachingKeyResolver(eventKeyFactory);
        lenient().when(eventKeyFactory.createEventKey(anyString())).thenReturn(mockEventKey);
    }

    @Test
    void testStaticKeyCaching() {
        String key = "user.id";

        // First resolution should create cache entry
        assertThat(resolver.resolveKey(key, event, evaluator), is(mockEventKey));
        assertThat(resolver.getCacheSize(), is(1));

        // Second resolution should use cache
        assertThat(resolver.resolveKey(key, event, evaluator), is(mockEventKey));
        assertThat(resolver.getCacheSize(), is(1));

        // Verify EventKeyFactory only called once
        verify(eventKeyFactory, times(1)).createEventKey(anyString());
    }

    @Test
    void testDynamicKeyResolution() {
        String key = "user.%{type}.id";
        String resolvedKey = "user.admin.id";
        when(event.formatString(eq(key), eq(evaluator))).thenReturn(resolvedKey);

        // First resolution
        assertThat(resolver.resolveKey(key, event, evaluator), is(mockEventKey));
        assertThat(resolver.getCacheSize(), is(1));

        // Second resolution should reuse KeyInfo but still format string
        assertThat(resolver.resolveKey(key, event, evaluator), is(mockEventKey));
        assertThat(resolver.getCacheSize(), is(1));

        // Verify string formatting called twice
        verify(event, times(2)).formatString(eq(key), eq(evaluator));
    }

    @Test
    void testNullKeyResolution() {
        assertThat(resolver.resolveKey(null, event, evaluator), is(nullValue()));
        assertThat(resolver.getCacheSize(), is(0));
    }

    @Test
    void testCacheClear() {
        resolver.resolveKey("key1", event, evaluator);
        resolver.resolveKey("key2", event, evaluator);
        assertThat(resolver.getCacheSize(), is(2));

        resolver.clearCache();
        assertThat(resolver.getCacheSize(), is(0));
    }
}