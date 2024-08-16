/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingEventKeyFactoryTest {
    private static final int CACHE_SIZE = 2;
    @Mock
    private EventKeyFactory innerEventKeyFactory;

    @Mock
    private EventConfiguration eventConfiguration;

    @BeforeEach
    void setUp() {
        when(eventConfiguration.getMaximumCachedKeys()).thenReturn(CACHE_SIZE);
    }

    private EventKeyFactory createObjectUnderTest() {
        return new CachingEventKeyFactory(innerEventKeyFactory, eventConfiguration);
    }

    @ParameterizedTest
    @EnumSource(EventKeyFactory.EventAction.class)
    void createEventKey_with_EventAction_returns_inner_createEventKey(final EventKeyFactory.EventAction eventAction) {
        final String key = UUID.randomUUID().toString();
        final EventKey eventKey = mock(EventKey.class);

        when(innerEventKeyFactory.createEventKey(key, eventAction)).thenReturn(eventKey);

        final EventKey actualEventKey = createObjectUnderTest().createEventKey(key, eventAction);
        assertThat(actualEventKey, sameInstance(eventKey));
    }

    @Test
    void createEventKey_returns_inner_createEventKey() {
        final String key = UUID.randomUUID().toString();
        final EventKey eventKey = mock(EventKey.class);

        when(innerEventKeyFactory.createEventKey(key, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey);
        final EventKey actualEventKey = createObjectUnderTest().createEventKey(key);
        assertThat(actualEventKey, sameInstance(eventKey));
    }

    @ParameterizedTest
    @EnumSource(EventKeyFactory.EventAction.class)
    void createEventKey_with_EventAction_returns_same_instance_without_calling_inner_createEventKey_for_same_key(final EventKeyFactory.EventAction eventAction) {
        final String key = UUID.randomUUID().toString();
        final EventKey eventKey = mock(EventKey.class);

        when(innerEventKeyFactory.createEventKey(key, eventAction)).thenReturn(eventKey);

        final EventKeyFactory objectUnderTest = createObjectUnderTest();
        final EventKey actualKey = objectUnderTest.createEventKey(key, eventAction);
        final EventKey actualKey2 = objectUnderTest.createEventKey(key, eventAction);

        assertThat(actualKey, sameInstance(eventKey));
        assertThat(actualKey2, sameInstance(eventKey));

        verify(innerEventKeyFactory).createEventKey(key, eventAction);
    }

    @Test
    void createEventKey_returns_same_instance_without_calling_inner_createEventKey_for_same_key() {
        final String key = UUID.randomUUID().toString();
        final EventKey eventKey = mock(EventKey.class);

        when(innerEventKeyFactory.createEventKey(key, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey);

        final EventKeyFactory objectUnderTest = createObjectUnderTest();
        final EventKey actualKey = objectUnderTest.createEventKey(key);
        final EventKey actualKey2 = objectUnderTest.createEventKey(key);

        assertThat(actualKey, sameInstance(eventKey));
        assertThat(actualKey2, sameInstance(eventKey));

        verify(innerEventKeyFactory).createEventKey(key, EventKeyFactory.EventAction.ALL);
    }

    @Test
    void createEventKey_with_EventAction_returns_different_values_for_different_keys() {
        final String key1 = UUID.randomUUID().toString();
        final String key2 = UUID.randomUUID().toString();
        final EventKey eventKey1 = mock(EventKey.class);
        final EventKey eventKey2 = mock(EventKey.class);

        when(innerEventKeyFactory.createEventKey(key1, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey1);
        when(innerEventKeyFactory.createEventKey(key2, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey2);

        final EventKeyFactory objectUnderTest = createObjectUnderTest();
        final EventKey actualEventKey1 = objectUnderTest.createEventKey(key1, EventKeyFactory.EventAction.ALL);
        assertThat(actualEventKey1, sameInstance(eventKey1));
        final EventKey actualEventKey2 = objectUnderTest.createEventKey(key2, EventKeyFactory.EventAction.ALL);
        assertThat(actualEventKey2, sameInstance(eventKey2));
    }

    @Test
    void createEventKey_with_EventAction_returns_different_values_for_different_actions() {
        final String key = UUID.randomUUID().toString();
        final EventKey eventKeyGet = mock(EventKey.class);
        final EventKey eventKeyPut = mock(EventKey.class);

        when(innerEventKeyFactory.createEventKey(key, EventKeyFactory.EventAction.GET)).thenReturn(eventKeyGet);
        when(innerEventKeyFactory.createEventKey(key, EventKeyFactory.EventAction.PUT)).thenReturn(eventKeyPut);

        final EventKeyFactory objectUnderTest = createObjectUnderTest();
        final EventKey actualEventKeyGet = objectUnderTest.createEventKey(key, EventKeyFactory.EventAction.GET);
        assertThat(actualEventKeyGet, sameInstance(eventKeyGet));
        final EventKey actualEventKeyPut = objectUnderTest.createEventKey(key, EventKeyFactory.EventAction.PUT);
        assertThat(actualEventKeyPut, sameInstance(eventKeyPut));
    }

    @Test
    void createEventKey_expires_after_reaching_maximum() {

        final List<String> keys = new ArrayList<>(CACHE_SIZE);
        for (int i = 0; i < CACHE_SIZE * 2; i++) {
            final String key = UUID.randomUUID().toString();
            final EventKey eventKey = mock(EventKey.class);
            when(innerEventKeyFactory.createEventKey(key, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey);
            keys.add(key);
        }

        final EventKeyFactory objectUnderTest = createObjectUnderTest();

        final int numberOfIterations = 10;
        for (int i = 0; i < numberOfIterations; i++) {
            for (final String key : keys) {
                objectUnderTest.createEventKey(key);
            }
        }

        verify(innerEventKeyFactory, atLeast(numberOfIterations * CACHE_SIZE))
                .createEventKey(anyString(), eq(EventKeyFactory.EventAction.ALL));
    }
}