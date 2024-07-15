/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class DefaultEventKeyFactoryTest {

    private DefaultEventKeyFactory createObjectUnderTest() {
        return new DefaultEventKeyFactory();
    }

    @Test
    void createEventKey_returns_correct_EventKey() {
        final String keyPath = UUID.randomUUID().toString();
        final EventKey eventKey = createObjectUnderTest().createEventKey(keyPath);

        assertThat(eventKey, notNullValue());
        assertThat(eventKey.getKey(), equalTo(keyPath));
    }

    @Test
    void createEventKey_with_EventAction_returns_correct_EventKey() {
        final String keyPath = UUID.randomUUID().toString();
        final EventKey eventKey = createObjectUnderTest().createEventKey(keyPath, EventKeyFactory.EventAction.GET);

        assertThat(eventKey, notNullValue());
        assertThat(eventKey.getKey(), equalTo(keyPath));
    }

    @Test
    void createEventKey_returns_JacksonEventKey() {
        final String keyPath = UUID.randomUUID().toString();
        final EventKey eventKey = createObjectUnderTest().createEventKey(keyPath);

        assertThat(eventKey, notNullValue());
        assertThat(eventKey.getClass().getSimpleName(), equalTo("JacksonEventKey"));

        assertThat(eventKey.getKey(), equalTo(keyPath));
    }
}