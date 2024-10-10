/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventFactoryApplicationConfigurationTest {
    private EventFactoryApplicationConfiguration createObjectUnderTest() {
        return new EventFactoryApplicationConfiguration();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void eventKeyFactory_returns_innerEventKeyFactory_if_EventConfiguration_is_cache_disabled(final int cacheMax) {
        final EventKeyFactory innerEventKeyFactory = mock(EventKeyFactory.class);
        final EventConfiguration eventConfiguration = mock(EventConfiguration.class);
        when(eventConfiguration.getMaximumCachedKeys()).thenReturn(cacheMax);

        final EventKeyFactory actualEventKeyFactory = createObjectUnderTest().eventKeyFactory(innerEventKeyFactory, eventConfiguration);

        assertThat(actualEventKeyFactory, sameInstance(innerEventKeyFactory));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 512, 1_000})
    void eventKeyFactory_returns_CachingEventKeyFactory_for_cacheable_sizes(final int cacheMax) {
        final EventKeyFactory innerEventKeyFactory = mock(EventKeyFactory.class);
        final EventConfiguration eventConfiguration = mock(EventConfiguration.class);
        when(eventConfiguration.getMaximumCachedKeys()).thenReturn(cacheMax);

        final EventKeyFactory actualEventKeyFactory = createObjectUnderTest().eventKeyFactory(innerEventKeyFactory, eventConfiguration);

        assertThat(actualEventKeyFactory, not(sameInstance(innerEventKeyFactory)));
        assertThat(actualEventKeyFactory, instanceOf(CachingEventKeyFactory.class));
    }
}