/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugin.DefaultPluginFactory;
import org.springframework.context.ApplicationContext;

import java.util.function.Function;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestApplicationContextProviderTest {
    @Mock
    private ExtensionContext extensionContext;

    @Mock
    private ExtensionContext.Store store;

    @BeforeEach
    void setUp() {
        when(extensionContext.getStore(any(ExtensionContext.Namespace.class)))
                .thenReturn(store);
    }

    @Test
    void get_creates_an_ApplicationContext_with_expected_beans() {
        when(store.getOrComputeIfAbsent(any(), any(), any())).thenAnswer(a -> {
            final Function computeFunction = a.getArgument(1, Function.class);

            return computeFunction.apply(null);
        });

        final ApplicationContext applicationContext = TestApplicationContextProvider.get(extensionContext);

        assertThat(applicationContext, notNullValue());
        assertThat(applicationContext.getBean(PluginFactory.class), instanceOf(DefaultPluginFactory.class));
        assertThat(applicationContext.getBean(EventFactory.class), instanceOf(TestEventFactory.class));
        assertThat(applicationContext.getBean(EventKeyFactory.class), instanceOf(TestEventKeyFactory.class));
    }

    @Test
    void get_called_multiple_times_returns_same_instance() {
        final ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(store.getOrComputeIfAbsent(any(), any(), any())).thenReturn(applicationContext);

        final ApplicationContext applicationContext1 = TestApplicationContextProvider.get(extensionContext);
        final ApplicationContext applicationContext2 = TestApplicationContextProvider.get(extensionContext);

        assertThat(applicationContext1, sameInstance(applicationContext));
        assertThat(applicationContext2, sameInstance(applicationContext));
    }
}