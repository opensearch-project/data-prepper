/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class EventActionTest {
    @ParameterizedTest
    @EnumSource(value = EventKeyFactory.EventAction.class, mode = EnumSource.Mode.EXCLUDE, names = {"GET"})
    void isMutableAction_is_true_for_mutable_actions(final EventKeyFactory.EventAction eventAction) {
        assertThat(eventAction.isMutableAction(), equalTo(true));
    }

    @ParameterizedTest
    @EnumSource(value = EventKeyFactory.EventAction.class, mode = EnumSource.Mode.INCLUDE, names = {"GET"})
    void isMutableAction_is_false_for_mutable_actions(final EventKeyFactory.EventAction eventAction) {
        assertThat(eventAction.isMutableAction(), equalTo(false));
    }

    @ParameterizedTest
    @EnumSource(value = EventKeyFactory.EventAction.class)
    void getSupportedActions_includes_self(final EventKeyFactory.EventAction eventAction) {
        assertThat(eventAction.getSupportedActions(), hasItem(eventAction));
    }

    @ParameterizedTest
    @EnumSource(value = EventKeyFactory.EventAction.class)
    void getSupportedActions_includes_for_all_actions_when_ALL(final EventKeyFactory.EventAction eventAction) {
        assertThat(EventKeyFactory.EventAction.ALL.getSupportedActions(), hasItem(eventAction));
    }

    @ParameterizedTest
    @ArgumentsSource(SupportsArgumentsProvider.class)
    void supports_returns_expected_value(final EventKeyFactory.EventAction eventAction, final EventKeyFactory.EventAction otherAction, final boolean expectedSupports) {
        assertThat(eventAction.getSupportedActions().contains(otherAction), equalTo(expectedSupports));
    }

    static class SupportsArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    arguments(EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.PUT, false),
                    arguments(EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.DELETE, false),
                    arguments(EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.ALL, false),
                    arguments(EventKeyFactory.EventAction.PUT, EventKeyFactory.EventAction.GET, false),
                    arguments(EventKeyFactory.EventAction.PUT, EventKeyFactory.EventAction.DELETE, false),
                    arguments(EventKeyFactory.EventAction.PUT, EventKeyFactory.EventAction.ALL, false),
                    arguments(EventKeyFactory.EventAction.DELETE, EventKeyFactory.EventAction.GET, false),
                    arguments(EventKeyFactory.EventAction.DELETE, EventKeyFactory.EventAction.PUT, false),
                    arguments(EventKeyFactory.EventAction.DELETE, EventKeyFactory.EventAction.ALL, false)
            );
        }
    }
}