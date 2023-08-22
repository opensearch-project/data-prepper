package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class NotificationSourceOptionTest {
    @ParameterizedTest
    @EnumSource(NotificationSourceOption.class)
    void fromOptionValue(final NotificationSourceOption option) {
        assertThat(NotificationSourceOption.fromOptionValue(option.name()), is(option));
    }
}
