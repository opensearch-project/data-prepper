/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class NotificationTypeOptionTest {
    @ParameterizedTest
    @EnumSource(NotificationTypeOption.class)
    void fromOptionValue(final NotificationTypeOption option) {
        assertThat(NotificationTypeOption.fromOptionValue(option.name()), is(option));
    }
}