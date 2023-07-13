/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class MskBrokerConnectionTypeTest {
    @ParameterizedTest
    @EnumSource(MskBrokerConnectionType.class)
    void fromTypeValue(final MskBrokerConnectionType type) {
        assertThat(MskBrokerConnectionType.fromTypeValue(type.name()), is(type));
    }
}
