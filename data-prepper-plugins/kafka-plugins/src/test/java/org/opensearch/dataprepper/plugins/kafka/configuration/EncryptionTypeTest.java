/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.model.plugin.kafka.EncryptionType;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class EncryptionTypeTest {
    @ParameterizedTest
    @EnumSource(EncryptionType.class)
    void fromTypeValue(final EncryptionType type) {
        assertThat(EncryptionType.fromTypeValue(type.name()), is(type));
    }
}
