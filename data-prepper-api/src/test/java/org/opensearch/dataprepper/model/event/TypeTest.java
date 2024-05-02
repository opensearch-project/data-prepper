/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class TypeTest {
    @ParameterizedTest
    @EnumSource(Type.class)
    void fromTypeName_returns_expected_value(final Type type) {
        assertThat(Type.fromTypeName(type.getTypeName()), equalTo(type));
    }
}