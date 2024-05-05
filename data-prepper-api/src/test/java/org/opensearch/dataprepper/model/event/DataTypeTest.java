/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataTypeTest {
    @ParameterizedTest
    @EnumSource(DataType.class)
    void fromTypeName_returns_expected_value(final DataType dataType) {
        assertThat(DataType.fromTypeName(dataType.getTypeName()), equalTo(dataType));
    }

    @Test
    void test_isSameType_withInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> DataType.isSameType(2, "unknown"));
    }

    @Test
    void test_isSameType() {
        assertThat(DataType.isSameType(2, "integer"), equalTo(true));
        assertThat(DataType.isSameType("testString", "string"), equalTo(true));
        assertThat(DataType.isSameType(2L, "long"), equalTo(true));
        assertThat(DataType.isSameType(2.0, "double"), equalTo(true));
        assertThat(DataType.isSameType(true, "boolean"), equalTo(true));

        assertThat(DataType.isSameType(false, "integer"), equalTo(false));
        assertThat(DataType.isSameType(2, "string"), equalTo(false));
        assertThat(DataType.isSameType("testString", "long"), equalTo(false));
        assertThat(DataType.isSameType("testString", "double"), equalTo(false));
        assertThat(DataType.isSameType(2, "boolean"), equalTo(false));
    }
}
