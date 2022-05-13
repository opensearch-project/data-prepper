/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SerializedJsonTest {
    @Test
    void fromString_returns_SerializedJsonImpl() {
        assertThat(SerializedJson.fromString("{}"), instanceOf(SerializedJsonImpl.class));
    }

    @Test
    void fromString_throws_if_the_jsonString_is_null() {
        assertThrows(NullPointerException.class, () -> SerializedJson.fromString(null));
    }
}