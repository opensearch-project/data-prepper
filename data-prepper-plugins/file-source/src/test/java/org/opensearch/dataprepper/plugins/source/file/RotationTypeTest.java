/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

class RotationTypeTest {

    @Test
    void noRotationEnumValueExists() {
        assertThat(RotationType.valueOf("NO_ROTATION"), equalTo(RotationType.NO_ROTATION));
    }

    @Test
    void createRenameEnumValueExists() {
        assertThat(RotationType.valueOf("CREATE_RENAME"), equalTo(RotationType.CREATE_RENAME));
    }

    @Test
    void copytruncateEnumValueExists() {
        assertThat(RotationType.valueOf("COPYTRUNCATE"), equalTo(RotationType.COPYTRUNCATE));
    }

    @Test
    void deletedEnumValueExists() {
        assertThat(RotationType.valueOf("DELETED"), equalTo(RotationType.DELETED));
    }

    @Test
    void valuesContainsFourEntries() {
        assertThat(RotationType.values().length, equalTo(4));
    }

    @Test
    void allValuesAreNotNull() {
        for (final RotationType type : RotationType.values()) {
            assertThat(type, notNullValue());
        }
    }
}
