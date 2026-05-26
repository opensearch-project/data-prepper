/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ImmutablePluginConfigVariableTest {

    @Test
    void testGetValue_returnsConstructedValue() {
        assertThat(new ImmutablePluginConfigVariable("my-value").getValue(), equalTo("my-value"));
    }

    @Test
    void testIsUpdatable_returnsFalse() {
        assertThat(new ImmutablePluginConfigVariable("v").isUpdatable(), equalTo(false));
    }

    @Test
    void testSetValue_throwsException() {
        assertThrows(Exception.class, () -> new ImmutablePluginConfigVariable("v").setValue("new"));
    }

    @Test
    void testRefresh_isNoOp() {
        final ImmutablePluginConfigVariable variable = new ImmutablePluginConfigVariable("v");
        variable.refresh();
        assertThat(variable.getValue(), equalTo("v"));
    }
}
