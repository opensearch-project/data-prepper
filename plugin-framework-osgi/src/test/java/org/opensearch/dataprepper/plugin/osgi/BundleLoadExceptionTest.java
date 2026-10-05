/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class BundleLoadExceptionTest {

    @Test
    void constructor_with_message_only() {
        final BundleLoadException ex = new BundleLoadException("test message");

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    void constructor_with_message_and_cause() {
        final RuntimeException cause = new RuntimeException("root cause");
        final BundleLoadException ex = new BundleLoadException("wrapper", cause);

        assertThat(ex.getMessage(), is("wrapper"));
        assertThat(ex.getCause(), is(notNullValue()));
        assertThat(ex.getCause().getMessage(), is("root cause"));
    }
}
