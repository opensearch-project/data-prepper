/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class GcpCredentialsExceptionTest {
    @Test
    void constructor_sets_message_and_cause() {
        final IOException cause = new IOException("boom");
        final GcpCredentialsException objectUnderTest = new GcpCredentialsException("failed", cause);

        assertThat(objectUnderTest.getMessage(), equalTo("failed"));
        assertThat(objectUnderTest.getCause(), sameInstance(cause));
    }
}
