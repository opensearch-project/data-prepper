/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.meter;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class EMFLoggingRegistryConfigTest {
    @Test
    void testDefault() {
        final EMFLoggingRegistryConfig objectUnderTest = k -> null;
        assertThat(objectUnderTest.prefix(), equalTo("emf"));
    }
}