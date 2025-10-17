/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the CoreVersionProvider which is generated dynamically with the
 * current Gradle version.
 */
class CoreVersionProviderTest {
    private CoreVersionProvider createObjectUnderTest() {
        return new CoreVersionProvider();
    }

    @Test
    void getVersionString_returns_project_version() {
        final String expectedVersion = System.getProperty("project.version");

        assertThat(createObjectUnderTest().getVersionString(), equalTo(expectedVersion));
    }
}