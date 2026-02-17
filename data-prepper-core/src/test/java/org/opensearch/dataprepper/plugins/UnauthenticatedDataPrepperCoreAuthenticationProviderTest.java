/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class UnauthenticatedDataPrepperCoreAuthenticationProviderTest {
    private UnauthenticatedDataPrepperCoreAuthenticationProvider createObjectUnderTest() {
        return new UnauthenticatedDataPrepperCoreAuthenticationProvider();
    }

    @Test
    void getAuthenticator_returns_null() {
        assertThat(createObjectUnderTest().getAuthenticator(), nullValue());
    }
}