/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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