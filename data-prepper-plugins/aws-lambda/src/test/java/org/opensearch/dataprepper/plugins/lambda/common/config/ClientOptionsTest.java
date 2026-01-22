/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common.config;

import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertEquals;

class ClientOptionsTest {

    @Test
    void testDefaultReadTimeout() {
        ClientOptions clientOptions = new ClientOptions();
        assertEquals(null, clientOptions.getReadTimeout());
    }

    @Test
    void testDefaultApiCallAttemptTimeout() {
        ClientOptions clientOptions = new ClientOptions();
        assertEquals(null, clientOptions.getApiCallAttemptTimeout());
    }
}
