/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.meter;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.meter.EMFLoggingRegistryConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class EMFLoggingRegistryConfigTest {
    @Test
    void testDefault() {
        final EMFLoggingRegistryConfig objectUnderTest = EMFLoggingRegistryConfig.DEFAULT;
        assertThat(objectUnderTest.prefix(), equalTo("emf"));
    }
}